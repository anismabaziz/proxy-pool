import sqlite3
from datetime import datetime, timedelta

from src.capabilities import Outcome, ProbeOutcome

DEFAULT_DB_PATH = "proxies.db"

# the shape the pool has in this build of the tool. Bumped whenever that shape
# changes, so a pool written by an older build is carried across rather than
# misread
SCHEMA_VERSION = 2

# how many consecutive unreachable results a candidate may accumulate before it
# leaves the pool. Not a setting: a retention rule nobody has checked is not a
# retention rule
STRIKE_LIMIT = 3

# how long a candidate may sit unprobed before the pool stops counting it as
# data. Age is not a rule in its own right: what matters is whether a recent run
# measured the candidate, not how long ago it was scraped
STALE_AFTER_DAYS = 7

_POOL_COLUMNS = """
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  address TEXT NOT NULL UNIQUE,
  latency_ms INTEGER,
  first_working_at DATETIME,
  last_probed_at DATETIME,
  strikes INTEGER NOT NULL DEFAULT 0
"""


def _connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def stale_before(moment: datetime) -> str:
    """The point before which no candidate has been probed by a recent run"""

    return (moment - timedelta(days=STALE_AFTER_DAYS)).isoformat()


def init_db(db_path: str) -> None:
    """Bring the pool at db_path up to the current schema, carrying the rows of
    an older pool across on the way"""

    conn = _connect(db_path)

    try:
        if _applied_version(conn) == SCHEMA_VERSION:
            return

        # one transaction around the whole rebuild, so a pool cannot be left
        # holding neither its old table nor its new one
        conn.execute("BEGIN")

        if _has_older_pool(conn):
            _carry_pool_across(conn)
        else:
            _create_pool(conn, "proxies")

        _index_pool(conn)
        _create_runs(conn)
        _record_version(conn, SCHEMA_VERSION)
        conn.commit()
    finally:
        conn.close()


def schema_version(db_path: str) -> int:
    """The schema version this pool was built with"""

    conn = _connect(db_path)

    try:
        version = _applied_version(conn)

        if version is None:
            raise sqlite3.DatabaseError(f"no schema version recorded in {db_path}")

        return version
    finally:
        conn.close()


def get_proxies(db_path: str) -> list[str]:
    """Every stored address, in the order the pool took them in"""

    conn = _connect(db_path)

    try:
        return [
            row[0] for row in conn.execute("SELECT address FROM proxies ORDER BY id")
        ]
    finally:
        conn.close()


def record_outcome(outcome: ProbeOutcome, db_path: str) -> bool:
    """Fold one probe result into the pool, and say whether the candidate left it
    as a result"""

    conn = _connect(db_path)

    try:
        now = datetime.now().isoformat()
        evicted = 0

        if outcome.state is Outcome.WORKING:
            # a working probe clears the strikes, since the count describes how
            # the candidate has been answering and this is it answering. The
            # first working time is only filled in when the pool has none, so a
            # candidate carried over from an older pool starts being measured
            conn.execute(
                """
                INSERT INTO proxies
                    (address, latency_ms, first_working_at, last_probed_at, strikes)
                VALUES (?, ?, ?, ?, 0)
                ON CONFLICT(address) DO UPDATE SET
                    latency_ms = EXCLUDED.latency_ms,
                    last_probed_at = EXCLUDED.last_probed_at,
                    first_working_at = COALESCE(
                        proxies.first_working_at, EXCLUDED.first_working_at
                    ),
                    strikes = 0
                """,
                (outcome.proxy, outcome.latency_ms, now, now),
            )
        elif outcome.state is Outcome.UNREACHABLE:
            # only a candidate the pool already holds can answer to a strike:
            # nothing here admits a candidate that has never worked
            conn.execute(
                "UPDATE proxies SET strikes = strikes + 1, last_probed_at = ?"
                " WHERE address = ?",
                (now, outcome.proxy),
            )
            evicted = conn.execute(
                "DELETE FROM proxies WHERE address = ? AND strikes >= ?",
                (outcome.proxy, STRIKE_LIMIT),
            ).rowcount
        else:
            # a refusal is a definitive answer rather than an absence of one, so
            # it neither adds a strike nor clears the ones already there
            conn.execute(
                "UPDATE proxies SET last_probed_at = ? WHERE address = ?",
                (now, outcome.proxy),
            )

        conn.commit()
    finally:
        conn.close()

    return bool(evicted)


def drop_stale(before: str, db_path: str) -> int:
    """Drop every candidate no run has probed since before, and say how many
    went. A candidate the pool has never probed is included: a row nobody has
    measured is not data"""

    conn = _connect(db_path)

    try:
        dropped = conn.execute(
            "DELETE FROM proxies WHERE last_probed_at IS NULL OR last_probed_at < ?",
            (before,),
        ).rowcount
        conn.commit()
    finally:
        conn.close()

    return dropped


def _create_pool(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({_POOL_COLUMNS})")


def _index_pool(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_latency ON proxies(latency_ms)")


def _create_runs(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          started_at DATETIME NOT NULL,
          scraped INTEGER NOT NULL,
          candidates INTEGER NOT NULL,
          working INTEGER NOT NULL,
          unreachable INTEGER NOT NULL,
          rejected INTEGER NOT NULL,
          contributions TEXT NOT NULL
        )
    """)


def _applied_version(conn: sqlite3.Connection) -> int | None:
    """The schema version this pool already carries, or nothing when it predates
    the marker"""

    marker = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'"
    ).fetchone()

    if marker is None:
        return None

    recorded = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()

    return None if recorded is None or recorded[0] is None else int(recorded[0])


def _record_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
    conn.execute("INSERT INTO schema_version (version) VALUES (?)", (version,))


def _has_older_pool(conn: sqlite3.Connection) -> bool:
    """Whether this pool predates the current columns, judged by the address
    column the current schema is built around"""

    columns = {row[1] for row in conn.execute("PRAGMA table_info(proxies)")}

    return bool(columns) and "address" not in columns


def _carry_pool_across(conn: sqlite3.Connection) -> None:
    """Build the pool again from the rows that still mean something. Rebuilding
    is the only way: the address column's declared type is corrected here, and a
    column's type cannot be corrected in place"""

    _create_pool(conn, "carried_pool")
    # the old pool recorded a candidate as working whenever it was last checked,
    # and never said when that was first, so that is where its clock starts
    conn.execute("""
        INSERT INTO carried_pool (address, latency_ms, first_working_at, last_probed_at)
        SELECT ip || ':' || port, latency_ms, last_checked, last_checked
        FROM proxies
    """)
    conn.execute("DROP TABLE proxies")
    conn.execute("ALTER TABLE carried_pool RENAME TO proxies")
