import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.capabilities import Outcome, ProbeOutcome

DEFAULT_DB_PATH = "proxies.db"

# the shape the pool has in this build of the tool. Bumped whenever that shape
# changes, so a pool written by an older build is carried across rather than
# misread
SCHEMA_VERSION = 3

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


@dataclass(frozen=True)
class SourceYield:
    """What one source contributed to one run, and how much of it worked"""

    source: str
    candidates: int
    working: int


@dataclass(frozen=True)
class RunRecord:
    """What one run found, kept so that a later reader can ask what has happened
    since. The candidates are the ones the run took on and still trusted: a
    candidate it dropped for having gone unprobed was never measured, so it is
    not in the count the run's figures describe"""

    started_at: datetime
    scraped: int
    candidates: int
    working: int
    unreachable: int
    rejected: int
    contributions: tuple[SourceYield, ...] = ()


@dataclass(frozen=True)
class Lifetime:
    """One spell of one candidate working: when it first worked, and when the pool
    let it go if it has. A candidate that goes unreachable once and answers again
    is still in the same spell, since a strike is not an ending: the spell ends
    where the pool drops the candidate, be that three strikes in a row or nobody
    measuring it for a week"""

    address: str
    started_at: datetime
    ended_at: datetime | None


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
        _create_deaths(conn)
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
            # the row as it stands after this miss: how many strikes it is on,
            # and when it first worked, which the pool forgets along with it
            standing = conn.execute(
                "SELECT strikes, first_working_at FROM proxies WHERE address = ?",
                (outcome.proxy,),
            ).fetchone()

            if standing is not None and standing[0] >= STRIKE_LIMIT:
                _record_ending(conn, outcome.proxy, standing[1], now)
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
        going = conn.execute(
            "SELECT address, first_working_at FROM proxies"
            " WHERE last_probed_at IS NULL OR last_probed_at < ?",
            (before,),
        ).fetchall()

        for address, first_working_at in going:
            _record_ending(conn, address, first_working_at, datetime.now().isoformat())

        dropped = conn.execute(
            "DELETE FROM proxies WHERE last_probed_at IS NULL OR last_probed_at < ?",
            (before,),
        ).rowcount
        conn.commit()
    finally:
        conn.close()

    return dropped


def record_run(record: RunRecord, db_path: str) -> None:
    """Keep what one run found, so that a later reader can ask what the runs
    between it and now came to"""

    conn = _connect(db_path)

    try:
        conn.execute(
            """
            INSERT INTO runs
                (started_at, scraped, candidates, working, unreachable, rejected,
                 contributions)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.started_at.isoformat(),
                record.scraped,
                record.candidates,
                record.working,
                record.unreachable,
                record.rejected,
                # the per-source counts are a shape of their own rather than a
                # column each, since the number of sources is not fixed
                json.dumps([asdict(yielded) for yielded in record.contributions]),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def read_runs(db_path: str) -> tuple[RunRecord, ...]:
    """Every run the pool has recorded, in the order they happened"""

    rows = _recorded(
        db_path,
        ("runs",),
        "SELECT started_at, scraped, candidates, working, unreachable, rejected,"
        " contributions FROM runs ORDER BY started_at, id",
    )

    return tuple(
        RunRecord(
            started_at=datetime.fromisoformat(row[0]),
            scraped=row[1],
            candidates=row[2],
            working=row[3],
            unreachable=row[4],
            rejected=row[5],
            contributions=_contributions(row[6]),
        )
        for row in rows
    )


def read_lifetimes(db_path: str) -> tuple[Lifetime, ...]:
    """Every spell of a candidate working that the pool still knows about: the
    ones going on in the pool, and the ones that ended"""

    rows = _recorded(
        db_path,
        ("proxies", "deaths"),
        "SELECT address, first_working_at, NULL FROM proxies"
        " WHERE first_working_at IS NOT NULL"
        " UNION ALL"
        " SELECT address, first_working_at, died_at FROM deaths",
    )

    # a candidate that worked, left, and worked again is two spells rather than
    # one, and the figure is about spells, so they are filed under the pair that
    # starts each of them
    spells = {
        (row[0], row[1]): Lifetime(
            address=row[0],
            started_at=datetime.fromisoformat(row[1]),
            ended_at=None if row[2] is None else datetime.fromisoformat(row[2]),
        )
        for row in rows
    }

    return tuple(sorted(spells.values(), key=lambda spell: spell.started_at))


def _record_ending(
    conn: sqlite3.Connection,
    address: str,
    first_working_at: str | None,
    died_at: str,
) -> None:
    """Keep the working spell of a candidate that is leaving the pool, since the
    pool forgets it and how long it lasted cannot be said after it has gone. A
    candidate nobody ever saw work has no spell to keep: it was never working, so
    its leaving says nothing about how long working candidates last"""

    if first_working_at is None:
        return

    conn.execute(
        "INSERT INTO deaths (address, first_working_at, died_at) VALUES (?, ?, ?)",
        (address, first_working_at, died_at),
    )


def _recorded(db_path: str, tables: tuple[str, ...], query: str) -> list[Any]:
    """What a pool has recorded, or nothing at all when it has not been written
    to. A pool that was never built, and a pool an older build wrote without the
    tables this one reads, are both pools with no history in them: asking one is
    a question whose answer is nothing rather than a failure"""

    if not Path(db_path).exists():
        return []

    conn = _connect(db_path)

    try:
        present = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

        if not set(tables) <= present:
            return []

        return conn.execute(query).fetchall()
    finally:
        conn.close()


def _contributions(raw: str) -> tuple[SourceYield, ...]:
    """The per-source counts a run recorded, read back as the shape they were
    written in"""

    return tuple(
        SourceYield(
            source=yielded["source"],
            candidates=yielded["candidates"],
            working=yielded["working"],
        )
        for yielded in json.loads(raw)
    )


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


def _create_deaths(conn: sqlite3.Connection) -> None:
    """When each working candidate's spell ended. The pool drops a candidate the
    moment it stops working, so without this the tool could only ever report on
    the candidates it still holds"""

    conn.execute("""
        CREATE TABLE IF NOT EXISTS deaths (
          address TEXT NOT NULL,
          first_working_at DATETIME NOT NULL,
          died_at DATETIME NOT NULL
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
