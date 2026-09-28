import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path

from src.capabilities import Outcome, ProbeOutcome
from src.storage import (
    SCHEMA_VERSION,
    STRIKE_LIMIT,
    Lifetime,
    RunRecord,
    SourceYield,
    drop_stale,
    get_proxies,
    init_db,
    read_lifetimes,
    read_runs,
    record_outcome,
    record_run,
    schema_version,
)

LEGACY_POOL = """
CREATE TABLE proxies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ip TEST NOT NULL,
  port INTEGER NOT NULL,
  protocol TEXT DEFAULT 'http',
  latency_ms INTEGER,
  last_checked DATETIME,
  times_used INTEGER DEFAULT 0,
  UNIQUE(ip, port)
)
"""


def ago(days: float) -> str:
    return (datetime.now() - timedelta(days=days)).isoformat()


def probed(address: str, db_path: Path) -> None:
    record_outcome(ProbeOutcome(address, Outcome.WORKING, 120), str(db_path))


def missed(address: str, db_path: Path) -> None:
    record_outcome(ProbeOutcome(address, Outcome.UNREACHABLE), str(db_path))


def refused(address: str, db_path: Path) -> None:
    record_outcome(ProbeOutcome(address, Outcome.REJECTED), str(db_path))


def older_pool(
    db_path: Path, *rows: tuple[str, int, int | None, str | None, int]
) -> None:
    """A pool in the shape the tool used before strikes and the schema marker"""

    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute(LEGACY_POOL)
        conn.executemany(
            "INSERT INTO proxies (ip, port, latency_ms, last_checked, times_used)"
            " VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()


def pooled(db_path: Path, column: str, address: str) -> object:
    with closing(sqlite3.connect(db_path)) as conn:
        found = conn.execute(
            f"SELECT {column} FROM proxies WHERE address = ?", (address,)
        ).fetchone()

    assert found is not None, f"{address} is not in the pool"
    return found[0]


def started_working(db_path: Path, address: str) -> str:
    """When the pool first recorded a candidate as working"""

    recorded = pooled(db_path, "first_working_at", address)
    assert isinstance(recorded, str)

    return recorded


def test_saved_proxies_come_back_as_addresses(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    probed("5.6.7.8:3128", pool)

    assert sorted(get_proxies(str(pool))) == ["1.2.3.4:8080", "5.6.7.8:3128"]


def test_saving_the_same_proxy_twice_does_not_duplicate_it(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    probed("1.2.3.4:8080", pool)

    assert get_proxies(str(pool)) == ["1.2.3.4:8080"]


def test_a_working_probe_stores_the_latency_it_measured(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    record_outcome(ProbeOutcome("1.2.3.4:8080", Outcome.WORKING, 340), str(pool))

    assert pooled(pool, "latency_ms", "1.2.3.4:8080") == 340


def test_the_first_working_time_is_stamped_and_never_moved(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)

    first_seen = pooled(pool, "first_working_at", "1.2.3.4:8080")
    assert first_seen is not None

    probed("1.2.3.4:8080", pool)

    assert pooled(pool, "first_working_at", "1.2.3.4:8080") == first_seen


def test_one_unreachable_result_keeps_the_candidate(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    missed("1.2.3.4:8080", pool)

    assert get_proxies(str(pool)) == ["1.2.3.4:8080"]


def test_two_unreachable_results_still_keep_the_candidate(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    missed("1.2.3.4:8080", pool)
    missed("1.2.3.4:8080", pool)

    assert get_proxies(str(pool)) == ["1.2.3.4:8080"]


def test_the_three_unreachable_results_take_the_candidate_out(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)

    for _ in range(STRIKE_LIMIT - 1):
        missed("1.2.3.4:8080", pool)
        assert get_proxies(str(pool)) == ["1.2.3.4:8080"]

    missed("1.2.3.4:8080", pool)

    assert get_proxies(str(pool)) == []


def test_a_working_probe_clears_the_strikes(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    missed("1.2.3.4:8080", pool)
    missed("1.2.3.4:8080", pool)
    probed("1.2.3.4:8080", pool)

    missed("1.2.3.4:8080", pool)
    missed("1.2.3.4:8080", pool)

    assert get_proxies(str(pool)) == ["1.2.3.4:8080"]


def test_a_refusal_is_not_a_strike(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    refused("1.2.3.4:8080", pool)
    refused("1.2.3.4:8080", pool)
    missed("1.2.3.4:8080", pool)
    missed("1.2.3.4:8080", pool)

    assert get_proxies(str(pool)) == ["1.2.3.4:8080"]

    missed("1.2.3.4:8080", pool)

    assert get_proxies(str(pool)) == []


def test_one_candidate_leaving_does_not_take_its_neighbours(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    probed("5.6.7.8:3128", pool)

    for _ in range(STRIKE_LIMIT):
        missed("1.2.3.4:8080", pool)

    assert get_proxies(str(pool)) == ["5.6.7.8:3128"]


def test_an_unreachable_result_admits_nobody_to_the_pool(pool: Path) -> None:
    missed("1.2.3.4:8080", pool)

    assert get_proxies(str(pool)) == []


def test_a_candidate_probed_before_the_cutoff_goes(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)

    assert drop_stale(ago(1), str(pool)) == 0
    assert get_proxies(str(pool)) == ["1.2.3.4:8080"]

    assert drop_stale(datetime.now().isoformat(), str(pool)) == 1
    assert get_proxies(str(pool)) == []


def test_a_candidate_no_run_ever_probed_goes(pool: Path) -> None:
    with closing(sqlite3.connect(pool)) as conn:
        conn.execute("INSERT INTO proxies (address) VALUES (?)", ("1.2.3.4:8080",))
        conn.commit()

    assert drop_stale(ago(1), str(pool)) == 1
    assert get_proxies(str(pool)) == []


def test_the_pool_records_which_schema_version_was_applied(pool: Path) -> None:
    assert schema_version(str(pool)) == SCHEMA_VERSION


def test_a_carried_pool_records_the_version_too(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    older_pool(db_path, ("1.2.3.4", 8080, 120, None, 0))

    init_db(str(db_path))

    assert schema_version(str(db_path)) == SCHEMA_VERSION


def test_the_pool_carries_a_place_to_record_runs(pool: Path) -> None:
    with closing(sqlite3.connect(pool)) as conn:
        recorded = [row[1] for row in conn.execute("PRAGMA table_info(runs)")]

    assert recorded == [
        "id",
        "started_at",
        "scraped",
        "candidates",
        "working",
        "unreachable",
        "rejected",
        "contributions",
    ]


def test_the_address_column_is_declared_as_text(pool: Path) -> None:
    with closing(sqlite3.connect(pool)) as conn:
        declared = {
            row[1]: row[2] for row in conn.execute("PRAGMA table_info(proxies)")
        }

    assert declared["address"] == "TEXT"


def test_the_address_column_is_the_only_way_in(pool: Path) -> None:
    with closing(sqlite3.connect(pool)) as conn:
        declared = [row[1] for row in conn.execute("PRAGMA table_info(proxies)")]

    assert declared == [
        "id",
        "address",
        "latency_ms",
        "first_working_at",
        "last_probed_at",
        "strikes",
    ]


def test_an_older_pool_is_carried_across(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"

    older_pool(
        db_path,
        ("1.2.3.4", 8080, 120, "2026-06-01T21:06:10.322543", 4),
        ("5.6.7.8", 3128, 340, "2026-06-01T21:06:11.322543", 0),
    )

    init_db(str(db_path))

    assert sorted(get_proxies(str(db_path))) == ["1.2.3.4:8080", "5.6.7.8:3128"]

    with closing(sqlite3.connect(db_path)) as conn:
        carried = conn.execute(
            "SELECT address, latency_ms, first_working_at, last_probed_at, strikes"
            " FROM proxies ORDER BY address"
        ).fetchall()

    assert carried == [
        (
            "1.2.3.4:8080",
            120,
            "2026-06-01T21:06:10.322543",
            "2026-06-01T21:06:10.322543",
            0,
        ),
        (
            "5.6.7.8:3128",
            340,
            "2026-06-01T21:06:11.322543",
            "2026-06-01T21:06:11.322543",
            0,
        ),
    ]


def test_an_older_pool_loses_the_columns_nothing_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"

    older_pool(db_path, ("1.2.3.4", 8080, 120, None, 0))

    init_db(str(db_path))

    with closing(sqlite3.connect(db_path)) as conn:
        declared = [entry[1] for entry in conn.execute("PRAGMA table_info(proxies)")]

    assert "protocol" not in declared
    assert "times_used" not in declared


def test_carrying_a_pool_across_twice_changes_nothing(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"

    older_pool(db_path, ("1.2.3.4", 8080, 120, None, 0))

    init_db(str(db_path))
    init_db(str(db_path))

    assert get_proxies(str(db_path)) == ["1.2.3.4:8080"]


def run_record(
    started_at: datetime,
    contributions: tuple[SourceYield, ...] = (),
) -> RunRecord:
    return RunRecord(
        started_at=started_at,
        scraped=900,
        candidates=500,
        working=12,
        unreachable=470,
        rejected=18,
        contributions=contributions,
    )


def test_a_recorded_run_comes_back_as_it_was_written(pool: Path) -> None:
    record = run_record(
        datetime(2026, 6, 1, 8, 0),
        (SourceYield("geonode", 300, 5), SourceYield("github_raw_spys", 200, 7)),
    )

    record_run(record, str(pool))

    assert read_runs(str(pool)) == (record,)


def test_recorded_runs_come_back_in_the_order_they_happened(pool: Path) -> None:
    second = run_record(datetime(2026, 6, 2, 8, 0))
    first = run_record(datetime(2026, 6, 1, 8, 0))

    record_run(second, str(pool))
    record_run(first, str(pool))

    assert [record.started_at for record in read_runs(str(pool))] == [
        first.started_at,
        second.started_at,
    ]


def test_a_run_that_read_no_source_records_no_contribution(pool: Path) -> None:
    record = run_record(datetime(2026, 6, 1, 8, 0), ())

    record_run(record, str(pool))

    assert read_runs(str(pool))[0].contributions == ()


def test_a_candidate_still_in_the_pool_is_a_working_spell_open(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    started = started_working(pool, "1.2.3.4:8080")

    assert read_lifetimes(str(pool)) == (
        Lifetime("1.2.3.4:8080", datetime.fromisoformat(started), None),
    )


def test_a_candidate_that_worked_and_left_keeps_the_time_it_started(pool: Path) -> None:
    probed("1.2.3.4:8080", pool)
    started = started_working(pool, "1.2.3.4:8080")

    for _ in range(STRIKE_LIMIT):
        missed("1.2.3.4:8080", pool)

    spell = read_lifetimes(str(pool))[0]

    assert (spell.address, spell.started_at.isoformat()) == ("1.2.3.4:8080", started)
    assert spell.ended_at is not None


def test_a_candidate_nobody_ever_saw_work_leaves_nothing_to_measure(pool: Path) -> None:
    """A candidate that never carried a request was never working, so the fact
    that it left says nothing about how long working candidates last"""

    for _ in range(STRIKE_LIMIT):
        missed("1.2.3.4:8080", pool)

    assert read_lifetimes(str(pool)) == ()


def test_a_candidate_dropped_for_going_unprobed_keeps_its_starting_time(
    pool: Path,
) -> None:
    """A candidate nobody measured for a week is out of the pool whatever its
    strikes say, and how long it had been working still counts"""

    probed("1.2.3.4:8080", pool)
    started = started_working(pool, "1.2.3.4:8080")

    assert drop_stale(datetime.now().isoformat(), str(pool)) == 1

    spell = read_lifetimes(str(pool))[0]

    assert (spell.address, spell.started_at.isoformat()) == ("1.2.3.4:8080", started)
    assert spell.ended_at is not None


def test_a_candidate_dropped_for_going_unprobed_before_it_ever_worked_is_ignored(
    pool: Path,
) -> None:
    with closing(sqlite3.connect(pool)) as conn:
        conn.execute("INSERT INTO proxies (address) VALUES (?)", ("1.2.3.4:8080",))
        conn.commit()

    drop_stale(ago(1), str(pool))

    assert read_lifetimes(str(pool)) == ()


def test_a_candidate_that_came_back_is_measured_from_the_second_time(
    pool: Path,
) -> None:
    """A candidate that left and worked again had two working spells, and the
    figure is about spells rather than addresses"""

    probed("1.2.3.4:8080", pool)

    for _ in range(STRIKE_LIMIT):
        missed("1.2.3.4:8080", pool)

    probed("1.2.3.4:8080", pool)

    spells = read_lifetimes(str(pool))

    assert len(spells) == 2
    assert [spell.ended_at is None for spell in spells] == [False, True]
    assert spells[0].started_at < spells[1].started_at


def test_a_pool_written_before_the_tool_kept_working_spells_gains_that_table(
    pool: Path,
) -> None:
    """The marker is what lets a pool that already exists pick up a new table, so
    a pool out there is neither left without one nor thrown away for it"""

    probed("1.2.3.4:8080", pool)

    with closing(sqlite3.connect(pool)) as conn:
        conn.execute("DROP TABLE deaths")
        conn.execute("DELETE FROM schema_version")
        conn.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION - 1,))
        conn.commit()

    init_db(str(pool))

    assert schema_version(str(pool)) == SCHEMA_VERSION
    assert get_proxies(str(pool)) == ["1.2.3.4:8080"]
    assert [spell.address for spell in read_lifetimes(str(pool))] == ["1.2.3.4:8080"]


def test_a_pool_written_before_there_was_any_history_reads_as_having_none(
    pool: Path,
) -> None:
    """A pool an older build wrote has no deaths table to read, and asking it for
    them is a question with the answer nothing rather than a failure"""

    with closing(sqlite3.connect(pool)) as conn:
        conn.execute("DROP TABLE deaths")
        conn.commit()

    assert read_runs(str(pool)) == ()
    assert read_lifetimes(str(pool)) == ()
