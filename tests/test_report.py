from datetime import datetime, timedelta
from pathlib import Path

from src.report import (
    HistoryReport,
    SourceStanding,
    hit_rate_per_run,
    read_history,
    render_markdown,
    survival,
    yield_per_source,
)
from src.storage import Lifetime, RunRecord, SourceYield, record_run

DAY_ONE = datetime(2026, 6, 1, 8, 0)


def days_after(moment: datetime, days: float) -> datetime:
    return moment + timedelta(days=days)


def scraped_run(
    started_at: datetime,
    candidates: int = 100,
    working: int = 0,
    unreachable: int | None = None,
    rejected: int = 0,
    contributions: tuple[SourceYield, ...] = (),
) -> RunRecord:
    return RunRecord(
        started_at=started_at,
        scraped=candidates,
        candidates=candidates,
        working=working,
        unreachable=candidates - working - rejected
        if unreachable is None
        else unreachable,
        rejected=rejected,
        contributions=contributions,
    )


def yields(report: HistoryReport) -> dict[str, float | None]:
    return {
        standing.source: standing.yield_rate for standing in yield_per_source(report)
    }


def standings_of(report: HistoryReport) -> dict[str, SourceStanding]:
    return {standing.source: standing for standing in yield_per_source(report)}


def history_of(
    *runs: RunRecord,
    lifetimes: tuple[Lifetime, ...] = (),
    roster: tuple[str, ...] = (),
) -> HistoryReport:
    return HistoryReport(runs=runs, lifetimes=lifetimes, roster=roster)


def test_a_run_whose_probes_all_reached_nothing_has_no_hit_rate() -> None:
    """A run whose every chunk was lost measured no probe at all, and a rate of
    zero for it would read as a finding about the proxies rather than as a
    measurement the run never managed to make"""

    lost = RunRecord(
        started_at=DAY_ONE,
        scraped=200,
        candidates=200,
        working=0,
        unreachable=0,
        rejected=0,
    )

    assert hit_rate_per_run(history_of(lost))[0].hit_rate is None


def test_hit_rate_is_the_share_of_probes_that_carried_a_request() -> None:
    report = history_of(scraped_run(DAY_ONE, candidates=200, working=6))

    assert hit_rate_per_run(report)[0].hit_rate == 0.03


def test_hit_rate_leaves_out_the_candidates_nobody_got_to_probe() -> None:
    """The candidates a run took on but never reached an answer about are its own
    failure, not a fact about the proxies, so they stay out of the rate"""

    partial = RunRecord(
        started_at=DAY_ONE,
        scraped=200,
        candidates=200,
        working=5,
        unreachable=95,
        rejected=0,
    )
    line = hit_rate_per_run(history_of(partial))[0]

    assert (line.measured, line.hit_rate) == (100, 0.05)


def test_hit_rate_is_stated_for_every_run_in_the_order_they_happened() -> None:
    report = history_of(
        scraped_run(days_after(DAY_ONE, 1), candidates=100, working=1),
        scraped_run(DAY_ONE, candidates=100, working=10),
    )

    assert [line.started_at for line in hit_rate_per_run(report)] == [
        DAY_ONE,
        days_after(DAY_ONE, 1),
    ]


def test_hit_rate_separates_the_three_states_a_probe_can_reach() -> None:
    report = history_of(
        scraped_run(DAY_ONE, candidates=100, working=4, unreachable=90, rejected=6)
    )

    line = hit_rate_per_run(report)[0]

    assert (line.working, line.unreachable, line.rejected) == (4, 90, 6)


def test_yield_counts_what_a_source_contributed_and_what_worked() -> None:
    report = history_of(
        scraped_run(
            DAY_ONE,
            contributions=(SourceYield("geonode", 300, 5),),
        )
    )

    standing = yield_per_source(report)[0]

    assert (standing.source, standing.runs, standing.candidates, standing.working) == (
        "geonode",
        1,
        300,
        5,
    )
    assert standing.yield_rate == 5 / 300


def test_yield_per_source_is_not_the_hit_rate_of_the_run() -> None:
    """Hit rate is one number for a whole run; yield is a rate per source, so a
    run whose sources differ can have both without either standing in for the
    other"""

    report = history_of(
        scraped_run(
            DAY_ONE,
            candidates=400,
            working=20,
            contributions=(SourceYield("rich", 100, 18), SourceYield("poor", 300, 2)),
        )
    )

    assert hit_rate_per_run(report)[0].hit_rate == 0.05
    assert yields(report) == {"rich": 0.18, "poor": 2 / 300}


def test_a_source_that_never_answered_is_told_apart_from_one_nothing_worked() -> None:
    report = history_of(
        scraped_run(
            DAY_ONE,
            contributions=(SourceYield("quiet", 200, 0),),
        ),
        roster=("quiet", "broken"),
    )

    standings = {standing.source: standing for standing in yield_per_source(report)}

    assert standings["broken"].standing == "never answered"
    assert standings["quiet"].standing == "nothing worked"


def test_a_source_nobody_has_asked_yet_is_not_written_off() -> None:
    """A pool that has only been revalidated has never read a source, so saying
    one never answered would be blaming a source for a run that did not ask"""

    revalidated = RunRecord(
        started_at=DAY_ONE,
        scraped=0,
        candidates=20,
        working=2,
        unreachable=18,
        rejected=0,
    )

    report = history_of(revalidated, roster=("geonode",))

    assert standings_of(report)["geonode"].standing == "not asked yet"


def test_yield_adds_up_over_the_runs_a_source_answered_in() -> None:
    report = history_of(
        scraped_run(DAY_ONE, contributions=(SourceYield("geonode", 100, 1),)),
        scraped_run(
            days_after(DAY_ONE, 1), contributions=(SourceYield("geonode", 100, 4),)
        ),
    )

    standing = yield_per_source(report)[0]

    assert (standing.runs, standing.candidates, standing.working) == (2, 200, 5)
    assert standing.yield_rate == 0.025


def test_a_source_the_code_no_longer_carries_still_reports_its_runs() -> None:
    report = history_of(
        scraped_run(DAY_ONE, contributions=(SourceYield("retired", 100, 2),)),
        roster=("current",),
    )

    # a source that answered is worth reading before one that did not, so the
    # never-answered ones sit at the bottom of the table
    assert [standing.source for standing in yield_per_source(report)] == [
        "retired",
        "current",
    ]


def test_a_single_run_leaves_survival_undefined() -> None:
    """One run has had no time to lose anything, so a curve drawn from it would
    be a confident number about nothing"""

    report = history_of(
        scraped_run(DAY_ONE, working=3),
        lifetimes=(
            Lifetime("1.1.1.1:80", DAY_ONE, None),
            Lifetime("2.2.2.2:80", DAY_ONE, None),
        ),
    )

    assert survival(report).measured is False
    assert "at least a day" in survival(report).undefined_because


def test_survival_is_undefined_when_nothing_has_ever_worked() -> None:
    report = history_of(scraped_run(DAY_ONE), scraped_run(days_after(DAY_ONE, 4)))

    assert survival(report).measured is False


def test_survival_counts_the_candidates_still_working_a_day_at_a_time() -> None:
    report = history_of(
        scraped_run(DAY_ONE),
        scraped_run(days_after(DAY_ONE, 3)),
        lifetimes=(
            Lifetime("1.1.1.1:80", DAY_ONE, None),
            Lifetime("2.2.2.2:80", DAY_ONE, days_after(DAY_ONE, 1)),
            Lifetime("3.3.3.3:80", days_after(DAY_ONE, 2), None),
        ),
    )

    points = survival(report).points

    assert [(point.day, point.at_risk, point.in_pool) for point in points] == [
        (0, 2, 2),
        (1, 2, 2),
        (2, 3, 2),
        (3, 3, 2),
    ]
    assert [point.share for point in points] == [1.0, 1.0, 2 / 3, 2 / 3]


def test_survival_measures_from_the_first_run_rather_than_the_first_success() -> None:
    report = history_of(
        scraped_run(DAY_ONE),
        scraped_run(days_after(DAY_ONE, 2)),
        lifetimes=(Lifetime("1.1.1.1:80", days_after(DAY_ONE, 1), None),),
    )

    assert [(point.day, point.at_risk) for point in survival(report).points] == [
        (0, 0),
        (1, 1),
        (2, 1),
    ]


def test_a_day_nobody_had_started_working_by_carries_no_share() -> None:
    report = history_of(
        scraped_run(DAY_ONE),
        scraped_run(days_after(DAY_ONE, 1)),
        lifetimes=(Lifetime("1.1.1.1:80", days_after(DAY_ONE, 1), None),),
    )

    first = survival(report).points[0]

    assert (first.at_risk, first.in_pool, first.share) == (0, 0, None)


def test_a_history_with_nothing_in_it_says_so() -> None:
    rendered = render_markdown(history_of())

    assert "No runs have been recorded yet" in rendered


def test_the_report_is_a_markdown_table_a_terminal_can_read() -> None:
    report = history_of(
        scraped_run(
            DAY_ONE,
            candidates=400,
            working=20,
            unreachable=370,
            rejected=10,
            contributions=(SourceYield("geonode", 300, 18),),
        ),
        scraped_run(days_after(DAY_ONE, 1), candidates=400, working=24),
        lifetimes=(Lifetime("1.1.1.1:80", DAY_ONE, None),),
        roster=("geonode", "broken"),
    )

    rendered = render_markdown(report)

    assert "| 1 | 2026-06-01 08:00 | 400 | 400 | 20 | 370 | 10 | 5.0% |" in rendered
    assert "| geonode | 1 | 300 | 18 | 6.0% | yielded |" in rendered
    assert "| broken | 0 | 0 | 0 | — | never answered |" in rendered
    assert "2026-06-01 08:00" in rendered
    assert "5.0%" in rendered


def test_the_report_shows_a_figure_it_does_not_have_as_a_dash() -> None:
    report = history_of(scraped_run(DAY_ONE, candidates=0, working=0))

    rendered = render_markdown(report)

    assert "| — |" in rendered
    assert "Not measured yet" in rendered


def test_the_history_is_read_out_of_a_pool(pool: Path) -> None:
    record = scraped_run(
        DAY_ONE,
        candidates=100,
        working=4,
        contributions=(SourceYield("geonode", 60, 4),),
    )

    record_run(record, str(pool))

    assert read_history(str(pool)).runs == (record,)


def test_a_pool_that_has_never_been_written_to_has_no_history(tmp_path: Path) -> None:
    """Asking a pool that does not exist is a question with the answer nothing,
    and it should not leave a database file behind for having been asked"""

    missing = tmp_path / "never-built.db"

    history = read_history(str(missing))

    assert (history.runs, history.lifetimes) == ((), ())
    assert not missing.exists()
    assert "No runs have been recorded yet" in render_markdown(history)
