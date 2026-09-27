"""What the pool remembers, and what can be said about it.

Every run leaves a row behind. This module reads those rows back and turns them
into the three questions a reader of the numbers would ask: what share of the
probes worked, what each source is worth, and how long a working candidate goes
on working. Where an answer needs history the pool has not accumulated yet, it is
left out rather than filled in.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from src.storage import Lifetime, RunRecord, read_lifetimes, read_runs

# the bucket a survival figure is measured in. A candidate that dies between two
# runs dies on one of these days, and a curve in hours would be a curve of one
# reading per run pretending to be finer than it is
DAY = timedelta(days=1)

# what the report prints where it has no figure to print, rather than a zero,
# which reads as a measurement
NO_FIGURE = "—"


@dataclass(frozen=True)
class HistoryReport:
    """Everything the pool has to say, and the sources the tool knows about"""

    runs: tuple[RunRecord, ...]
    lifetimes: tuple[Lifetime, ...]
    roster: tuple[str, ...] = ()


@dataclass(frozen=True)
class RunHitRate:
    """One run, as a share of the probes that reached an answer"""

    started_at: datetime
    scraped: int
    candidates: int
    working: int
    unreachable: int
    rejected: int

    @property
    def measured(self) -> int:
        """The candidates whose probes reached one of the three states. It is
        fewer than the candidates the run took on whenever a chunk of probes was
        lost, and a candidate nobody managed to probe is not a candidate that
        did not work"""

        return self.working + self.unreachable + self.rejected

    @property
    def hit_rate(self) -> float | None:
        """The share of the run's probes that carried a request, over the probes
        that reached an answer rather than over everything the run took on"""

        return share_of(self.working, self.measured)


@dataclass(frozen=True)
class SourceStanding:
    """One source, as a share of the candidates it got measured"""

    source: str
    asked: bool
    runs: int
    candidates: int
    working: int

    @property
    def yield_rate(self) -> float | None:
        """The share of a source's own candidates that worked, which is a
        different question from how well the run as a whole did"""

        return share_of(self.working, self.candidates)

    @property
    def standing(self) -> str:
        """What a source is worth in words. A source that never answered and a
        source whose candidates never worked are both worth nothing, and they
        are not the same thing: the first is a source to drop, the second is a
        source to keep scraping. A source nobody has asked yet is neither, since
        nothing is known about it"""

        if not self.asked:
            return "not asked yet"

        if self.runs == 0:
            return "never answered"

        if self.working == 0:
            return "nothing worked"

        return "yielded"


@dataclass(frozen=True)
class SurvivalPoint:
    """One day of the survival curve, counted as it stood at the start of the
    day: a candidate that left during a day is still counted in that day"""

    day: int
    at_risk: int
    in_pool: int

    @property
    def share(self) -> float | None:
        return share_of(self.in_pool, self.at_risk)


@dataclass(frozen=True)
class Survival:
    """The share of the candidates that started working which the pool still
    holds, a day at a time. The points are empty when the recorded runs have not
    spanned enough time for the figure to mean anything, and the reason says so"""

    points: tuple[SurvivalPoint, ...]
    undefined_because: str = ""

    @property
    def measured(self) -> bool:
        return bool(self.points)


def read_history(db_path: str, roster: Sequence[str] = ()) -> HistoryReport:
    """Everything the pool at db_path remembers about past runs"""

    return HistoryReport(
        runs=read_runs(db_path),
        lifetimes=read_lifetimes(db_path),
        roster=tuple(roster),
    )


def hit_rate_per_run(history: HistoryReport) -> tuple[RunHitRate, ...]:
    """The share of probes that carried a request, run by run, oldest first"""

    return tuple(
        RunHitRate(
            started_at=record.started_at,
            scraped=record.scraped,
            candidates=record.candidates,
            working=record.working,
            unreachable=record.unreachable,
            rejected=record.rejected,
        )
        for record in _chronological(history)
    )


def yield_per_source(history: HistoryReport) -> tuple[SourceStanding, ...]:
    """What each source is worth, with every source the tool knows about in the
    answer even when it answered nothing at all"""

    # every run that scraped asked every source, so a source missing from one of
    # those runs answered nothing rather than having gone unasked
    asked = any(record.scraped for record in history.runs)
    counted: dict[str, tuple[int, int, int]] = {
        source: (0, 0, 0) for source in (*history.roster, *_named(history))
    }

    for record in history.runs:
        for contribution in record.contributions:
            runs, candidates, working = counted.get(contribution.source, (0, 0, 0))
            counted[contribution.source] = (
                runs + 1,
                candidates + contribution.candidates,
                working + contribution.working,
            )

    standings = [
        SourceStanding(
            source=source,
            asked=asked,
            runs=runs,
            candidates=candidates,
            working=working,
        )
        for source, (runs, candidates, working) in counted.items()
    ]

    # the sources that answered come first, since a source that never answered is
    # a question the reader has to act on rather than a number to read
    return tuple(
        sorted(standings, key=lambda standing: (standing.runs == 0, standing.source))
    )


def survival(history: HistoryReport) -> Survival:
    """How long a working candidate went on working, measured from the first run
    recorded rather than from the first success, since a candidate cannot have
    worked before the tool started looking"""

    runs = _chronological(history)

    if not runs:
        return Survival((), "no runs have been recorded yet")

    if not history.lifetimes:
        return Survival((), "nothing has worked yet, so there is nothing to follow")

    first, span = runs[0].started_at, runs[-1].started_at - runs[0].started_at

    if span < DAY:
        return Survival(
            (),
            f"the recorded runs span {describe(span)}, and a figure needs at least "
            f"a day",
        )

    points = tuple(
        SurvivalPoint(day=day, at_risk=at_risk, in_pool=in_pool)
        for day, at_risk, in_pool in (
            (day, *_counted(history.lifetimes, first + day * DAY))
            for day in range(span.days + 1)
        )
    )

    return Survival(points)


def _chronological(history: HistoryReport) -> tuple[RunRecord, ...]:
    """The recorded runs in the order they happened, which the caller is not
    asked to have put them in already"""

    return tuple(sorted(history.runs, key=lambda record: record.started_at))


def _counted(lifetimes: Sequence[Lifetime], at: datetime) -> tuple[int, int]:
    """How many candidates had started working by a moment, and how many of
    those the pool still held then"""

    started = [spell for spell in lifetimes if spell.started_at <= at]
    held = [
        spell for spell in started if spell.ended_at is None or spell.ended_at >= at
    ]

    return len(started), len(held)


def _named(history: HistoryReport) -> tuple[str, ...]:
    """Every source a recorded run named that the tool's own list of sources does
    not, so a source dropped from the code still reports what it contributed"""

    seen = {
        contribution.source
        for record in history.runs
        for contribution in record.contributions
    }

    return tuple(sorted(seen - set(history.roster)))


def share_of(part: int, whole: int) -> float | None:
    """A share, or nothing at all when there is nothing to divide: a rate of zero
    is a measurement, and a run that measured nothing has no measurement to make"""

    return None if whole == 0 else part / whole


def describe(moment: timedelta) -> str:
    """How long a stretch of time was, in the words a reader would use"""

    seconds = int(moment.total_seconds())

    if seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minute" if minutes == 1 else f"{minutes} minutes"

    hours = seconds // 3600
    return f"{hours} hour" if hours == 1 else f"{hours} hours"


def share(rate: float | None) -> str:
    """A rate as the report prints it, or as the report admits it has none"""

    return NO_FIGURE if rate is None else f"{rate * 100:.1f}%"


def render_markdown(history: HistoryReport) -> str:
    """The history as markdown, which a terminal reads and a person can paste
    into a document without a generator having to stay honest about it"""

    if not history.runs:
        return "# Run history\n\nNo runs have been recorded yet.\n"

    return "\n".join(
        (
            "# Run history",
            "",
            "## Hit rate per run",
            "",
            _table(
                (
                    "Run",
                    "Started",
                    "Scraped",
                    "Candidates",
                    "Working",
                    "Unreachable",
                    "Rejected",
                    "Hit rate",
                ),
                [
                    (
                        str(number),
                        when(line.started_at),
                        str(line.scraped),
                        str(line.candidates),
                        str(line.working),
                        str(line.unreachable),
                        str(line.rejected),
                        share(line.hit_rate),
                    )
                    for number, line in enumerate(hit_rate_per_run(history), start=1)
                ],
            ),
            "",
            "## Yield per source",
            "",
            "What each source got measured in, and how much of it worked. A run"
            " draws its candidates in turn and stops at its budget, so a source"
            " crowded out of a run is not in that run's yield.",
            "",
            _table(
                ("Source", "Runs", "Candidates", "Working", "Yield", "Standing"),
                [
                    (
                        standing.source,
                        str(standing.runs),
                        str(standing.candidates),
                        str(standing.working),
                        share(standing.yield_rate),
                        standing.standing,
                    )
                    for standing in yield_per_source(history)
                ],
            ),
            "",
            "## Survival",
            "",
            *_survival_section(survival(history)),
        )
    )


def _survival_section(figure: Survival) -> tuple[str, ...]:
    if not figure.measured:
        return ("Not measured yet: " + figure.undefined_because + ".",)

    return (
        "The share of the candidates that started working which the pool still"
        " holds, counted at the start of each day since the first run.",
        "",
        _table(
            ("Day", "At risk", "In the pool", "Share"),
            [
                (
                    str(point.day),
                    str(point.at_risk),
                    str(point.in_pool),
                    share(point.share),
                )
                for point in figure.points
            ],
        ),
    )


def when(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M")


def _table(headings: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    """A markdown table: the pipe rows a terminal draws as columns and a reader
    pastes into a document unchanged"""

    lines = [
        "| " + " | ".join(headings) + " |",
        "| " + " | ".join("---" for _ in headings) + " |",
        *("| " + " | ".join(row) + " |" for row in rows),
    ]

    return "\n".join(lines)
