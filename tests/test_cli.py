import asyncio
import logging
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field, fields
from datetime import datetime
from pathlib import Path

import pytest
from src.cli import (
    EXIT_FAILED,
    EXIT_INTERRUPTED,
    EXIT_OK,
    EXIT_USAGE,
    Command,
    build_parser,
    main,
    run_command,
    settings_from_args,
)
from src.logs import configure, level_for
from src.report import HistoryReport
from src.run import (
    Retention,
    RevalidationReport,
    RunReport,
    RunSettings,
    report_retention,
)
from src.storage import RunRecord, SourceYield

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def normal_voice() -> Iterator[None]:
    """A test that lowers or raises the voice hands it back afterwards, since
    the level a run speaks at is a fact about the process, not about a test"""

    yield
    configure(0)


@dataclass
class FakeRunner:
    """Runs no command, and records what the command line asked it to run"""

    report: RunReport | RevalidationReport | HistoryReport | None = None
    raise_on_run: BaseException | None = None
    asked: list[tuple[Command, RunSettings]] = field(default_factory=list)

    def __call__(
        self, command: Command, settings: RunSettings
    ) -> RunReport | RevalidationReport | HistoryReport:
        self.asked.append((command, settings))

        if self.raise_on_run is not None:
            raise self.raise_on_run

        return RunReport(0, 0, 0, 0) if self.report is None else self.report


def settings_for(argv: list[str]) -> RunSettings:
    return settings_from_args(build_parser().parse_args(argv))


def names(settings: RunSettings) -> set[str]:
    return {field.name for field in fields(settings)}


def test_no_arguments_prints_usage_and_leaves_the_pool_alone(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pool = tmp_path / "pool.db"
    runner = FakeRunner()

    assert main(["--db", str(pool)], runner) == EXIT_USAGE
    assert "usage" in capsys.readouterr().out.lower()
    assert runner.asked == []
    assert not pool.exists()


def test_verbosity_alone_is_no_action_either(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pool = tmp_path / "pool.db"
    runner = FakeRunner()

    assert main(["-vv", "--db", str(pool)], runner) == EXIT_USAGE
    assert "usage" in capsys.readouterr().out.lower()
    assert runner.asked == []


def test_a_run_is_told_every_value_the_command_line_gave(tmp_path: Path) -> None:
    pool = tmp_path / "pool.db"
    runner = FakeRunner()

    code = main(
        [
            "scrape",
            "--db",
            str(pool),
            "--target",
            "https://example.test/ip",
            "--user-agent",
            "curl/8",
            "--max-candidates",
            "7",
            "--chunk-size",
            "3",
            "--connector-limit",
            "4",
            "--connector-limit-per-host",
            "2",
            "--fetch-timeout",
            "2.5",
            "--probe-timeout",
            "1.5",
            "--chunk-delay-base",
            "0",
            "--chunk-delay-step",
            "0.25",
            "--failure-delay",
            "0.5",
            "--fetch-attempts",
            "5",
        ],
        runner,
    )

    assert code == EXIT_OK
    assert [command.__name__ for command, _ in runner.asked] == ["scrape"]
    assert runner.asked[0][1] == RunSettings(
        db_path=str(pool),
        validation_target="https://example.test/ip",
        user_agent="curl/8",
        max_candidates=7,
        chunk_size=3,
        connector_limit=4,
        connector_limit_per_host=2,
        fetch_timeout=2.5,
        probe_timeout=1.5,
        chunk_delay_base=0.0,
        chunk_delay_step=0.25,
        failure_delay=0.5,
        fetch_attempts=5,
    )


def test_revalidation_is_reachable_from_the_command_line(tmp_path: Path) -> None:
    pool = tmp_path / "pool.db"
    runner = FakeRunner()

    assert main(["revalidate", "--db", str(pool)], runner) == EXIT_OK
    assert [command.__name__ for command, _ in runner.asked] == ["revalidate"]


def test_both_subcommands_take_the_same_values(tmp_path: Path) -> None:
    pool = tmp_path / "pool.db"
    runner = FakeRunner()

    main(["scrape", "--db", str(pool), "--max-candidates", "1"], runner)
    main(["revalidate", "--db", str(pool), "--max-candidates", "1"], runner)

    assert [settings for _, settings in runner.asked] == [
        settings_for(["scrape", "--db", str(pool), "--max-candidates", "1"]),
        settings_for(["revalidate", "--db", str(pool), "--max-candidates", "1"]),
    ]


def test_a_run_keeps_every_value_it_was_not_asked_to_change() -> None:
    settings = settings_for(["scrape", "--db", "elsewhere.db"])

    assert settings == RunSettings(db_path="elsewhere.db")


TUNED = {
    "--db": ("db_path", "elsewhere.db"),
    "--target": ("validation_target", "https://example.test/ip"),
    "--user-agent": ("user_agent", "curl/8"),
    "--max-candidates": ("max_candidates", "7"),
    "--chunk-size": ("chunk_size", "3"),
    "--connector-limit": ("connector_limit", "4"),
    "--connector-limit-per-host": ("connector_limit_per_host", "2"),
    "--fetch-timeout": ("fetch_timeout", "2.5"),
    "--probe-timeout": ("probe_timeout", "1.5"),
    "--chunk-delay-base": ("chunk_delay_base", "1.5"),
    "--chunk-delay-step": ("chunk_delay_step", "0.25"),
    "--failure-delay": ("failure_delay", "0.5"),
    "--fetch-attempts": ("fetch_attempts", "5"),
    "--fetch-backoff-seconds": ("fetch_backoff_seconds", "3.5"),
    "--dns-cache-seconds": ("dns_cache_seconds", "60"),
}


@pytest.mark.parametrize(("flag", "value"), TUNED.items())
def test_a_flag_moves_one_value_and_leaves_the_rest_alone(
    flag: str, value: tuple[str, str]
) -> None:
    name, given = value
    settings = settings_for(["scrape", flag, given])
    defaults = settings_for(["scrape"])

    moved = {
        field.name
        for field in fields(settings)
        if getattr(settings, field.name) != getattr(defaults, field.name)
    }

    assert moved == {name}


def test_no_value_a_flag_does_not_name_is_left_behind() -> None:
    """Every field on the settings is either a value the command line can set or
    something a command line has no business setting, and there is no field the
    command line sets that the settings cannot hold"""

    settable = {name for name, _ in TUNED.values()} | {"verbosity"}
    untunable = {"sources"}

    assert names(RunSettings()) - settable == untunable


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["-q"], "WARNING"),
        ([], "INFO"),
        (["-v"], "DEBUG"),
        (["-vvv"], "DEBUG"),
    ],
)
def test_verbosity_chooses_how_much_gets_through(
    argv: list[str], expected: str
) -> None:
    verbosity = settings_for(["scrape", *argv]).verbosity

    assert logging.getLevelName(level_for(verbosity)) == expected


def test_a_run_that_was_interrupted_exits_without_a_traceback(
    capsys: pytest.CaptureFixture[str],
) -> None:
    runner = FakeRunner(raise_on_run=KeyboardInterrupt())

    code = main(["scrape"], runner)

    captured = capsys.readouterr()

    assert code == EXIT_INTERRUPTED
    assert "Traceback" not in captured.err
    assert "interrupt" in captured.err.lower()


def test_asking_for_more_and_for_less_at_once_is_refused(
    capsys: pytest.CaptureFixture[str],
) -> None:
    runner = FakeRunner()

    with pytest.raises(SystemExit) as refused:
        main(["scrape", "-v", "-q"], runner)

    assert refused.value.code == EXIT_USAGE
    assert runner.asked == []
    assert "not allowed with" in capsys.readouterr().err


def test_a_run_that_was_cancelled_ends_the_way_an_interrupted_one_does(
    capsys: pytest.CaptureFixture[str],
) -> None:
    runner = FakeRunner(raise_on_run=asyncio.CancelledError())

    code = main(["scrape"], runner)

    captured = capsys.readouterr()

    assert code == EXIT_INTERRUPTED
    assert "Traceback" not in captured.err
    assert "cancel" in captured.err.lower()


def test_a_run_that_failed_says_so_and_exits_non_zero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    runner = FakeRunner(raise_on_run=RuntimeError("the pool is on fire"))

    code = main(["scrape"], runner)

    captured = capsys.readouterr()

    assert code == EXIT_FAILED
    assert "the pool is on fire" in captured.err
    assert "Traceback" in captured.err


def test_the_default_runner_runs_the_command_it_is_given() -> None:
    async def command(settings: RunSettings) -> RunReport:
        return RunReport(0, 2, 2, 0)

    report = run_command(command, RunSettings())

    assert isinstance(report, RunReport)
    assert report.candidates == 2


@pytest.mark.parametrize("argv", [[], ["-q"], ["nonsense"], ["scrape", "--nope"]])
def test_the_module_entry_point_runs_and_reports_its_exit_code(
    argv: list[str],
) -> None:
    """Invoking the tool as a module is how it is meant to be run, and it has to
    resolve its own imports from wherever it is called"""

    finished = subprocess.run(
        [sys.executable, "-m", "src", *argv],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert finished.returncode == EXIT_USAGE
    assert "usage" in (finished.stdout + finished.stderr).lower()


def test_the_printed_lines_of_a_run_carry_a_level(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure(0)
    report_retention(Retention(working=[], evicted=2, stale=1))

    captured = capsys.readouterr().err

    assert "[POOL] INFO" in captured
    assert "Working: 0" in captured
    assert "Left on three strikes: 2" in captured


def test_a_quiet_run_prints_nothing_a_normal_one_would(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure(-1)
    report_retention(Retention(working=[], evicted=0, stale=0))

    assert "POOL" not in capsys.readouterr().err


def test_the_report_is_reachable_from_the_command_line(tmp_path: Path) -> None:
    pool = tmp_path / "pool.db"
    runner = FakeRunner()

    assert main(["report", "--db", str(pool)], runner) == EXIT_OK
    assert [command.__name__ for command, _ in runner.asked] == ["report"]


def test_the_report_answers_on_stdout_so_it_can_be_redirected(
    capsys: pytest.CaptureFixture[str],
) -> None:
    history = HistoryReport(
        runs=(
            RunRecord(
                started_at=datetime(2026, 6, 1, 8, 0),
                scraped=300,
                candidates=300,
                working=18,
                unreachable=280,
                rejected=2,
                contributions=(SourceYield("geonode", 300, 18),),
            ),
        ),
        lifetimes=(),
        roster=("geonode", "broken"),
    )

    main(["report"], FakeRunner(report=history))
    out = capsys.readouterr().out

    assert "| geonode | 1 | 300 | 18 | 6.0% | yielded |" in out
    assert "| broken | 0 | 0 | 0 | — | never answered |" in out
