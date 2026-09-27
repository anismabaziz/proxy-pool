"""The tool as you run it: one subcommand per run, every tuned value read from the
command line, and no action at all when no subcommand is given."""

import argparse
import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, replace
from typing import Any

import aiohttp

from src import run as pipeline
from src.http import HttpFetcher, HttpProber, probing_session
from src.logs import configure, get_logger
from src.report import HistoryReport, read_history, render_markdown
from src.run import RevalidationReport, RunReport, RunSettings
from src.sources import SOURCES

# a run that did what it was asked is a success, one that could not finish is a
# failure, one that was asked for nothing is a usage error, and one that was cut
# short says so the way a shell expects
EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130

type Report = RunReport | RevalidationReport | HistoryReport
type Command = Callable[[RunSettings], Coroutine[Any, Any, Report]]
type Runner = Callable[[Command, RunSettings], Report]


@dataclass(frozen=True)
class Tunable:
    """One value a run is allowed to tune, and the flag that sets it"""

    field: str
    flag: str
    read_as: Callable[[str], Any]
    help: str

    @property
    def dest(self) -> str:
        """The name the parser knows this value by"""

        return self.flag.lstrip("-")


# every tunable, in one place: adding one is a line here rather than a line in
# the parser, a line building the settings, and a third line keeping the two in
# step
TUNING: tuple[Tunable, ...] = (
    Tunable("db_path", "--db", str, "where the pool lives"),
    Tunable(
        "validation_target",
        "--target",
        str,
        "the endpoint every probe is sent to, reached through the candidate",
    ),
    Tunable("user_agent", "--user-agent", str, "what sources are asked as"),
    Tunable(
        "max_candidates",
        "--max-candidates",
        int,
        "how many candidates one run probes at most",
    ),
    Tunable(
        "chunk_size", "--chunk-size", int, "how many candidates are probed at once"
    ),
    Tunable(
        "connector_limit",
        "--connector-limit",
        int,
        "how many requests are in flight at once while probing",
    ),
    Tunable(
        "connector_limit_per_host",
        "--connector-limit-per-host",
        int,
        "how many of those go to one candidate at a time",
    ),
    Tunable(
        "fetch_timeout",
        "--fetch-timeout",
        float,
        "how long a source gets to answer, in seconds",
    ),
    Tunable(
        "probe_timeout",
        "--probe-timeout",
        float,
        "how long a candidate gets to carry the request, in seconds",
    ),
    Tunable(
        "fetch_attempts",
        "--fetch-attempts",
        int,
        "how many times a source is asked before it is given up on",
    ),
    Tunable(
        "fetch_backoff_seconds",
        "--fetch-backoff-seconds",
        float,
        "how long the first retry of a source waits, doubling each time after",
    ),
    Tunable(
        "dns_cache_seconds",
        "--dns-cache-seconds",
        int,
        "how long a name a probe resolved is remembered for",
    ),
    Tunable(
        "chunk_delay_base",
        "--chunk-delay-base",
        float,
        "how long a run waits before its first chunk of probes, in seconds",
    ),
    Tunable(
        "chunk_delay_step",
        "--chunk-delay-step",
        float,
        "how much longer each chunk after that waits, in seconds",
    ),
    Tunable(
        "failure_delay",
        "--failure-delay",
        float,
        "how long a run waits after a chunk of probes is lost, in seconds",
    ),
)


async def scrape(settings: RunSettings) -> RunReport:
    """A full scrape run over the real internet: every source, then every
    candidate that came back"""

    async with (
        aiohttp.ClientSession() as fetching,
        probing_session(settings) as probing,
    ):
        return await pipeline.run(
            settings, HttpFetcher(fetching, settings), HttpProber(probing, settings)
        )


async def revalidate(settings: RunSettings) -> RevalidationReport:
    """A revalidation run over the candidates the pool already holds"""

    async with probing_session(settings) as probing:
        return await pipeline.revalidate(settings, HttpProber(probing, settings))


async def report(settings: RunSettings) -> HistoryReport:
    """Read back what the runs so far recorded, with every source the tool knows
    about in the answer so a source that stopped answering is visible"""

    return read_history(settings.db_path, tuple(source.name for source in SOURCES))


# the runs a command line can name, and what each one is for
SUBCOMMANDS: dict[str, tuple[Command, str]] = {
    "scrape": (scrape, "fetch every source and probe what it offered"),
    "revalidate": (
        revalidate,
        "probe the pool that already exists and let each candidate in or out",
    ),
    "report": (
        report,
        "read the runs recorded so far back as hit rate, yield, and survival",
    ),
}


def run_command(command: Command, settings: RunSettings) -> Report:
    """Run one command to completion on a fresh event loop"""

    return asyncio.run(command(settings))


def common_arguments() -> argparse.ArgumentParser:
    """Everything both subcommands take, so neither run can be started with a
    value the other would refuse"""

    common = argparse.ArgumentParser(add_help=False)
    defaults = RunSettings()

    # asking for more and asking for less at the same time is a contradiction,
    # and one this tool has no way to resolve in the user's favour
    voice = common.add_mutually_exclusive_group()
    voice.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="say more: -v for what a run measured, -vv for every candidate's fate",
    )
    voice.add_argument(
        "-q", "--quiet", action="store_true", help="say only what went wrong"
    )

    for tunable in TUNING:
        common.add_argument(
            tunable.flag,
            dest=tunable.dest,
            type=tunable.read_as,
            default=getattr(defaults, tunable.field),
            help=tunable.help,
        )

    return common


def build_parser() -> argparse.ArgumentParser:
    """The whole command line, whose only optional part is which run to make"""

    common = common_arguments()
    parser = argparse.ArgumentParser(
        prog="python -m src",
        parents=[common],
        description=(
            "Scrape public proxy lists, probe what comes back, and keep the pool "
            "honest."
        ),
    )
    # deliberately not required: a bare invocation prints usage and stops,
    # rather than defaulting to a run that rewrites a pool
    named = parser.add_subparsers(dest="subcommand", metavar="SUBCOMMAND")

    for name, (command, summary) in SUBCOMMANDS.items():
        named.add_parser(name, parents=[common], help=summary).set_defaults(
            command=command
        )

    return parser


def settings_from_args(args: argparse.Namespace) -> RunSettings:
    """The settings a run gets: every tuned value as the command line gave it,
    and every other value as the tool ships with"""

    tuned = {tunable.field: getattr(args, tunable.dest) for tunable in TUNING}

    return replace(
        RunSettings(),
        **tuned,
        # quiet is the only thing that lowers the voice, and it says so by
        # counting below the run's own
        verbosity=-1 if args.quiet else args.verbose,
    )


def present(outcome: Report) -> None:
    """What a subcommand has to say for itself. A run speaks through the log,
    where a line can be filtered or silenced; the report is the answer to a
    question, so it goes to stdout where it can be read, piped, or redirected
    into a document"""

    if isinstance(outcome, HistoryReport):
        print(render_markdown(outcome))


def main(argv: list[str] | None = None, execute: Runner = run_command) -> int:
    """What the tool was asked to do, and what it did about it"""

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.subcommand is None:
        # no subcommand named means no run started: a careless invocation stops
        # here with the pool exactly as it found it
        parser.print_usage()
        return EXIT_USAGE

    settings = settings_from_args(args)
    configure(settings.verbosity)
    logger = get_logger("FINISH")

    try:
        present(execute(args.command, settings))
    except KeyboardInterrupt:
        logger.warning("Interrupted before the run finished")
        return EXIT_INTERRUPTED
    except asyncio.CancelledError:
        # something upstream withdrew the run rather than the person at the
        # keyboard stopping it, which ends the same way
        logger.warning("Cancelled before the run finished")
        return EXIT_INTERRUPTED
    except Exception as failure:
        # the traceback comes with it, since a bug inside a run is exactly what
        # a quiet reader needs and what a cron run can afford to lose
        logger.error("The run stopped: %s", failure, exc_info=failure)
        return EXIT_FAILED

    return EXIT_OK
