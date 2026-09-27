import asyncio
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from itertools import zip_longest

from src.capabilities import Fetcher, ProbeOutcome, Prober
from src.sources import SOURCES, Source
from src.storage import (
    DEFAULT_DB_PATH,
    drop_stale,
    get_proxies,
    init_db,
    record_outcome,
    stale_before,
)

DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

# an HTTPS endpoint that answers with the caller's own address as bare text, so
# that carrying the request is something we can see in the body rather than
# something we take on trust from a status code
DEFAULT_VALIDATION_TARGET = "https://api.ipify.org"


@dataclass(frozen=True)
class RunSettings:
    """Every value a run is allowed to tune"""

    sources: tuple[Source, ...] = SOURCES
    db_path: str = DEFAULT_DB_PATH
    validation_target: str = DEFAULT_VALIDATION_TARGET
    user_agent: str = DEFAULT_USER_AGENT
    max_candidates: int = 5000
    chunk_size: int = 300
    connector_limit: int = 25
    connector_limit_per_host: int = 10
    fetch_timeout: float = 10
    probe_timeout: float = 5
    chunk_delay_base: float = 2
    chunk_delay_step: float = 0.5
    failure_delay: float = 10


@dataclass(frozen=True)
class SourceContribution:
    """What one source offered a run"""

    source: str
    payload_format: str
    candidates: int


@dataclass
class RunReport:
    """What a full scrape run found"""

    scraped: int
    candidates: int
    probed: int
    failed_chunks: int
    contributions: tuple[SourceContribution, ...] = ()
    working: list[ProbeOutcome] = field(default_factory=list)
    saved: int = 0
    evicted: int = 0
    stale: int = 0


@dataclass
class RevalidationReport:
    """What a revalidation run found among the stored proxies"""

    candidates: int
    failed_chunks: int
    updated: int
    evicted: int
    stale: int
    working: list[ProbeOutcome] = field(default_factory=list)


@dataclass(frozen=True)
class Retention:
    """What the pool's own rules did to a run's outcomes"""

    working: list[ProbeOutcome]
    evicted: int
    stale: int


def apply_retention(outcomes: Sequence[ProbeOutcome], db_path: str) -> Retention:
    """Fold every probe result into the pool, then drop whatever no recent run
    probed. A candidate the pool carries but nobody has measured is not data,
    and a candidate that has missed three times running has been"""

    working = [outcome for outcome in outcomes if outcome.is_working]
    evicted = 0

    for outcome in outcomes:
        if record_outcome(outcome, db_path):
            evicted += 1

    return Retention(
        working=working,
        evicted=evicted,
        stale=drop_stale(stale_before(datetime.now()), db_path),
    )


def report_retention(retention: Retention) -> None:
    """What the pool's rules did, in the words a run uses to describe itself"""

    print(f"[POOL] Working: {len(retention.working)}")
    print(f"[POOL] Left on three strikes: {retention.evicted}")
    print(f"[POOL] Left unprobed: {retention.stale}")


def chunks(candidates: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for start in range(0, len(candidates), size):
        yield candidates[start : start + size]


def take_turns(per_source: Sequence[Sequence[str]]) -> list[str]:
    """Every source's candidates, drawn in turn, so a source with a long list
    cannot fill the whole budget and leave the rest unrepresented"""

    return [
        candidate
        for turn in zip_longest(*per_source, fillvalue=None)
        for candidate in turn
        if candidate is not None
    ]


async def scrape_sources(
    sources: Sequence[Source], fetcher: Fetcher
) -> list[tuple[Source, list[str]]]:
    """Read every source in its own declared format, and return what each offered"""

    payloads = await asyncio.gather(*(fetcher.fetch(s) for s in sources))

    scraped: list[tuple[Source, list[str]]] = []

    for source, payload in zip(sources, payloads, strict=True):
        candidates = source.scrape(payload.text)
        scraped.append((source, candidates))
        print(f"[✓] {source.name}: found {len(candidates)} candidates")

    return scraped


async def probe_in_chunks(
    prober: Prober,
    candidates: Sequence[str],
    settings: RunSettings,
    label: str,
) -> tuple[list[ProbeOutcome], int]:
    """Probe candidates in chunks, pausing between chunks and surviving a bad
    one, and hand back every outcome, since what did not work is what retention
    has to reason about"""

    outcomes: list[ProbeOutcome] = []
    failed_chunks = 0

    for index, chunk in enumerate(chunks(candidates, settings.chunk_size)):
        await asyncio.sleep(
            settings.chunk_delay_base + index * settings.chunk_delay_step
        )

        try:
            chunk_outcomes = await asyncio.gather(*(prober.probe(c) for c in chunk))
        except Exception as e:
            print(f"[ERROR] Chunk failed: {e}")
            failed_chunks += 1
            await asyncio.sleep(settings.failure_delay)
            continue

        outcomes.extend(chunk_outcomes)
        working = sum(1 for outcome in chunk_outcomes if outcome.is_working)
        print(f"[{label}] Chunk {index + 1}: Working {working}")

    return outcomes, failed_chunks


async def run(settings: RunSettings, fetcher: Fetcher, prober: Prober) -> RunReport:
    """Fetch every source, probe what came back, and store what still works"""

    init_db(settings.db_path)

    scraped = await scrape_sources(settings.sources, fetcher)

    unique = list(dict.fromkeys(take_turns([found for _, found in scraped])))
    print(f"[RAW] Total unique candidates scraped: {len(unique)}")

    candidates = unique[: settings.max_candidates]
    outcomes, failed_chunks = await probe_in_chunks(
        prober, candidates, settings, "VALID"
    )
    retention = apply_retention(outcomes, settings.db_path)
    report_retention(retention)

    print(f"[FINISH] Probed {len(candidates)} candidates")

    return RunReport(
        scraped=len(unique),
        candidates=len(candidates),
        probed=len(candidates),
        failed_chunks=failed_chunks,
        contributions=tuple(
            SourceContribution(
                source=source.name,
                payload_format=source.payload_format.value,
                candidates=len(found),
            )
            for source, found in scraped
        ),
        working=retention.working,
        saved=len(retention.working),
        evicted=retention.evicted,
        stale=retention.stale,
    )


async def revalidate(settings: RunSettings, prober: Prober) -> RevalidationReport:
    """Probe the stored proxies again, and let each one in or out of the pool
    according to what the probe found"""

    init_db(settings.db_path)

    stored = get_proxies(settings.db_path)
    print(f"[START] Proxy list: {len(stored)} proxies")

    outcomes, failed_chunks = await probe_in_chunks(
        prober, stored, settings, "REVALIDATE"
    )
    retention = apply_retention(outcomes, settings.db_path)
    report_retention(retention)

    print(f"[FINISH] Revalidated {len(stored)} candidates")

    return RevalidationReport(
        candidates=len(stored),
        failed_chunks=failed_chunks,
        updated=len(retention.working),
        evicted=retention.evicted,
        stale=retention.stale,
        working=retention.working,
    )
