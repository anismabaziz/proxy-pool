import asyncio
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from src.capabilities import Fetcher, ProbeOutcome, Prober
from src.ingest import extract
from src.sources import SOURCES, Source
from src.storage import (
    DEFAULT_DB_PATH,
    get_proxies,
    init_db,
    remove_proxies,
    save_working_proxies,
    update_proxies,
)

DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"


@dataclass(frozen=True)
class RunSettings:
    """Every value a run is allowed to tune"""

    sources: tuple[Source, ...] = SOURCES
    db_path: str = DEFAULT_DB_PATH
    probe_url: str = "http://httpbin.org/ip"
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


@dataclass
class RunReport:
    """What a full scrape run found"""

    harvested: int
    candidates: int
    probed: int
    failed_chunks: int
    working: list[ProbeOutcome] = field(default_factory=list)
    saved: int = 0


@dataclass
class RevalidationReport:
    """What a revalidation run found among the stored proxies"""

    candidates: int
    failed_chunks: int
    updated: int
    removed: int
    working: list[ProbeOutcome] = field(default_factory=list)


def addresses(outcomes: Sequence[ProbeOutcome]) -> list[tuple[str, int]]:
    """The working outcomes as the "ip:port, latency" pairs storage stores"""

    return [(outcome.proxy, outcome.latency_ms) for outcome in outcomes]


def chunks(candidates: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for start in range(0, len(candidates), size):
        yield candidates[start : start + size]


async def probe_in_chunks(
    prober: Prober,
    candidates: Sequence[str],
    settings: RunSettings,
    label: str,
) -> tuple[list[ProbeOutcome], int]:
    """Probe candidates in chunks, pausing between chunks and surviving a bad one"""

    working: list[ProbeOutcome] = []
    failed_chunks = 0

    for index, chunk in enumerate(chunks(candidates, settings.chunk_size)):
        await asyncio.sleep(
            settings.chunk_delay_base + index * settings.chunk_delay_step
        )

        try:
            outcomes = await asyncio.gather(*(prober.probe(c) for c in chunk))
        except Exception as e:
            print(f"[ERROR] Chunk failed: {e}")
            failed_chunks += 1
            await asyncio.sleep(settings.failure_delay)
            continue

        chunk_working = [outcome for outcome in outcomes if outcome.is_working]
        working.extend(chunk_working)
        print(f"[{label}] Chunk {index + 1}: Working {len(chunk_working)}")

    return working, failed_chunks


async def run(settings: RunSettings, fetcher: Fetcher, prober: Prober) -> RunReport:
    """Fetch every source, probe what came back, and store what still works"""

    init_db(settings.db_path)

    payloads = await asyncio.gather(*(fetcher.fetch(s) for s in settings.sources))

    harvested: list[str] = []
    for payload in payloads:
        proxies = await extract(payload.text)
        print(f"[✓] {payload.source}: found {len(proxies)} proxies")
        harvested.extend(proxies)

    unique = list(set(harvested))
    print(f"[RAW] Total unique proxies harvested: {len(unique)}")

    candidates = unique[: settings.max_candidates]
    working, failed_chunks = await probe_in_chunks(
        prober, candidates, settings, "VALID"
    )

    save_working_proxies(addresses(working), settings.db_path)
    print(f"\n[DB] Saved proxies to db: {len(working)}")
    print(f"\n[FINISH] Working proxies: {len(working)}")

    return RunReport(
        harvested=len(unique),
        candidates=len(candidates),
        probed=len(candidates),
        failed_chunks=failed_chunks,
        working=working,
        saved=len(working),
    )


async def revalidate(settings: RunSettings, prober: Prober) -> RevalidationReport:
    """Probe the stored proxies again, refresh the ones that work, drop the rest"""

    stored = get_proxies(settings.db_path)
    print(f"[START] Proxy list: {len(stored)} proxies")

    working, failed_chunks = await probe_in_chunks(
        prober, stored, settings, "REVALIDATE"
    )

    targets = addresses(working)
    _, updated, last_checked = update_proxies(targets, settings.db_path)
    print(f"[DB] Updated working proxies: {updated}")

    _, removed = remove_proxies(last_checked, settings.db_path)
    print(f"[DB] Removed dead proxies: {removed}")

    print(f"[FINISH] Working proxies: {len(targets)}")

    return RevalidationReport(
        candidates=len(stored),
        failed_chunks=failed_chunks,
        updated=updated,
        removed=removed,
        working=working,
    )
