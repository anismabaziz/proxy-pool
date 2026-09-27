import asyncio
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from src.capabilities import Outcome, Payload, ProbeOutcome
from src.run import RunSettings, revalidate, run
from src.sources import EmptySourceError, PayloadFormat, Source
from src.storage import get_proxies, read_runs, record_outcome


@dataclass
class FakeFetcher:
    bodies: dict[str, str]
    asked: list[str] = field(default_factory=list)

    async def fetch(self, source: Source) -> Payload:
        self.asked.append(source.name)
        return Payload(source=source.name, text=self.bodies.get(source.name, ""))


@dataclass
class FakeProbe:
    working: set[str]
    latency_ms: int = 100
    otherwise: dict[str, Outcome] = field(default_factory=dict)
    asked: list[str] = field(default_factory=list)

    async def probe(self, candidate: str) -> ProbeOutcome:
        self.asked.append(candidate)
        state = (
            Outcome.WORKING
            if candidate in self.working
            else self.otherwise.get(candidate, Outcome.UNREACHABLE)
        )

        return ProbeOutcome(
            proxy=candidate,
            state=state,
            latency_ms=self.latency_ms if state is Outcome.WORKING else None,
        )


def run_settings(sources: tuple[Source, ...], pool: Path) -> RunSettings:
    """The same settings a real run uses, minus the wait between chunks"""

    return RunSettings(
        sources=sources,
        db_path=str(pool),
        max_candidates=1000,
        chunk_size=2,
        chunk_delay_base=0,
        chunk_delay_step=0,
        failure_delay=0,
    )


def text_source(name: str) -> Source:
    return Source(name, f"https://{name}.example", PayloadFormat.ADDRESS_LIST)


def sources() -> tuple[Source, ...]:
    return text_source("first"), text_source("second")


async def test_every_source_is_fetched(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80\n2.2.2.2:80", "second": "3.3.3.3:80"})

    await run(run_settings(sources(), pool), fetcher, FakeProbe(working=set()))

    assert sorted(fetcher.asked) == ["first", "second"]


async def test_only_candidates_reaching_the_probe_are_probed(pool: Path) -> None:
    fetcher = FakeFetcher(
        {
            "first": "1.1.1.1:80\n2.2.2.2:80",
            "second": "2.2.2.2:80\n3.3.3.3:80",
        }
    )
    probe = FakeProbe(working={"1.1.1.1:80"})

    report = await run(run_settings(sources(), pool), fetcher, probe)

    assert sorted(probe.asked) == ["1.1.1.1:80", "2.2.2.2:80", "3.3.3.3:80"]
    assert report.scraped == 3
    assert report.candidates == 3
    assert report.probed == 3


async def test_duplicates_across_sources_are_probed_once(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "1.1.1.1:80"})
    probe = FakeProbe(working=set())

    await run(run_settings(sources(), pool), fetcher, probe)

    assert probe.asked == ["1.1.1.1:80"]


async def test_a_probe_failure_costs_the_whole_chunk(pool: Path) -> None:
    class ExplodingProbe(FakeProbe):
        async def probe(self, candidate: str) -> ProbeOutcome:
            self.asked.append(candidate)
            raise RuntimeError("connection reset")

    fetcher = FakeFetcher(
        {
            "first": "1.1.1.1:80\n2.2.2.2:80",
            "second": "1.1.1.1:80\n2.2.2.2:80",
        }
    )
    probe = ExplodingProbe(working=set())

    report = await run(run_settings(sources(), pool), fetcher, probe)

    assert report.failed_chunks == 1
    assert report.working == []


async def test_an_interrupted_run_is_not_swallowed(pool: Path) -> None:
    """A cancellation is an answer, not a failure to swallow: a run that takes
    one stops rather than carrying on with half its chunks"""

    class InterruptedProbe(FakeProbe):
        async def probe(self, candidate: str) -> ProbeOutcome:
            raise asyncio.CancelledError

    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "2.2.2.2:80"})

    with pytest.raises(asyncio.CancelledError):
        await run(
            run_settings(sources(), pool), fetcher, InterruptedProbe(working=set())
        )


async def test_working_proxies_are_saved(pool: Path) -> None:
    fetcher = FakeFetcher(
        {
            "first": "1.1.1.1:80\n2.2.2.2:80",
            "second": "1.1.1.1:80\n2.2.2.2:80",
        }
    )

    report = await run(
        run_settings(sources(), pool), fetcher, FakeProbe(working={"1.1.1.1:80"})
    )

    assert [(w.proxy, w.latency_ms) for w in report.working] == [("1.1.1.1:80", 100)]
    assert report.saved == 1
    assert get_proxies(str(pool)) == ["1.1.1.1:80"]


def stored(pool: Path, *addresses: str) -> None:
    """A pool holding candidates that have all worked at least once"""

    for address in addresses:
        record_outcome(ProbeOutcome(address, Outcome.WORKING, 120), str(pool))


async def test_revalidation_keeps_a_candidate_that_missed_once(pool: Path) -> None:
    stored(pool, "1.1.1.1:80", "2.2.2.2:80")

    report = await revalidate(
        run_settings(sources(), pool), FakeProbe(working={"1.1.1.1:80"})
    )

    assert report.candidates == 2
    assert report.updated == 1
    assert report.evicted == 0
    assert sorted(get_proxies(str(pool))) == ["1.1.1.1:80", "2.2.2.2:80"]


async def test_revalidation_keeps_a_candidate_the_target_refuses(pool: Path) -> None:
    stored(pool, "1.1.1.1:80", "2.2.2.2:80")

    probe = FakeProbe(
        working={"1.1.1.1:80"}, otherwise={"2.2.2.2:80": Outcome.REJECTED}
    )
    report = await revalidate(run_settings(sources(), pool), probe)

    assert report.evicted == 0
    assert sorted(get_proxies(str(pool))) == ["1.1.1.1:80", "2.2.2.2:80"]


async def test_revalidation_evicts_a_candidate_on_its_third_miss(pool: Path) -> None:
    stored(pool, "1.1.1.1:80", "2.2.2.2:80")
    settings = run_settings(sources(), pool)
    gone = FakeProbe(working=set())
    staying = FakeProbe(working={"2.2.2.2:80"})

    await revalidate(settings, gone)
    await revalidate(settings, gone)
    assert sorted(get_proxies(str(pool))) == ["1.1.1.1:80", "2.2.2.2:80"]

    report = await revalidate(settings, staying)

    assert report.evicted == 1
    assert get_proxies(str(pool)) == ["2.2.2.2:80"]


async def test_a_candidate_working_again_starts_its_strikes_over(pool: Path) -> None:
    stored(pool, "1.1.1.1:80")
    settings = run_settings(sources(), pool)
    gone = FakeProbe(working=set())

    await revalidate(settings, gone)
    await revalidate(settings, gone)
    await revalidate(settings, FakeProbe(working={"1.1.1.1:80"}))

    await revalidate(settings, gone)
    await revalidate(settings, gone)

    assert get_proxies(str(pool)) == ["1.1.1.1:80"]


async def test_a_run_drops_a_candidate_it_never_probed(pool: Path) -> None:
    with closing(sqlite3.connect(pool)) as conn:
        conn.execute("INSERT INTO proxies (address) VALUES (?)", ("9.9.9.9:80",))
        conn.commit()

    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "2.2.2.2:80"})

    report = await run(
        run_settings(sources(), pool), fetcher, FakeProbe(working={"1.1.1.1:80"})
    )

    assert report.stale == 1
    assert get_proxies(str(pool)) == ["1.1.1.1:80"]


GEONODE_PAYLOAD = json.dumps(
    {
        "data": [
            {"ip": "1.1.1.1", "port": "8080", "protocols": ["http"]},
            {"ip": "2.2.2.2", "port": "3128", "protocols": ["http"]},
        ],
        "total": 2,
    }
)


async def test_a_json_source_offers_the_addresses_it_splits_into_fields(
    pool: Path,
) -> None:
    source = Source("geonode", "https://geonode.example", PayloadFormat.GEONODE_JSON)
    fetcher = FakeFetcher({"geonode": GEONODE_PAYLOAD})
    probe = FakeProbe(working=set())

    report = await run(run_settings((source,), pool), fetcher, probe)

    assert sorted(probe.asked) == ["1.1.1.1:8080", "2.2.2.2:3128"]
    assert report.scraped == 2


async def test_a_source_that_offers_nothing_is_an_error(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "no addresses in here"})

    with pytest.raises(EmptySourceError, match="second"):
        await run(run_settings(sources(), pool), fetcher, FakeProbe(working=set()))


async def test_a_json_source_without_entries_is_an_error(pool: Path) -> None:
    source = Source("geonode", "https://geonode.example", PayloadFormat.GEONODE_JSON)
    fetcher = FakeFetcher({"geonode": json.dumps({"data": []})})

    with pytest.raises(EmptySourceError, match="geonode"):
        await run(run_settings((source,), pool), fetcher, FakeProbe(working=set()))


async def test_a_json_source_answering_with_an_error_page_is_an_error(
    pool: Path,
) -> None:
    source = Source("geonode", "https://geonode.example", PayloadFormat.GEONODE_JSON)
    fetcher = FakeFetcher({"geonode": "<html><body>502 Bad Gateway</body></html>"})

    with pytest.raises(EmptySourceError, match="geonode"):
        await run(run_settings((source,), pool), fetcher, FakeProbe(working=set()))


async def test_a_run_reports_what_each_source_contributed(pool: Path) -> None:
    fetcher = FakeFetcher(
        {
            "first": "1.1.1.1:80\n2.2.2.2:80",
            "second": "2.2.2.2:80\n3.3.3.3:80",
        }
    )

    report = await run(run_settings(sources(), pool), fetcher, FakeProbe(working=set()))

    assert [(c.source, c.candidates) for c in report.contributions] == [
        ("first", 2),
        ("second", 2),
    ]


async def test_a_long_source_cannot_crowd_out_a_short_one(pool: Path) -> None:
    fetcher = FakeFetcher(
        {
            "first": "".join(f"1.1.1.{n}:80\n" for n in range(10)),
            "second": "9.9.9.9:80\n",
        }
    )
    probe = FakeProbe(working=set())
    settings = RunSettings(
        sources=sources(),
        db_path=str(pool),
        max_candidates=2,
        chunk_size=10,
        chunk_delay_base=0,
        chunk_delay_step=0,
        failure_delay=0,
    )

    await run(settings, fetcher, probe)

    assert probe.asked == ["1.1.1.0:80", "9.9.9.9:80"]


async def test_a_candidate_answering_with_the_wrong_body_stays_out_of_the_pool(
    pool: Path,
) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "2.2.2.2:80"})
    probe = FakeProbe(
        working={"2.2.2.2:80"}, otherwise={"1.1.1.1:80": Outcome.REJECTED}
    )

    report = await run(run_settings(sources(), pool), fetcher, probe)

    assert [outcome.proxy for outcome in report.working] == ["2.2.2.2:80"]
    assert get_proxies(str(pool)) == ["2.2.2.2:80"]


async def test_an_unreachable_candidate_stays_out_of_the_pool(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "2.2.2.2:80"})
    probe = FakeProbe(
        working={"2.2.2.2:80"}, otherwise={"1.1.1.1:80": Outcome.UNREACHABLE}
    )

    report = await run(run_settings(sources(), pool), fetcher, probe)

    assert [outcome.proxy for outcome in report.working] == ["2.2.2.2:80"]
    assert get_proxies(str(pool)) == ["2.2.2.2:80"]


def test_the_default_validation_target_is_https() -> None:
    assert RunSettings().validation_target.startswith("https://")


def ago(days: float) -> str:
    return (datetime.now() - timedelta(days=days)).isoformat()


async def test_a_run_records_when_it_happened_and_what_it_found(pool: Path) -> None:
    fetcher = FakeFetcher(
        {
            "first": "1.1.1.1:80\n2.2.2.2:80",
            "second": "3.3.3.3:80",
        }
    )
    probe = FakeProbe(
        working={"2.2.2.2:80"}, otherwise={"1.1.1.1:80": Outcome.REJECTED}
    )

    started_before = datetime.now()
    await run(run_settings(sources(), pool), fetcher, probe)
    record = read_runs(str(pool))[0]

    assert started_before <= record.started_at <= datetime.now()
    assert (record.scraped, record.candidates) == (3, 3)
    assert (record.working, record.unreachable, record.rejected) == (1, 1, 1)


async def test_a_run_records_what_each_source_contributed(pool: Path) -> None:
    fetcher = FakeFetcher(
        {
            "first": "1.1.1.1:80\n2.2.2.2:80",
            "second": "2.2.2.2:80\n3.3.3.3:80",
        }
    )

    await run(
        run_settings(sources(), pool),
        fetcher,
        FakeProbe(working={"2.2.2.2:80"}),
    )
    record = read_runs(str(pool))[0]

    # the candidate two sources offered is counted once, against the source that
    # got there first, so what the sources add up to is what the run measured
    assert [(c.source, c.candidates, c.working) for c in record.contributions] == [
        ("first", 1, 0),
        ("second", 2, 1),
    ]
    assert sum(c.candidates for c in record.contributions) == record.candidates


async def test_a_run_whose_chunks_were_all_lost_records_no_outcome(pool: Path) -> None:
    """A chunk that blew up says nothing about the candidates in it, so the run
    records them as taken on and measured nowhere rather than as misses"""

    class ExplodingProbe(FakeProbe):
        async def probe(self, candidate: str) -> ProbeOutcome:
            self.asked.append(candidate)
            raise RuntimeError("connection reset")

    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "2.2.2.2:80"})

    await run(run_settings(sources(), pool), fetcher, ExplodingProbe(working=set()))
    record = read_runs(str(pool))[0]

    assert (record.candidates, record.working, record.unreachable) == (2, 0, 0)


async def test_a_run_that_never_finished_records_nothing(pool: Path) -> None:
    """A run that failed has no finding to keep: half a run is not a run, and
    recording it as one would put a row in the history that never happened"""

    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "no addresses in here"})

    with pytest.raises(EmptySourceError):
        await run(run_settings(sources(), pool), fetcher, FakeProbe(working=set()))

    assert read_runs(str(pool)) == ()


async def test_a_revalidation_records_what_it_found(pool: Path) -> None:
    stored(pool, "1.1.1.1:80", "2.2.2.2:80")

    await revalidate(run_settings(sources(), pool), FakeProbe(working={"1.1.1.1:80"}))
    record = read_runs(str(pool))[0]

    assert (record.scraped, record.candidates) == (0, 2)
    assert (record.working, record.unreachable, record.rejected) == (1, 1, 0)
    assert record.contributions == ()


async def test_a_revalidation_does_not_measure_candidates_it_no_longer_trusts(
    pool: Path,
) -> None:
    """A candidate no recent run probed is not data, so it is dropped before the
    run rather than counted in what the run found"""

    stored(pool, "1.1.1.1:80")

    with closing(sqlite3.connect(pool)) as conn:
        conn.execute("UPDATE proxies SET last_probed_at = ?", (ago(30),))
        conn.commit()

    report = await revalidate(run_settings(sources(), pool), FakeProbe(working=set()))
    record = read_runs(str(pool))[0]

    assert (record.candidates, record.working) == (0, 0)
    assert report.stale == 1
    assert get_proxies(str(pool)) == []


async def test_a_source_whose_candidates_were_all_duplicates_is_still_named(
    pool: Path,
) -> None:
    """A source that answered with nothing of its own has still answered, and the
    report has to be able to tell that apart from a source that stayed silent"""

    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "1.1.1.1:80\n2.2.2.2:80"})

    await run(
        run_settings(sources(), pool),
        fetcher,
        FakeProbe(working={"1.1.1.1:80"}),
    )
    record = read_runs(str(pool))[0]

    assert [(c.source, c.candidates, c.working) for c in record.contributions] == [
        ("first", 1, 1),
        ("second", 1, 0),
    ]


async def test_every_run_adds_its_own_row_to_the_history(pool: Path) -> None:
    settings = run_settings(sources(), pool)
    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "2.2.2.2:80"})

    await run(settings, fetcher, FakeProbe(working={"1.1.1.1:80"}))
    await revalidate(settings, FakeProbe(working={"1.1.1.1:80"}))

    assert [record.candidates for record in read_runs(str(pool))] == [2, 1]
