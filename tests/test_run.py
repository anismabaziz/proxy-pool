import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from src.capabilities import Outcome, Payload, ProbeOutcome
from src.run import RunSettings, revalidate, run
from src.sources import EmptySourceError, PayloadFormat, Source
from src.storage import get_proxies, init_db, save_working_proxies


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


def run_settings(sources: tuple[Source, ...]) -> RunSettings:
    """The same settings a real run uses, minus the wait between chunks"""

    return RunSettings(
        sources=sources,
        db_path="proxies.db",
        max_candidates=1000,
        chunk_size=2,
        chunk_delay_base=0,
        chunk_delay_step=0,
        failure_delay=0,
    )


@pytest.fixture
def pool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    monkeypatch.chdir(tmp_path)
    init_db("proxies.db")
    yield tmp_path / "proxies.db"


def text_source(name: str) -> Source:
    return Source(name, f"https://{name}.example", PayloadFormat.ADDRESS_LIST)


def sources() -> tuple[Source, ...]:
    return text_source("first"), text_source("second")


async def test_every_source_is_fetched(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80\n2.2.2.2:80", "second": "3.3.3.3:80"})

    await run(run_settings(sources()), fetcher, FakeProbe(working=set()))

    assert sorted(fetcher.asked) == ["first", "second"]


async def test_only_candidates_reaching_the_probe_are_probed(pool: Path) -> None:
    fetcher = FakeFetcher(
        {
            "first": "1.1.1.1:80\n2.2.2.2:80",
            "second": "2.2.2.2:80\n3.3.3.3:80",
        }
    )
    probe = FakeProbe(working={"1.1.1.1:80"})

    report = await run(run_settings(sources()), fetcher, probe)

    assert sorted(probe.asked) == ["1.1.1.1:80", "2.2.2.2:80", "3.3.3.3:80"]
    assert report.scraped == 3
    assert report.candidates == 3
    assert report.probed == 3


async def test_duplicates_across_sources_are_probed_once(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "1.1.1.1:80"})
    probe = FakeProbe(working=set())

    await run(run_settings(sources()), fetcher, probe)

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

    report = await run(run_settings(sources()), fetcher, probe)

    assert report.failed_chunks == 1
    assert report.working == []


async def test_working_proxies_are_saved(pool: Path) -> None:
    fetcher = FakeFetcher(
        {
            "first": "1.1.1.1:80\n2.2.2.2:80",
            "second": "1.1.1.1:80\n2.2.2.2:80",
        }
    )

    report = await run(
        run_settings(sources()), fetcher, FakeProbe(working={"1.1.1.1:80"})
    )

    assert [(w.proxy, w.latency_ms) for w in report.working] == [("1.1.1.1:80", 100)]
    assert report.saved == 1
    assert get_proxies() == ["1.1.1.1:80"]


async def test_revalidation_drops_the_proxies_that_stopped_working(pool: Path) -> None:
    save_working_proxies([("1.1.1.1:80", 120), ("2.2.2.2:80", 340)])

    report = await revalidate(
        run_settings(sources()), FakeProbe(working={"1.1.1.1:80"})
    )

    assert report.candidates == 2
    assert report.updated == 1
    assert report.removed == 1
    assert get_proxies() == ["1.1.1.1:80"]


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

    report = await run(run_settings((source,)), fetcher, probe)

    assert sorted(probe.asked) == ["1.1.1.1:8080", "2.2.2.2:3128"]
    assert report.scraped == 2


async def test_a_source_that_offers_nothing_is_an_error(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "no addresses in here"})

    with pytest.raises(EmptySourceError, match="second"):
        await run(run_settings(sources()), fetcher, FakeProbe(working=set()))


async def test_a_json_source_without_entries_is_an_error(pool: Path) -> None:
    source = Source("geonode", "https://geonode.example", PayloadFormat.GEONODE_JSON)
    fetcher = FakeFetcher({"geonode": json.dumps({"data": []})})

    with pytest.raises(EmptySourceError, match="geonode"):
        await run(run_settings((source,)), fetcher, FakeProbe(working=set()))


async def test_a_json_source_answering_with_an_error_page_is_an_error(
    pool: Path,
) -> None:
    source = Source("geonode", "https://geonode.example", PayloadFormat.GEONODE_JSON)
    fetcher = FakeFetcher({"geonode": "<html><body>502 Bad Gateway</body></html>"})

    with pytest.raises(EmptySourceError, match="geonode"):
        await run(run_settings((source,)), fetcher, FakeProbe(working=set()))


async def test_a_run_reports_what_each_source_contributed(pool: Path) -> None:
    fetcher = FakeFetcher(
        {
            "first": "1.1.1.1:80\n2.2.2.2:80",
            "second": "2.2.2.2:80\n3.3.3.3:80",
        }
    )

    report = await run(run_settings(sources()), fetcher, FakeProbe(working=set()))

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
        db_path="proxies.db",
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

    report = await run(run_settings(sources()), fetcher, probe)

    assert [outcome.proxy for outcome in report.working] == ["2.2.2.2:80"]
    assert get_proxies() == ["2.2.2.2:80"]


async def test_an_unreachable_candidate_stays_out_of_the_pool(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80", "second": "2.2.2.2:80"})
    probe = FakeProbe(
        working={"2.2.2.2:80"}, otherwise={"1.1.1.1:80": Outcome.UNREACHABLE}
    )

    report = await run(run_settings(sources()), fetcher, probe)

    assert [outcome.proxy for outcome in report.working] == ["2.2.2.2:80"]
    assert get_proxies() == ["2.2.2.2:80"]


def test_the_default_validation_target_is_https() -> None:
    assert RunSettings().validation_target.startswith("https://")
