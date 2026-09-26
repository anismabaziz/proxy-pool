from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from src.capabilities import Payload, ProbeOutcome
from src.run import RunSettings, revalidate, run
from src.sources import Source
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
    asked: list[str] = field(default_factory=list)

    async def probe(self, candidate: str) -> ProbeOutcome:
        self.asked.append(candidate)
        return ProbeOutcome(
            proxy=candidate,
            latency_ms=self.latency_ms if candidate in self.working else 0,
            is_working=candidate in self.working,
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


def sources() -> tuple[Source, ...]:
    return (
        Source("first", "https://first.example"),
        Source("second", "https://second.example"),
    )


async def test_every_source_is_fetched(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80\n2.2.2.2:80"})

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
    assert report.harvested == 3
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

    fetcher = FakeFetcher({"first": "1.1.1.1:80\n2.2.2.2:80"})
    probe = ExplodingProbe(working=set())

    report = await run(run_settings(sources()), fetcher, probe)

    assert report.failed_chunks == 1
    assert report.working == []


async def test_working_proxies_are_saved(pool: Path) -> None:
    fetcher = FakeFetcher({"first": "1.1.1.1:80\n2.2.2.2:80"})

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
