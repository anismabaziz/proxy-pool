from dataclasses import dataclass
from typing import Protocol

from src.sources import Source


@dataclass(frozen=True)
class Payload:
    """The raw text a source handed back, and which source it came from"""

    source: str
    text: str


@dataclass(frozen=True)
class ProbeOutcome:
    """What a probe found when it put one candidate through a real request"""

    proxy: str
    latency_ms: int
    is_working: bool


class Fetcher(Protocol):
    async def fetch(self, source: Source) -> Payload: ...


class Prober(Protocol):
    async def probe(self, candidate: str) -> ProbeOutcome: ...
