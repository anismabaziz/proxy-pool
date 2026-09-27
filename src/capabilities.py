from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from src.sources import Source


@dataclass(frozen=True)
class Payload:
    """The raw text a source handed back, and which source it came from"""

    source: str
    text: str


class Outcome(Enum):
    """The three states a probe resolves to, and no others"""

    WORKING = "working"
    UNREACHABLE = "unreachable"
    REJECTED = "rejected"


@dataclass(frozen=True)
class ProbeOutcome:
    """What a probe found when it put one candidate through a real request"""

    proxy: str
    state: Outcome
    latency_ms: int | None = None
    reason: str = ""

    @property
    def is_working(self) -> bool:
        return self.state is Outcome.WORKING


class Fetcher(Protocol):
    async def fetch(self, source: Source) -> Payload: ...


class Prober(Protocol):
    async def probe(self, candidate: str) -> ProbeOutcome: ...
