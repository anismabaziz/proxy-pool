import aiohttp

from src.capabilities import Payload, ProbeOutcome
from src.ingest import request
from src.sources import Source
from src.validation import probe


class HttpFetcher:
    """Fetch a source over HTTP"""

    def __init__(
        self, session: aiohttp.ClientSession, user_agent: str, timeout_s: float
    ) -> None:
        self._session = session
        self._user_agent = user_agent
        self._timeout_s = timeout_s

    async def fetch(self, source: Source) -> Payload:
        text = await request(
            self._session, source.url, self._user_agent, self._timeout_s
        )

        return Payload(source=source.name, text=text)


class HttpProber:
    """Probe a candidate over HTTP"""

    def __init__(
        self, session: aiohttp.ClientSession, target_url: str, timeout_s: float
    ) -> None:
        self._session = session
        self._target_url = target_url
        self._timeout_s = timeout_s

    async def probe(self, candidate: str) -> ProbeOutcome:
        return await probe(self._session, candidate, self._target_url, self._timeout_s)
