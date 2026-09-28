import aiohttp

from src.capabilities import Payload, ProbeOutcome
from src.ingest import request
from src.run import RunSettings
from src.sources import Source
from src.validation import probe


def probing_session(settings: RunSettings) -> aiohttp.ClientSession:
    """A session that keeps as many probes in flight as the settings allow, and
    no more, since a candidate asked too hard at once answers with a connection
    reset rather than with the target"""

    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=settings.connector_limit,
            limit_per_host=settings.connector_limit_per_host,
            ssl=False,
            ttl_dns_cache=settings.dns_cache_seconds,
            force_close=False,
        )
    )


class HttpFetcher:
    """Fetch a source over HTTP, as often and as patiently as the settings say"""

    def __init__(self, session: aiohttp.ClientSession, settings: RunSettings) -> None:
        self._session = session
        self._settings = settings

    async def fetch(self, source: Source) -> Payload:
        settings = self._settings

        text = await request(
            self._session,
            source.url,
            settings.user_agent,
            settings.fetch_timeout,
            settings.fetch_attempts,
            settings.fetch_backoff_seconds,
        )

        return Payload(source=source.name, text=text)


class HttpProber:
    """Probe a candidate over HTTP against the target the settings name"""

    def __init__(self, session: aiohttp.ClientSession, settings: RunSettings) -> None:
        self._session = session
        self._settings = settings

    async def probe(self, candidate: str) -> ProbeOutcome:
        return await probe(
            self._session,
            candidate,
            self._settings.validation_target,
            self._settings.probe_timeout,
        )
