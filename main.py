import asyncio

import aiohttp
from src.http import HttpFetcher, HttpProber
from src.run import RevalidationReport, RunReport, RunSettings, revalidate, run


def probing_session(settings: RunSettings) -> aiohttp.ClientSession:
    """A session that accepts up to settings.connector_limit jobs at a time"""

    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=settings.connector_limit,
            limit_per_host=settings.connector_limit_per_host,
            ssl=False,
            ttl_dns_cache=300,
            force_close=False,
        )
    )


async def scrape(settings: RunSettings | None = None) -> RunReport:
    """A full scrape run over the real internet"""

    settings = settings or RunSettings()

    async with aiohttp.ClientSession() as fetch_session:
        fetcher = HttpFetcher(
            fetch_session, settings.user_agent, settings.fetch_timeout
        )

        async with probing_session(settings) as probe_session:
            prober = HttpProber(
                probe_session, settings.validation_target, settings.probe_timeout
            )

            return await run(settings, fetcher, prober)


async def revalidate_stored(settings: RunSettings | None = None) -> RevalidationReport:
    """A revalidation run over the proxies already in the database"""

    settings = settings or RunSettings()

    async with probing_session(settings) as session:
        prober = HttpProber(session, settings.validation_target, settings.probe_timeout)

        return await revalidate(settings, prober)


if __name__ == "__main__":
    asyncio.run(scrape())
