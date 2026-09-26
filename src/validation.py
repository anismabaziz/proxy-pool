import time

import aiohttp

from src.capabilities import ProbeOutcome


async def probe(
    session: aiohttp.ClientSession, candidate: str, test_url: str, timeout_s: float
) -> ProbeOutcome:
    """Put one candidate through a request and time how long it takes"""

    proxy_url = f"http://{candidate}"
    start = time.time()

    # get the test url using the proxy and calc its latency
    try:
        async with session.get(
            test_url, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=timeout_s)
        ) as resp:
            latency = (int)((time.time() - start) * 1000)

            if resp.status == 200:
                return ProbeOutcome(
                    proxy=candidate, latency_ms=latency, is_working=True
                )

    except:  # noqa: E722 - a candidate that errors is simply not working
        pass

    return ProbeOutcome(proxy=candidate, latency_ms=0, is_working=False)
