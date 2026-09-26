import asyncio
import re

import aiohttp


async def request(
    session: aiohttp.ClientSession, url: str, user_agent: str, timeout_s: float
) -> str:
    """Get the text at url, retrying a few times before giving up"""

    headers = {"User-Agent": user_agent}

    # at each attempt we try to get the url html if there is any problem we stop
    # sleep time is exponential
    for attemps in range(3):
        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
        except:  # noqa: E722 - a source that keeps failing is treated as empty
            await asyncio.sleep(2**attemps)
            continue

        # an unsuccessful response: pause, then let the loop try again with the
        # response closed rather than held open for the length of the pause
        await asyncio.sleep(1)

    return ""


async def extract(html: str) -> list[str]:
    """Extract IP:PORT pattern from html or raw text"""

    patterns = [
        r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{2,5}\b",
        r"(\d+\.\d+\.\d+\.\d+):(\d+)",
    ]

    matches = re.findall(patterns[0], html)

    # dedupe
    return list(set(matches))
