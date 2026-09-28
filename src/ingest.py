import asyncio

import aiohttp


async def request(
    session: aiohttp.ClientSession,
    url: str,
    user_agent: str,
    timeout_s: float,
    attempts: int,
    backoff_s: float,
) -> str:
    """Get the text at url, retrying as many times as we are given before giving
    up on it"""

    headers = {"User-Agent": user_agent}

    for attempt in range(attempts):
        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
        except Exception:
            # a source that dropped the connection is worth another attempt, and
            # what it dropped us is of no use now that we are about to try again
            pass

        if attempt < attempts - 1:
            # an unsuccessful response or a dropped one: pause before trying
            # again, longer each time, with the response already closed
            await asyncio.sleep(backoff_s * 2**attempt)

    return ""
