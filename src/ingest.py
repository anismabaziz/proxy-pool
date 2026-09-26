import asyncio
import re

import aiohttp


async def fetch(session: aiohttp.ClientSession, url : str) -> str:
  """ Fetch html from url with retry logic """

  headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

  # at each attempt we try to get the url html if there is any problem we stop 
  # sleep time is exponential
  for attemps in range(3):
    try:
      async with session.get(
        url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
      ) as resp:
        if resp.status == 200:
          return await resp.text()
    except:  # noqa: E722 - narrowed once fetch failures are handled one by one
      await asyncio.sleep(2 ** attemps)
      continue

    # an unsuccessful response: pause, then let the loop try again with the
    # response closed rather than held open for the length of the pause
    await asyncio.sleep(1)

  return ""



async def extract(html: str, source_name: str) -> list[str]:
  """ Extract IP:PORT pattern from html or raw text """

  patterns = [
    r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{2,5}\b',
    r'(\d+\.\d+\.\d+\.\d+):(\d+)'
  ]

  matches = re.findall(patterns[0], html)

  # dedupe
  return list(set(matches))


async def scrape_source(session: aiohttp.ClientSession, url: str, name: str) -> list[str]:
  """ Main scraping function """
  html = await fetch(session, url)
  if not html:
    return []
  
  proxies = await extract(html, url)
  print(f"[✓] {name}: found {len(proxies)} proxies")

  return proxies