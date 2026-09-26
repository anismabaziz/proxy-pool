import aiohttp
import asyncio
import time
from typing import Tuple, List

async def test_proxy(session: aiohttp.ClientSession, proxy: str) -> Tuple[str, int, bool]:
  """ Test proxy returns (proxy, latency_ms, is_working) """

  proxy_url = f"http://{proxy}"
  test_url = "http://httpbin.org/ip"
  start = time.time()


  # get the test url using the proxy and calc its latency
  try:
    async with session.get(test_url, proxy=proxy_url, timeout=5) as resp:
      latency = (int) ((time.time() - start) * 1000)
      
      if resp.status == 200:
        return (proxy, latency, True)
      

  except:
    pass

  return (proxy, 0, False)


async def validate_batch(proxies: List[str], concurrent: int = 50) -> List[Tuple[str, int]]:
  """ Validate a batch of proxies with concurrency control """

  # create a connector that accepts up ot x concurrent jobs
  connector = aiohttp.TCPConnector(limit=concurrent, limit_per_host=10, ssl=False, ttl_dns_cache=300, force_close=False)

  # use the connector to spin up tasks of validation and run them concurrently
  async with aiohttp.ClientSession(connector=connector) as session:

    tasks = [test_proxy(session, p) for p in proxies]
    results = await asyncio.gather(*tasks)

    working = [(p, l) for p, l, ok in results if ok]

    return working