import aiohttp
import asyncio
from src.storage import init_db, save_working_proxies
from src.ingest import scrape_source
from src.sources import SOURCES
from src.validation import validate_batch

async def main():
  init_db()
  all_found = []


  async with aiohttp.ClientSession() as session:
    # scrape all sources concurently
    scrape_tasks = [scrape_source(session, url, name) for name, url in SOURCES.items()]
    results = await asyncio.gather(*scrape_tasks)

    for res in results:
      all_found.extend(res)

  unique_proxies = list(set(all_found))
  print(f"[RAW] Total unique proxies harvested: {len(unique_proxies)}")

  # validate chunks
  chunk_size = 500
  all_working = []
  for i in range(0, len(unique_proxies), chunk_size):
    chunk = unique_proxies[i:i+chunk_size]
    working = await validate_batch(chunk, concurrent=100)
    all_working.extend(working)
    print(f"[VALID] Chunk {i // chunk_size + 1}: {len(working)} working")

  save_working_proxies(all_working)

  print(f"\n[FINISHED] Working proxies: {len(all_working)}")


if __name__ == "__main__":
  asyncio.run(main())


