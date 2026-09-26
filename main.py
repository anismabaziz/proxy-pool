import asyncio

import aiohttp
from src.ingest import scrape_source
from src.sources import SOURCES
from src.storage import (
    get_proxies,
    init_db,
    remove_proxies,
    save_working_proxies,
    update_proxies,
)
from src.validation import validate_batch


async def scrape() -> None:
    init_db()
    all_found = []

    async with aiohttp.ClientSession() as session:
        # scrape all sources concurently
        scrape_tasks = [
            scrape_source(session, url, name) for name, url in SOURCES.items()
        ]
        results = await asyncio.gather(*scrape_tasks)

        for res in results:
            all_found.extend(res)

    unique_proxies = list(set(all_found))
    print(f"[RAW] Total unique proxies harvested: {len(unique_proxies)}")

    # validate chunks
    chunk_size = 300
    all_working = []

    # add a max
    unique_proxies = unique_proxies[:5000]

    for i in range(0, len(unique_proxies), chunk_size):
        chunk = unique_proxies[i : i + chunk_size]

        await asyncio.sleep(2 + (i // chunk_size) * 0.5)

        try:
            working = await validate_batch(chunk, concurrent=25)
            all_working.extend(working)
            print(f"[VALID] Chunk {i // chunk_size + 1}: Working {len(working)}")

        except Exception as e:
            print(f"[ERROR] Chunk failed: {e}")
            await asyncio.sleep(10)

    save_working_proxies(all_working)
    print(f"\n[DB] Saved proxies to db: {len(all_working)}")

    print(f"\n[FINISH] Working proxies: {len(all_working)}")


async def revalidate_proxies() -> None:
    """
    Revalidates already stored proxies
    """

    proxies = get_proxies()
    chunk_size = 300
    all_working = []

    print(f"[START] Proxy list: {len(proxies)} proxies")

    for i in range(0, len(proxies), chunk_size):
        chunk = proxies[i : i + chunk_size]

        await asyncio.sleep(2 + (i // chunk_size) * 0.5)

        try:
            working = await validate_batch(chunk, concurrent=25)
            all_working.extend(working)
            print(f"[REVALIDATE] Chunk {i // chunk_size + 1}: Working {len(working)}")

        except Exception as e:
            print(f"[ERROR] Chunk failed: {e}")
            await asyncio.sleep(10)

    # update the working proxies
    _, count, last_checked = update_proxies(all_working)
    print(f"[DB] Updated working proxies: {count}")

    # remove not working proxies
    _, count = remove_proxies(last_checked)
    print(f"[DB] Removed dead proxies: {count}")

    print(f"[FINISH] Working proxies: {len(all_working)}")


if __name__ == "__main__":
    asyncio.run(scrape())
