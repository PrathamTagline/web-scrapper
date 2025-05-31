import asyncio
import time
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from datetime import datetime
import os

visited_urls = set()
MAX_PAGES = 1000000
CONCURRENT_CONTEXTS = 4
TASKS_PER_CONTEXT = 5

timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
main_output_file = f"scraped_clean_html_{timestamp}.html"

async def intercept_requests(route):
    if route.request.resource_type in ["image", "media", "font"]:
        await route.abort()
    else:
        await route.continue_()

async def scrape_page(context_id, page, url, base_url, url_queue, retries=3):
    if url in visited_urls or len(visited_urls) >= MAX_PAGES:
        await page.close()
        return

    visited_urls.add(url)
    print(f"🔍 [{context_id}] Scraping: {url}")

    for attempt in range(1, retries + 1):
        try:
            start = time.time()
            await page.goto(url, wait_until='networkidle')
            duration = time.time() - start
            print(f"⏱️ [{context_id}] Page loaded in {duration:.2f} seconds: {url}")

            await page.wait_for_selector("body", timeout=5000)

            html_content = await page.content()
            title = await page.title()

            soup = BeautifulSoup(html_content, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            for tag in soup.find_all(attrs={"style": True}):
                del tag["style"]

            temp_file = f"temp_scrape_{context_id}.html"
            with open(temp_file, "a", encoding="utf-8") as f:
                f.write(f"\n<!-- START OF PAGE: {url} -->\n")
                f.write(f"<!-- Page Title: {title} -->\n")
                f.write(soup.prettify())
                f.write(f"\n<!-- END OF PAGE: {url} -->\n\n")

            links = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for link in links:
                parsed = urlparse(link)
                if parsed.scheme.startswith("http") and base_url in parsed.netloc:
                    clean_link = link.split("#")[0].rstrip("/")
                    if clean_link not in visited_urls and url_queue.qsize() + len(visited_urls) < MAX_PAGES:
                        await url_queue.put(clean_link)

            break

        except PlaywrightTimeoutError:
            print(f"⚠️ Timeout visiting {url}, attempt {attempt}/{retries}")
            if attempt == retries:
                print(f"❌ Giving up on {url} after {retries} attempts")
        except Exception as e:
            print(f"⚠️ Error visiting {url}: {e}")
            break

    try:
        await page.close()
    except Exception as e:
        print(f"⚠️ Error closing page: {e}")

async def context_worker(context, base_url, url_queue, context_id, shutdown_event):
    await context.route("**/*", intercept_requests)

    while not shutdown_event.is_set() and len(visited_urls) < MAX_PAGES:
        tasks = []
        if url_queue.empty():
            await asyncio.sleep(1)
            continue

        for _ in range(TASKS_PER_CONTEXT):
            if url_queue.empty():
                break
            url = await url_queue.get()
            page = await context.new_page()
            tasks.append(scrape_page(context_id, page, url, base_url, url_queue))

        if tasks:
            await asyncio.gather(*tasks)
        else:
            await asyncio.sleep(1)

async def merge_temp_files(context_ids):
    with open(main_output_file, "w", encoding="utf-8") as outfile:
        for cid in context_ids:
            temp_file = f"temp_scrape_{cid}.html"
            if os.path.exists(temp_file):
                with open(temp_file, "r", encoding="utf-8") as f:
                    outfile.write(f.read())
                os.remove(temp_file)

async def monitor_progress(url_queue, shutdown_event):
    empty_since = None

    while not shutdown_event.is_set():
        qsize = url_queue.qsize()
        print(f"📊 Queue: {qsize} | Visited: {len(visited_urls)}")

        if qsize == 0:
            if empty_since is None:
                empty_since = time.time()
            elif time.time() - empty_since >= 120:
                print("🛑 Queue has been empty for 2 minutes. Assuming crawl is complete.")
                shutdown_event.set()
                break
        else:
            empty_since = None

        await asyncio.sleep(5)

async def main():
    start_url = "https://taglineinfotech.com/"
    base_url = urlparse(start_url).netloc
    url_queue = asyncio.Queue()
    await url_queue.put(start_url.rstrip("/"))

    start_time = datetime.now()
    print(f"📝 Output File: {main_output_file}")
    print(f"⏱️ Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    shutdown_event = asyncio.Event()

    async with async_playwright() as p:
        # ✅ Launch one shared browser
        browser = await p.chromium.launch(headless=True)
        
        context_ids = list(range(CONCURRENT_CONTEXTS))

        # ✅ Create multiple contexts from the same browser
        contexts = [await browser.new_context() for _ in context_ids]

        workers = [
            asyncio.create_task(context_worker(contexts[i], base_url, url_queue, context_ids[i], shutdown_event))
            for i in range(CONCURRENT_CONTEXTS)
        ]

        monitor = asyncio.create_task(monitor_progress(url_queue, shutdown_event))

        await asyncio.gather(*workers)
        monitor.cancel()
        await browser.close()

    await merge_temp_files(context_ids)

    end_time = datetime.now()
    print(f"🏁 End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🕒 Total Time: {end_time - start_time}")
    print(f"✅ Total Pages Scraped: {len(visited_urls)}")

if __name__ == "__main__":
    asyncio.run(main())
