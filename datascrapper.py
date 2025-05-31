import asyncio
import time
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from datetime import datetime
import os

visited_urls = set()
MAX_PAGES = 1000000
CONCURRENT_CONTEXTS = 4  # Number of parallel browser contexts
TASKS_PER_CONTEXT = 5    # Pages each context handles concurrently

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
            # Dynamic wait: wait until network idle or max 30s
            await page.goto(url, wait_until='networkidle')
            duration = time.time() - start
            print(f"⏱️ [{context_id}] Page loaded in {duration:.2f} seconds: {url}")

            # Extra wait for body selector to ensure page rendered
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

            # Extract and enqueue links
            links = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for link in links:
                parsed = urlparse(link)
                if parsed.scheme.startswith("http") and base_url in parsed.netloc:
                    clean_link = link.split("#")[0].rstrip("/")
                    if clean_link not in visited_urls and url_queue.qsize() + len(visited_urls) < MAX_PAGES:
                        await url_queue.put(clean_link)

            break  # success, exit retry loop

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

async def context_worker(playwright, base_url, url_queue, context_id):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    await context.route("**/*", intercept_requests)

    while len(visited_urls) < MAX_PAGES:
        tasks = []
        # Avoid busy waiting, break if queue empty
        if url_queue.empty():
            await asyncio.sleep(1)
            continue

        for _ in range(TASKS_PER_CONTEXT):
            if url_queue.empty():
                break
            url = await url_queue.get()
            tasks.append(scrape_page(context_id, await context.new_page(), url, base_url, url_queue))

        if tasks:
            await asyncio.gather(*tasks)
        else:
            # If no tasks to run, wait a bit before retrying
            await asyncio.sleep(1)

    await browser.close()

async def merge_temp_files(context_ids):
    with open(main_output_file, "w", encoding="utf-8") as outfile:
        for cid in context_ids:
            temp_file = f"temp_scrape_{cid}.html"
            if os.path.exists(temp_file):
                with open(temp_file, "r", encoding="utf-8") as f:
                    outfile.write(f.read())
                os.remove(temp_file)

async def monitor_progress(url_queue):
    while True:
        print(f"📊 Queue: {url_queue.qsize()} | Visited: {len(visited_urls)}")
        if url_queue.empty() and len(visited_urls) >= MAX_PAGES:
            break
        await asyncio.sleep(5)

async def main():
    start_url = "https://taglineinfotech.com/"
    base_url = urlparse(start_url).netloc
    url_queue = asyncio.Queue()
    await url_queue.put(start_url.rstrip("/"))

    start_time = datetime.now()
    print(f"📝 Output File: {main_output_file}")
    print(f"⏱️ Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    async with async_playwright() as p:
        context_ids = list(range(CONCURRENT_CONTEXTS))
        workers = [
            asyncio.create_task(context_worker(p, base_url, url_queue, cid))
            for cid in context_ids
        ]
        monitor = asyncio.create_task(monitor_progress(url_queue))

        await asyncio.gather(*workers)
        # Once workers done, cancel monitoring
        monitor.cancel()

    await merge_temp_files(context_ids)

    end_time = datetime.now()
    print(f"🏁 End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🕒 Total Time: {end_time - start_time}")
    print(f"✅ Total Pages Scraped: {len(visited_urls)}")

if __name__ == "__main__":
    asyncio.run(main())
