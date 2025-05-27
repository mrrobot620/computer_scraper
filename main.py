import os
import csv
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from rich import print
from rich.panel import Panel
import json
import time
from datetime import datetime
import random
import async_timeout
from urllib.parse import urlparse
from PIL import Image
import io

class ProductScraper:
    def __init__(self) -> None:
        print(f"[bold Green]SUCCESS:[/] Initializing Scraper")
        self.category_tree = {}
        self.semaphore = asyncio.Semaphore(30)
        self.image_download_queue = []
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.csv_file = f"entries_{timestamp}.csv"
        self.MAX_RETRIES = 5


    async def get_categories1(self, session):
        url = "https://www.ictechdistribution.com"
        async with session.get(url) as response:
            if response.status == 200:
                return await response.text()
            print(f"[bold Red]ERROR:[/] Request to url: {url} failed with status code: [red]{response.status}[/]")

    async def parse_categoriese_1_url(self, html_content):
        soup = BeautifulSoup(html_content, 'lxml')
        urls = [item.find("a")["href"] for item in soup.find_all('div', class_="col-12 col-md-3")]
        if urls:
            for url in urls:
                print(f"[bold Green]SUCCESS:[/] Fetched Category Url: {url}")
            return urls
        print(f"[bold yellow]INFO:[/] Not found any url")
        return []

    async def parse_categoriese_2_url(self, html_content):
        soup = BeautifulSoup(html_content, 'lxml')
        urls = []
        for ul in soup.find_all('ul', class_="wo-bodytype"):
            for li in ul.find_all('li'):
                a_tag = li.find("a")
                if a_tag and a_tag.has_attr("href"):
                    urls.append(a_tag["href"])

        if urls:
            for url in urls:
                print(f"[bold Green]SUCCESS:[/] Fetched C2 Category Url: {url}")
            return urls
        print(f"[bold yellow]INFO:[/] No C2 Category URLs found")
        return []

    async def get_categories_level(self, session, url):
        async with session.get(url) as response:
            if response.status == 200:
                return await response.text()
            print(f"[bold Red]ERROR:[/] Request to url: {url} failed with status code: {response.status}")

    async def parse_categoriese_3_url(self, html_content):
        soup = BeautifulSoup(html_content, 'lxml')
        ul_elements = soup.find_all('ul', class_="wo-bodytype")
        if not ul_elements:
            print(f"[bold yellow]INFO:[/] No 'ul.wo-bodytype' elements found. Skipping.")
            return []

        urls = []
        for ul in ul_elements:
            for li in ul.find_all('li'):
                a_tag = li.find("a")
                if a_tag and a_tag.has_attr("href"):
                    urls.append(a_tag["href"])

        if urls:
            for url in urls:
                print(f"[bold Green]SUCCESS:[/] Fetched C3 Category Url: {url}")
            return urls
        print(f"[bold yellow]INFO:[/] No C3 Category URLs found")
        return []

    async def start_scraping(self):
        async with aiohttp.ClientSession() as session:
            root_html = await self.get_categories1(session)
            c1_urls = await self.parse_categoriese_1_url(root_html)

            for c1_url in c1_urls:
                self.category_tree[c1_url] = {}
                c2_html = await self.get_categories_level(session, c1_url)
                c2_urls = await self.parse_categoriese_2_url(c2_html)

                for c2_url in c2_urls:
                    self.category_tree[c1_url][c2_url] = {}
                    c3_html = await self.get_categories_level(session, c2_url)
                    c3_urls = await self.parse_categoriese_3_url(c3_html)

                    for c3_url in c3_urls:
                        self.category_tree[c1_url][c2_url][c3_url] = {}

        with open("category_tree.json", "w") as f:
            json.dump(self.category_tree, f, indent=4)

    async def scroll_product_tree(self):
        with open('category_tree.json', 'r') as f:
            data = json.load(f)

        async with aiohttp.ClientSession() as session:
            await self._scroll_product_tree(session, data)

    async def _scroll_product_tree(self, session, data, path=None):
        if path is None:
            path = []

        for key, value in data.items():
            new_path = path + [key]
            if isinstance(value, dict) and value:
                await self._scroll_product_tree(session, value, new_path)
            else:
                final_url = ">".join(new_path)
                await self.get_products(session, final_url)

    async def get_products(self, session, url: str):
        h_category = await self.category_helper(url)
        final_url = url.split(">")[-1]
        values = final_url.split("/")
        category = values[-2]
        data1 = values[-4]
        data2 = values[-3]
        print(f"Category: {category} | h_category: {h_category} | data1: {data1} | data2: {data2}")
        await self.scrape_products(session, data1, data2, h_category)

    async def category_helper(self, url: str) -> str:
        return " > ".join([s.split("/")[-2] for s in url.split(">")])

    async def scrape_products(self, session, c1: str, c2: str, category) -> None:
        url = "https://www.ictechdistribution.com/traffic-data/load_products_data.html"
        page = 1
        total_urls: list[str] = []

        while True:
            data = {
                'search': 'search',
                'page': page,
                'searchQuery': '',
                'data1': c1,
                'data2': c2
            }
            html = await self.safe_post(session, url, data=data)
            if not html:
                print(f"[bold red]ERROR:[/] Failed to fetch products from {url} — aborting.")
                return

            soup = BeautifulSoup(html, 'html.parser')
            if not soup.find(class_='wo-topvehiclesholder'):
                print(f"[bold yellow]No 'wo-topvehiclesholder' found on page {page}. Stopping.[/]")
                break

            urls = [item.find("a")["href"] for item in soup.find_all('div', class_="wo-vehicles__title")]
            total_urls.extend(set(urls))

            print(f"[Page {page}] - URLs: {len(total_urls)}")
            page += 1

        await self.final_scrap(session, category, total_urls)


    async def fetch_url(self, session, category, url):
        final_url = "https://www.ictechdistribution.com" + url
        retries = 0

        while retries < self.MAX_RETRIES:
            try:
                async with self.semaphore:
                    async with session.get(final_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            data = await response.text()
                            print(f"[bold blue]INFO:[/][{category}] Fetched data from {final_url}")
                            await self.create_entry(data, category, url)
                            return data
                        else:
                            print(f"[bold red]ERROR:[/] Request to url: {final_url} failed with status code: {response.status}")
                            return None              
            except (aiohttp.ServerDisconnectedError, aiohttp.ClientConnectorError,aiohttp.ClientOSError, asyncio.TimeoutError) as e: 
                retries += 1
                wait = 2 ** retries + random.uniform(0, 1)
                print(f"[bold yellow]WARN:[/] Server disconnected for {final_url}, retrying in {wait:.2f}s... (attempt {retries}/{self.MAX_RETRIES})")
                await asyncio.sleep(wait)
            except Exception as e:
                retries += 1
                wait = 2 ** retries + random.uniform(0, 1)
                print(f"[bold red]ERROR:[/] Exception for {final_url}: {e} — Retrying in {wait:.2f}s (attempt {retries}/{self.MAX_RETRIES})")
                await asyncio.sleep(wait)
        print(f"[bold red]FAILED:[/] Max retries reached for {final_url}")
        return None

    async def final_scrap(self, session, category, urls):
        tasks = [self.fetch_url(session, category, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results

    async def create_entry(self, data, category, url):

        soup = BeautifulSoup(data, 'lxml')

        slider_div = soup.find('div', id='wo-vsingleslider')
        imgs = slider_div.find_all('img')
        image_urls = [img['src'] for img in imgs]

        desc_div = soup.select_one("div#detail div.wo-description")
        description = desc_div.get_text(strip=True, separator="\n") if desc_div else "Description not found"

        features_div = soup.find('div', id='features')
        features_text = features_div.get_text(separator="\n", strip=True)

        specification_div = soup.find('div', id='policy')

        specs = specification_div.find_all('li')

        simple_ul = '<ul>\n'

        for spec in specs:
            label = spec.find('strong')
            value = spec.find('span')
            if label and value:
                label_text = label.get_text(strip=True).replace(':', '')
                value_text = value.get_text(strip=True)
                simple_ul += f'  <li>{label_text}: {value_text}</li>\n'

        simple_ul += '</ul>'

        entry_id = url.split("/")[-3]
        own_images = []

        for i, _ in enumerate(image_urls):
            image_id = f"https://dubaicomputershop.com/pro-images/{entry_id}_{i}.png"
            own_images.append(image_id)

        data = {
            "ID": url.split("/")[-3],
            "Type": "simple",
            "SKU": url.split("/")[-3],
            "Name": url.split("/")[-2],
            "Published": 1,
            "Is featured?": 0,
            "Visibility in catalog": "visible",
            "Categories": category,
            "Images": " | ".join(own_images),
            "Meta: _wp_page_template": "default",
            'product-description': description,
            "FeaturesTab": features_text,
            "specification": simple_ul, 
        }

        self.image_download_queue.append({
            "id": url.split("/")[-3],
            "image_urls": image_urls,
        })


        file_exists = os.path.isfile(self.csv_file)

        with open(self.csv_file, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)

    async def download_images(self, max_retries=3, base_delay=1):
        os.makedirs("images", exist_ok=True)
        semaphore = asyncio.Semaphore(10)
        async def download(session, img_url, file_path):
            for attempt in range(1, max_retries + 1):
                try:
                    async with semaphore:
                        async with async_timeout.timeout(5):
                            async with session.get(img_url) as resp:
                                if resp.status == 200:
                                    img_bytes = await resp.read()
                                    try:
                                        image = Image.open(io.BytesIO(img_bytes)).convert("RGBA")
                                        image.save(file_path, format="PNG")
                                        print(f"[green]Saved as PNG:[/] {file_path}")
                                        return
                                    except Exception as e:
                                        print(f"[red]Failed to convert image {img_url}: {e}")
                                        return
                                else:
                                    print(f"[yellow]Attempt {attempt}: Failed to download {img_url} - Status {resp.status}")
                except Exception as e:
                    print(f"[red]Attempt {attempt}: Error downloading {img_url}: {e}")
                if attempt < max_retries:
                    delay = base_delay * 2 ** (attempt - 1)
                    print(f"[cyan]Retrying in {delay} seconds...[/]")
                    await asyncio.sleep(delay)
                else:
                    print(f"[red]Failed after {max_retries} attempts: {img_url}")

        async with aiohttp.ClientSession() as session:
            tasks = []
            for entry in self.image_download_queue:
                entry_id = entry["id"]
                for i, img_url in enumerate(entry["image_urls"]):
                    filename = f"{entry_id}_{i}.png"
                    file_path = os.path.join("images", filename)
                    tasks.append(download(session, img_url, file_path))
            await asyncio.gather(*tasks)
    
    async def safe_post(self, session, url, data):
        retries = 0
        BASE_WAIT = 2
        MAX_WAIT = 60

        while retries < self.MAX_RETRIES:
            try:
                async with self.semaphore:
                    async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            return await response.text()
                        else:
                            print(f"[bold red]ERROR:[/] POST to {url} failed with status {response.status}")
                            return None
            except (aiohttp.ServerDisconnectedError, aiohttp.ClientConnectorError,
                    aiohttp.ClientOSError, asyncio.TimeoutError) as e:
                retries += 1
                wait = min(BASE_WAIT ** retries + random.uniform(0, 1), MAX_WAIT)
                print(f"[bold yellow]WARN:[/] Retryable POST error for {url}: {type(e).__name__} — retrying in {wait:.2f}s (attempt {retries}/{self.MAX_RETRIES})")
                await asyncio.sleep(wait)
            except Exception as e:
                retries += 1
                wait = min(BASE_WAIT ** retries + random.uniform(0, 1), MAX_WAIT)
                print(f"[bold red]ERROR:[/] Unexpected POST exception for {url}: {e} — retrying in {wait:.2f}s (attempt {retries}/{self.MAX_RETRIES})")
                await asyncio.sleep(wait)

        print(f"[bold red]FAILED:[/] Max POST retries reached for {url}")
        return None

    
if __name__ == '__main__':
    print(Panel("[bold green]\u2705 Scraping Process Started \u2705[/]", expand=False))
    scraper = ProductScraper()
    asyncio.run(scraper.start_scraping())
    asyncio.run(scraper.scroll_product_tree())
    asyncio.run(scraper.download_images())



