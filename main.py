import asyncio
import json
import os
import tempfile
import sys
import subprocess
import re
from typing import Dict, List
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from SavingOnDrive import SavingOnDrive
import logging
from datetime import datetime

nest_asyncio.apply()

# Set up logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.DEBUG,  # Increased verbosity to match restaurants
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class TalabatGroceries:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.talabat.com"
        logging.info(f"Initialized TalabatGroceries with URL: {self.url}")

    async def get_general_link(self, page):
        logging.debug("Attempting to get general link")
        retries = 3
        while retries > 0:
            try:
                link_element = await page.wait_for_selector('//a[@data-testid="view-all-link"]', timeout=60000)
                if link_element:
                    full_link = self.base_url + await link_element.get_attribute('href')
                    logging.info(f"General link found: {full_link}")
                    return full_link
                logging.warning("General link not found")
                return None
            except PlaywrightTimeoutError:
                logging.warning("Timeout waiting for general link")
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return None

    async def get_delivery_fees(self, page):
        logging.debug("Attempting to get delivery fees")
        retries = 3
        while retries > 0:
            try:
                delivery_fees_element = await page.query_selector('//div[contains(@class, "delivery-fee")]//span')
                delivery_fees = await delivery_fees_element.inner_text() if delivery_fees_element else "N/A"
                logging.info(f"Delivery fees: {delivery_fees}")
                return delivery_fees
            except PlaywrightTimeoutError:
                logging.warning("Timeout waiting for delivery fees")
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return "N/A"

    async def get_minimum_order(self, page):
        logging.debug("Attempting to get minimum order")
        retries = 3
        while retries > 0:
            try:
                minimum_order_element = await page.query_selector('//div[contains(@class, "minimum-order")]//span')
                minimum_order = await minimum_order_element.inner_text() if minimum_order_element else "N/A"
                logging.info(f"Minimum order: {minimum_order}")
                return minimum_order
            except PlaywrightTimeoutError:
                logging.warning("Timeout waiting for minimum order")
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return "N/A"

    async def extract_category_names(self, page):
        logging.debug("Attempting to extract category names")
        retries = 3
        while retries > 0:
            try:
                await page.wait_for_selector('//span[@data-testid="category-name"]', timeout=60000)
                category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
                category_names = [await element.inner_text() for element in category_name_elements]
                logging.info(f"Category names extracted: {category_names}")
                return category_names
            except PlaywrightTimeoutError:
                logging.warning("Timeout waiting for category names")
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_category_links(self, page):
        logging.debug("Attempting to extract category links")
        retries = 3
        while retries > 0:
            try:
                await page.wait_for_selector('//a[@data-testid="category-item-container"]', timeout=60000)
                category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
                category_links = [self.base_url + await element.get_attribute('href') for element in category_link_elements]
                logging.info(f"Category links extracted: {category_links}")
                return category_links
            except PlaywrightTimeoutError:
                logging.warning("Timeout waiting for category links")
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_all_items_from_sub_category(self, sub_category_link):
        logging.debug(f"Attempting to extract all items from sub-category: {sub_category_link}")
        default_values = []
        for browser_type in ["chromium", "firefox"]:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    sub_page = await browser.new_page()
                    await sub_page.goto(sub_category_link, timeout=120000)
                    await sub_page.wait_for_load_state("networkidle", timeout=120000)
                    try:
                        await sub_page.wait_for_selector('//div[contains(@class, "category-items-container")]', timeout=120000)
                    except PlaywrightTimeoutError:
                        logging.warning(f"Timeout waiting for 'category-items-container' on {sub_category_link}")
                        await sub_page.wait_for_selector('//div[contains(@class, "items")] | //div[contains(@class, "products")]', timeout=60000)

                    pagination_element = await sub_page.query_selector('//ul[@class="paginate-wrap"]')
                    total_pages = 1
                    if pagination_element:
                        page_numbers = await pagination_element.query_selector_all('//li[contains(@class, "paginate-li")]//a')
                        total_pages = len(page_numbers) if page_numbers else 1
                    logging.info(f"Found {total_pages} pages in this sub-category")

                    items = []
                    for page_number in range(1, total_pages + 1):
                        logging.debug(f"Processing page {page_number} of {total_pages}")
                        page_url = f"{sub_category_link}&page={page_number}" if page_number > 1 else sub_category_link
                        await sub_page.goto(page_url, timeout=120000)
                        await sub_page.wait_for_load_state("networkidle", timeout=120000)
                        item_elements = await sub_page.query_selector_all('//a[@data-testid="grocery-item-link-nofollow" and contains(@href, "/product/")]')
                        logging.info(f"Found {len(item_elements)} items on page {page_number}")
                        for i, element in enumerate(item_elements):
                            try:
                                item_name_element = await element.query_selector('div[data-test="item-name"], span[class*="name"], h3, p[class*="title"], div[class*="name"]')
                                item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {i+1}"
                                item_link = self.base_url + await element.get_attribute('href')
                                item_details = await self.extract_item_details(item_link)
                                items.append({"item_name": item_name, "item_link": item_link, **item_details})
                            except Exception as e:
                                logging.error(f"Error processing item {i+1}: {e}", exc_info=True)
                    await browser.close()
                    if items:
                        return items
            except Exception as e:
                logging.error(f"Error extracting items from sub-category {sub_category_link} using {browser_type}: {e}", exc_info=True)
                continue
        return default_values

    async def extract_item_details(self, item_link):
        logging.debug(f"Attempting to extract item details for {item_link}")
        default_values = {"item_price": "N/A", "item_description": "N/A", "item_delivery_time_range": "N/A", "item_images": []}
        for browser_type in ["chromium", "firefox"]:
            result = await self.extract_item_details_new_tab(item_link, browser_type)
            if result != default_values:
                return result
        return default_values

    async def extract_item_details_new_tab(self, item_link, browser_type):
        logging.debug(f"Extracting item details for {item_link} using {browser_type}")
        retries = 3
        while retries > 0:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    page = await browser.new_page()
                    await page.goto(item_link, timeout=90000)
                    await page.wait_for_load_state("networkidle", timeout=90000)
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(5000)

                    item_price_element = await page.query_selector(
                        '//div[contains(@class, "price")]//span[contains(text(), "KD")] | '
                        '//span[contains(@class, "price") and contains(text(), "KD")] | '
                        '//div[@data-testid="price"]//span | '
                        '//span[contains(@class, "amount")] | '
                        '//div[contains(@class, "price")]'
                    )
                    item_price = await item_price_element.inner_text() if item_price_element else "N/A"
                    logging.info(f"Item price: {item_price}")

                    item_description_element = await page.query_selector(
                        '//div[contains(@class, "description")]//p[not(.="Description")] | '
                        '//p[@data-testid="item-description"] | '
                        '//div[@data-testid="description"]//p[not(.="Description")] | '
                        '//div[contains(@class, "product-details")]//p | '
                        '//div[contains(@class, "about")]//p | '
                        '//p[contains(@class, "description") and not(.="Description")]'
                    )
                    item_description = await item_description_element.inner_text() if item_description_element else "N/A"
                    logging.info(f"Item description: {item_description}")

                    delivery_time_element = await page.query_selector(
                        '//div[@data-testid="delivery-tag"]//span | '
                        '//span[contains(@class, "delivery")] | '
                        '//div[contains(@class, "delivery-time")]//span'
                    )
                    delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"
                    logging.info(f"Delivery time range: {delivery_time}")

                    item_image_elements = await page.query_selector_all(
                        '//img[contains(@class, "item-image")] | '
                        '//div[@data-testid="item-image"]//img | '
                        '//img[@alt="product image"]'
                    )
                    item_images = [await img.get_attribute('src') for img in item_image_elements if await img.get_attribute('src')]
                    logging.info(f"Item images: {item_images}")

                    await browser.close()
                    return {
                        "item_price": item_price,
                        "item_description": item_description,
                        "item_delivery_time_range": delivery_time,
                        "item_images": item_images
                    }
            except Exception as e:
                logging.error(f"Error extracting item details for {item_link} using {browser_type}: {e}", exc_info=True)
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return {"item_price": "N/A", "item_description": "N/A", "item_delivery_time_range": "N/A", "item_images": []}

    async def extract_sub_categories(self, page, category_link):
        logging.debug(f"Attempting to extract sub-categories for category: {category_link}")
        retries = 3
        while retries > 0:
            try:
                await page.wait_for_selector('//div[@data-test="sub-category-container"]', timeout=60000)
                sub_category_elements = await page.query_selector_all('//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
                sub_categories = []
                for element in sub_category_elements:
                    try:
                        sub_category_name = await element.inner_text()
                        sub_category_link = self.base_url + await element.get_attribute('href')
                        logging.info(f"Processing sub-category: {sub_category_name}")
                        items = await self.extract_all_items_from_sub_category(sub_category_link)
                        sub_categories.append({
                            "sub_category_name": sub_category_name,
                            "sub_category_link": sub_category_link,
                            "Items": items
                        })
                    except Exception as e:
                        logging.error(f"Error processing sub-category: {e}", exc_info=True)
                logging.info(f"Extracted {len(sub_categories)} sub-categories")
                return sub_categories
            except PlaywrightTimeoutError:
                logging.warning("Timeout waiting for sub-categories")
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_categories(self, page):
        logging.info(f"Processing grocery: {self.url}")
        retries = 3
        while retries > 0:
            try:
                await page.goto(self.url, timeout=120000)
                await page.wait_for_load_state("networkidle", timeout=120000)
                logging.info("Page loaded successfully")
                delivery_fees = await self.get_delivery_fees(page)
                minimum_order = await self.get_minimum_order(page)
                view_all_link = await self.get_general_link(page)
                logging.info(f"Delivery fees: {delivery_fees}")
                logging.info(f"Minimum order: {minimum_order}")
                categories_data = []
                if view_all_link:
                    logging.info(f"Navigating to view all link: {view_all_link}")
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True)
                        category_page = await browser.new_page()
                        await category_page.goto(view_all_link, timeout=120000)
                        await category_page.wait_for_load_state("networkidle", timeout=120000)
                        category_names = await self.extract_category_names(category_page)
                        category_links = await self.extract_category_links(category_page)
                        logging.info(f"Found {len(category_names)} categories")
                        for index, (name, link) in enumerate(zip(category_names, category_links)):
                            logging.info(f"Processing category {index+1}/{len(category_names)}: {name}")
                            async with async_playwright() as p:
                                browser = await p.chromium.launch(headless=True)
                                sub_category_page = await browser.new_page()
                                await sub_category_page.goto(link, timeout=120000)
                                await sub_category_page.wait_for_load_state("networkidle", timeout=120000)
                                sub_categories = await self.extract_sub_categories(sub_category_page, link)
                                await browser.close()
                            logging.info(f"Found {len(sub_categories)} sub-categories in {name}")
                            categories_data.append({"name": name, "link": link, "sub_categories": sub_categories})
                        await browser.close()
                return {
                    "delivery_fees": delivery_fees,
                    "minimum_order": minimum_order,
                    "categories": categories_data
                }
            except Exception as e:
                logging.error(f"Error extracting categories: {e}", exc_info=True)
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        logging.error("Failed to extract categories after multiple attempts")
        return {"error": "Failed to extract categories after multiple attempts"}

class MainScraper:
    CURRENT_PROGRESS_FILE = "current_progress.json"
    SCRAPED_PROGRESS_FILE = "scraped_progress.json"

    def __init__(self):
        self.output_dir = "output"
        self.drive_uploader = SavingOnDrive('credentials.json')
        
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.current_progress = self.load_current_progress()
        self.scraped_progress = self.load_scraped_progress()
        
        self.github_token = os.environ.get('GITHUB_TOKEN')
        self.ensure_playwright_browsers()

    def ensure_playwright_browsers(self):
        try:
            print("Installing Playwright browsers...")
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "firefox"], 
                          check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print("Playwright browsers installed successfully")
        except subprocess.CalledProcessError as e:
            print(f"Error installing Playwright browsers: {e}")

    def load_current_progress(self) -> Dict:
        if os.path.exists(self.CURRENT_PROGRESS_FILE):
            try:
                with open(self.CURRENT_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                print(f"Loaded current progress from {self.CURRENT_PROGRESS_FILE}")
                return progress
            except Exception as e:
                print(f"Error loading current progress: {e}")
        
        default_progress = {
            "completed_areas": [],
            "current_area_index": 0,
            "last_updated": None,
            "current_progress": {
                "area_name": None,
                "current_grocery": 0,  # 0 means no grocery processed yet
                "total_groceries": 0,
                "processed_groceries": [],
                "current_category": None,
                "current_sub_category": None,
                "completed_categories": {}
            }
        }
        self.save_current_progress(default_progress)
        return default_progress

    def save_current_progress(self, progress: Dict = None):
        if progress is None:
            progress = self.current_progress
        try:
            progress["last_updated"] = datetime.now().isoformat()
            with tempfile.NamedTemporaryFile('w', delete=False, dir='.') as temp_file:
                json.dump(progress, temp_file, indent=2, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_filename = temp_file.name
            os.replace(temp_filename, self.CURRENT_PROGRESS_FILE)
            print(f"Saved current progress to {self.CURRENT_PROGRESS_FILE}")
        except Exception as e:
            print(f"Error saving current progress: {e}")

    def load_scraped_progress(self) -> Dict:
        if os.path.exists(self.SCRAPED_PROGRESS_FILE):
            try:
                with open(self.SCRAPED_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                print(f"Loaded scraped progress from {self.SCRAPED_PROGRESS_FILE}")
                return progress
            except Exception as e:
                print(f"Error loading scraped progress: {e}")
        
        default_progress = {
            "completed_areas": [],
            "current_area_index": 0,
            "last_updated": None,
            "all_results": {},
            "current_progress": {
                "area_name": None,
                "current_grocery": 0,
                "total_groceries": 0,
                "processed_groceries": [],
                "current_category": None,
                "current_sub_category": None,
                "completed_categories": {}
            }
        }
        self.save_scraped_progress(default_progress)
        return default_progress

    def save_scraped_progress(self, progress: Dict = None):
        if progress is None:
            progress = self.scraped_progress
        try:
            progress["last_updated"] = datetime.now().isoformat()
            with tempfile.NamedTemporaryFile('w', delete=False, dir='.') as temp_file:
                json.dump(progress, temp_file, indent=2, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_filename = temp_file.name
            os.replace(temp_filename, self.SCRAPED_PROGRESS_FILE)
            print(f"Saved scraped progress to {self.SCRAPED_PROGRESS_FILE}")
        except Exception as e:
            print(f"Error saving scraped progress: {e}")

    def commit_progress(self, message: str = "Periodic progress update"):
        if not self.github_token:
            print("No GITHUB_TOKEN available, skipping commit")
            return
        
        try:
            subprocess.run(["git", "config", "--global", "user.name", "GitHub Action"], check=True)
            subprocess.run(["git", "config", "--global", "user.email", "action@github.com"], check=True)
            subprocess.run(["git", "add", self.CURRENT_PROGRESS_FILE, self.SCRAPED_PROGRESS_FILE, self.output_dir], check=True)
            result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
            if result.returncode == 0 or "nothing to commit" in result.stdout:
                subprocess.run(["git", "push"], check=True, env={"GIT_AUTH_TOKEN": self.github_token})
                print(f"Committed progress: {message}")
            else:
                print("No changes to commit")
        except subprocess.CalledProcessError as e:
            print(f"Error committing progress: {e}")

    def print_progress_details(self):
        try:
            with open(self.CURRENT_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                current = json.load(f)
            print("\nCurrent Progress:")
            print(json.dumps(current, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"Error printing progress: {str(e)}")

    async def scrape_and_save_area(self, area_name: str, area_url: str) -> List[Dict]:
        print(f"\n{'='*50}")
        print(f"SCRAPING AREA: {area_name}")
        print(f"URL: {area_url}")
        print(f"{'='*50}\n")
        
        all_area_results = self.scraped_progress["all_results"].get(area_name, {})
        current_progress = self.current_progress["current_progress"]
        scraped_current_progress = self.scraped_progress["current_progress"]
        
        is_resuming = current_progress["area_name"] == area_name
        start_grocery = current_progress["current_grocery"] if is_resuming else 0
        
        if is_resuming:
            print(f"Resuming area {area_name} from grocery {start_grocery + 1 if start_grocery > 0 else 1}")
        else:
            current_progress.update({
                "area_name": area_name,
                "current_grocery": 0,
                "total_groceries": 0,
                "processed_groceries": [],
                "current_category": None,
                "current_sub_category": None,
                "completed_categories": {}
            })
            scraped_current_progress.update(current_progress)
            self.save_current_progress()
            self.save_scraped_progress()
            self.commit_progress(f"Started scraping area {area_name}")
        
        max_retries = 3
        groceries_on_page = []
        for attempt in range(max_retries):
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()
                    page.set_default_timeout(120000)
                    await page.goto(area_url, timeout=120000)
                    await page.wait_for_load_state("networkidle", timeout=120000)
                    groceries_on_page = await self.get_page_groceries(page)
                    await browser.close()
                if not groceries_on_page:
                    raise Exception("No groceries found")
                print(f"Found {len(groceries_on_page)} groceries in {area_name}")
                break
            except Exception as e:
                print(f"Error loading groceries for {area_name}: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying ({attempt + 1}/{max_retries})...")
                    await asyncio.sleep(5)
                else:
                    print(f"Skipping {area_name} after {max_retries} attempts")
                    groceries_on_page = []
        
        current_progress["total_groceries"] = len(groceries_on_page)
        scraped_current_progress["total_groceries"] = len(groceries_on_page)
        
        for grocery_idx, grocery in enumerate(groceries_on_page):
            grocery_num = grocery_idx + 1  # Start counting from 1
            if grocery_num <= current_progress["current_grocery"]:
                print(f"Skipping processed grocery {grocery_num}/{len(groceries_on_page)}: {grocery['grocery_title']}")
                continue
            
            grocery_title = grocery["grocery_title"]
            if grocery_title in current_progress["processed_groceries"]:
                print(f"Skipping grocery {grocery_num}/{len(groceries_on_page)}: {grocery_title} - Already processed previously")
                current_progress["current_grocery"] = grocery_num
                scraped_current_progress["current_grocery"] = grocery_num
                self.save_current_progress()
                self.save_scraped_progress()
                self.print_progress_details()
                self.commit_progress(f"Skipped grocery {grocery_title} in {area_name}")
                continue
            
            current_progress["current_grocery"] = grocery_num
            scraped_current_progress["current_grocery"] = grocery_num
            print(f"\nProcessing grocery {grocery_num}/{len(groceries_on_page)} in {area_name}: {grocery_title}")
            
            try:
                talabat_grocery = TalabatGroceries(grocery["grocery_link"])
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()
                    grocery_details = await talabat_grocery.extract_categories(page)
                    all_area_results[grocery_title] = {
                        "grocery_link": grocery["grocery_link"],
                        "delivery_time": grocery["delivery_time"],
                        "grocery_details": grocery_details
                    }
                    await self.process_grocery_categories(grocery_title, grocery_details, talabat_grocery, page)
                    await browser.close()
                
                if grocery_title not in current_progress["processed_groceries"]:
                    current_progress["processed_groceries"].append(grocery_title)
                    scraped_current_progress["processed_groceries"].append(grocery_title)
                self.scraped_progress["all_results"][area_name] = all_area_results
                self.save_current_progress()
                self.save_scraped_progress()
                self.print_progress_details()
                self.commit_progress(f"Processed grocery {grocery_title} in {area_name}")
                await asyncio.sleep(2)
            
            except Exception as e:
                print(f"Error processing grocery {grocery_num}/{len(groceries_on_page)}: {grocery_title}: {e}")
                import traceback
                traceback.print_exc()
                if grocery_title not in current_progress["processed_groceries"]:
                    current_progress["processed_groceries"].append(grocery_title)
                    scraped_current_progress["processed_groceries"].append(grocery_title)
                self.save_current_progress()
                self.save_scraped_progress()
                self.print_progress_details()
                self.commit_progress(f"Error processing grocery {grocery_title} in {area_name}")
        
        json_filename = os.path.join(self.output_dir, f"{area_name}.json")
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(all_area_results, f, indent=2, ensure_ascii=False)
        
        workbook = Workbook()
        self.create_excel_sheet(workbook, area_name, all_area_results)
        excel_filename = os.path.join(self.output_dir, f"{area_name}.xlsx")
        workbook.save(excel_filename)
        print(f"Excel file saved: {excel_filename}")
        
        if self.upload_to_drive(excel_filename):
            print(f"Uploaded {excel_filename} to Google Drive")
        else:
            print(f"Failed to upload {excel_filename} to Google Drive")
        
        current_progress.update({
            "area_name": None,
            "current_grocery": 0,
            "total_groceries": 0,
            "processed_groceries": [],
            "current_category": None,
            "current_sub_category": None,
            "completed_categories": {}
        })
        scraped_current_progress.update(current_progress)
        self.save_current_progress()
        self.save_scraped_progress()
        self.print_progress_details()
        self.commit_progress(f"Completed area {area_name}")
        
        print(f"Saved data for {len(all_area_results)} groceries in {area_name}")
        return list(all_area_results.values())

    async def process_grocery_categories(self, grocery_title, grocery_details, talabat_grocery, page):
        completed_categories = self.current_progress["current_progress"]["completed_categories"].get(grocery_title, {})
        for category in grocery_details.get("categories", []):
            category_name = category["name"]
            if category_name in completed_categories and all(sub["sub_category_name"] in completed_categories[category_name] for sub in category["sub_categories"]):
                logging.debug(f"Skipping completed category: {category_name}")
                continue
            await self.process_category(grocery_title, category, talabat_grocery, page)

    async def process_category(self, grocery_title, category_info, talabat_grocery, page):
        category_name = category_info["name"]
        category_link = category_info["link"]
        logging.info(f"Processing category: {category_name} in grocery: {grocery_title}")
        
        self.current_progress["current_progress"]["current_category"] = category_name
        self.scraped_progress["current_progress"]["current_category"] = category_name
        self.save_current_progress()
        self.save_scraped_progress()

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                sub_category_page = await browser.new_page()
                await sub_category_page.goto(category_link, timeout=120000)
                await sub_category_page.wait_for_load_state("networkidle", timeout=120000)
                sub_categories = await talabat_grocery.extract_sub_categories(sub_category_page, category_link)
                await browser.close()
                logging.info(f"Found {len(sub_categories)} sub-categories for {category_name}")
        except Exception as e:
            logging.error(f"Error extracting sub-categories for {category_name}: {e}", exc_info=True)
            sub_categories = []

        completed_sub_categories = self.current_progress["current_progress"]["completed_categories"].get(grocery_title, {}).get(category_name, [])
        for sub_category in sub_categories:
            if sub_category["sub_category_name"] not in completed_sub_categories:
                await self.process_sub_category(grocery_title, category_name, sub_category, talabat_grocery, page)

        if grocery_title not in self.current_progress["current_progress"]["completed_categories"]:
            self.current_progress["current_progress"]["completed_categories"][grocery_title] = {}
            self.scraped_progress["current_progress"]["completed_categories"][grocery_title] = {}
        self.current_progress["current_progress"]["completed_categories"][grocery_title][category_name] = [sub["sub_category_name"] for sub in sub_categories]
        self.scraped_progress["current_progress"]["completed_categories"][grocery_title][category_name] = [sub["sub_category_name"] for sub in sub_categories]
        self.save_current_progress()
        self.save_scraped_progress()

    async def process_sub_category(self, grocery_title, category_name, sub_category_info, talabat_grocery, page):
        sub_category_name = sub_category_info["sub_category_name"]
        logging.info(f"Processing sub-category: {sub_category_name} in category: {category_name} of grocery: {grocery_title}")
        
        self.current_progress["current_progress"]["current_sub_category"] = sub_category_name
        self.scraped_progress["current_progress"]["current_sub_category"] = sub_category_name
        self.save_current_progress()
        self.save_scraped_progress()

        try:
            items = await talabat_grocery.extract_all_items_from_sub_category(sub_category_info["sub_category_link"])
            sub_category_info["Items"] = items
            logging.info(f"Extracted {len(items)} items for sub-category {sub_category_name}")
        except Exception as e:
            logging.error(f"Error extracting items for sub-category {sub_category_name}: {e}", exc_info=True)
            sub_category_info["Items"] = []

    async def get_page_groceries(self, page) -> List[Dict]:
        logging.debug("Attempting to extract grocery information")
        retries = 3
        while retries > 0:
            try:
                await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=120000)
                vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
                logging.info(f"Found {len(vendor_containers)} grocery vendors on page")
                groceries_info = []
                for i, container in enumerate(vendor_containers, 1):
                    try:
                        title_element = await container.query_selector('a div h2')
                        grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"
                        link_element = await container.query_selector('a')
                        grocery_link = "https://www.talabat.com" + await link_element.get_attribute('href') if link_element else None
                        delivery_info = await container.query_selector('div.deliveryInfo')
                        delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"
                        delivery_time = re.findall(r'\d+', delivery_time_text)[0] + " mins" if re.findall(r'\d+', delivery_time_text) else "N/A"
                        if grocery_title and grocery_link and delivery_time != "N/A":
                            groceries_info.append({
                                "grocery_title": grocery_title,
                                "grocery_link": grocery_link,
                                "delivery_time": delivery_time
                            })
                    except Exception as e:
                        logging.error(f"Error processing grocery {i}: {e}", exc_info=True)
                return groceries_info
            except Exception as e:
                logging.error(f"Error extracting grocery info: {e}", exc_info=True)
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    def create_excel_sheet(self, workbook, sheet_name: str, data: Dict):
        sheet = workbook.create_sheet(title=sheet_name)
        simplified_data = []
        for grocery_title, grocery_data in data.items():
            general_info = {
                "Grocery Title": grocery_title,
                "Delivery Time": grocery_data.get("delivery_time", "N/A"),
                "Delivery Fees": grocery_data.get("grocery_details", {}).get("delivery_fees", "N/A"),
                "Minimum Order": grocery_data.get("grocery_details", {}).get("minimum_order", "N/A"),
                "URL": grocery_data.get("grocery_link", "N/A")
            }
            categories = grocery_data.get("grocery_details", {}).get("categories", [])
            for category in categories:
                category_name = category.get("name", "N/A")
                for sub_category in category.get("sub_categories", []):
                    sub_category_name = sub_category.get("sub_category_name", "N/A")
                    for item in sub_category.get("Items", []):
                        simplified_data.append({
                            **general_info,
                            "Category": category_name,
                            "Sub-Category": sub_category_name,
                            "Item Name": item.get("item_name", "N/A"),
                            "Item Price": item.get("item_price", "N/A"),
                            "Item Description": item.get("item_description", "N/A"),
                            "Item Link": item.get("item_link", "N/A")
                        })
            if not categories:
                simplified_data.append(general_info)
        
        if simplified_data:
            df = pd.DataFrame(simplified_data)
            for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
                for c_idx, value in enumerate(row, 1):
                    sheet.cell(row=r_idx, column=c_idx, value=value)
            for column in sheet.columns:
                max_length = max(len(str(cell.value or "")) for cell in column)
                column_letter = get_column_letter(column[0].column)
                sheet.column_dimensions[column_letter].width = min(max_length + 2, 50)
        else:
            sheet.cell(row=1, column=1, value="No data found for this area")

    def upload_to_drive(self, file_path):
        print(f"\nUploading {file_path} to Google Drive...")
        try:
            if not self.drive_uploader.authenticate():
                print("Failed to authenticate with Google Drive")
                return False
            file_ids = self.drive_uploader.upload_to_multiple_folders(file_path)
            return len(file_ids) == 2
        except Exception as e:
            print(f"Error uploading to Google Drive: {str(e)}")
            return False

    async def run(self):
        ahmadi_areas = [
            ("الظهر", "https://www.talabat.com/kuwait/groceries/59/dhaher"),
            ("الرقه", "https://www.talabat.com/kuwait/groceries/37/riqqa"),
            ("هدية", "https://www.talabat.com/kuwait/groceries/30/hadiya"),
            ("المنقف", "https://www.talabat.com/kuwait/groceries/32/mangaf"),
            ("أبو حليفة", "https://www.talabat.com/kuwait/groceries/2/abu-halifa"),
            ("الفنطاس", "https://www.talabat.com/kuwait/groceries/38/fintas"),
            ("العقيلة", "https://www.talabat.com/kuwait/groceries/79/egaila"),
            ("الصباحية", "https://www.talabat.com/kuwait/groceries/31/sabahiya"),
            ("الأحمدي", "https://www.talabat.com/kuwait/groceries/3/al-ahmadi"),
            ("الفحيحيل", "https://www.talabat.com/kuwait/groceries/5/fahaheel"),
            ("شرق الأحمدي", "https://www.talabat.com/kuwait/groceries/3/al-ahmadi"),
            ("ضاحية علي صباح السالم", "https://www.talabat.com/kuwait/groceries/82/ali-sabah-al-salem-umm-al-hayman"),
            ("ميناء عبد الله", "https://www.talabat.com/kuwait/groceries/100/mina-abdullah"),
            ("بنيدر", "https://www.talabat.com/kuwait/groceries/6650/bnaider"),
            ("الزور", "https://www.talabat.com/kuwait/groceries/2053/zour"),
            ("الجليعة", "https://www.talabat.com/kuwait/groceries/6860/al-julaiaa"),
            ("المهبولة", "https://www.talabat.com/kuwait/groceries/24/mahboula"),
            ("النويصيب", "https://www.talabat.com/kuwait/groceries/2054/nuwaiseeb"),
            ("الخيران", "https://www.talabat.com/kuwait/groceries/2726/khairan"),
            ("الوفرة", "https://www.talabat.com/kuwait/groceries/2057/wafra-farms"),
            ("ضاحية فهد الأحمد", "https://www.talabat.com/kuwait/groceries/98/fahad-al-ahmed"),
            ("ضاحية جابر العلي", "https://www.talabat.com/kuwait/groceries/60/jaber-al-ali"),
            ("مدينة صباح الأحمد السكنية", "https://www.talabat.com/kuwait/groceries/6931/sabah-al-ahmad-2"),
            ("مدينة صباح الأحمد البحرية", "https://www.talabat.com/kuwait/groceries/2726/khairan"),
            ("ميناء الأحمدي", "https://www.talabat.com/kuwait/groceries/3/al-ahmadi")
        ]
        
        excel_filename = os.path.join(self.output_dir, "الاحمدي_groceries.xlsx")
        workbook = Workbook()
        if "Sheet" in workbook.sheetnames:
            workbook.remove(workbook["Sheet"])
        
        completed_areas = self.current_progress["completed_areas"]
        current_area_index = self.current_progress["current_area_index"]
        
        print(f"Starting from area index {current_area_index}")
        print(f"Already completed areas: {', '.join(completed_areas) if completed_areas else 'None'}")
        
        resuming_area = self.current_progress["current_progress"]["area_name"]
        if resuming_area:
            for idx, (area_name, _) in enumerate(ahmadi_areas):
                if area_name == resuming_area:
                    print(f"Resuming from area {resuming_area} (index {idx})")
                    current_area_index = idx
                    self.current_progress["current_area_index"] = idx
                    self.scraped_progress["current_area_index"] = idx
                    self.save_current_progress()
                    self.save_scraped_progress()
                    self.commit_progress(f"Resuming from area {resuming_area}")
                    break
        
        for idx, (area_name, area_url) in enumerate(ahmadi_areas):
            if area_name in completed_areas and area_name != resuming_area:
                print(f"Skipping completed area: {area_name}")
                continue
            if idx < current_area_index:
                print(f"Skipping area {area_name} (index {idx} < {current_area_index})")
                continue
            
            self.current_progress["current_area_index"] = idx
            self.scraped_progress["current_area_index"] = idx
            self.save_current_progress()
            self.save_scraped_progress()
            self.commit_progress(f"Starting area {area_name} at index {idx}")
            
            try:
                area_results = await self.scrape_and_save_area(area_name, area_url)
                self.create_excel_sheet(workbook, area_name, {g["grocery_title"]: g for g in area_results})
                workbook.save(excel_filename)
                print(f"Updated Excel file: {excel_filename}")
                
                if area_name not in completed_areas:
                    completed_areas.append(area_name)
                    self.current_progress["completed_areas"] = completed_areas
                    self.scraped_progress["completed_areas"] = completed_areas
                self.save_current_progress()
                self.save_scraped_progress()
                self.print_progress_details()
                self.commit_progress(f"Completed area {area_name} in run")
                await asyncio.sleep(5)
            
            except Exception as e:
                print(f"Error processing area {area_name}: {e}")
                import traceback
                traceback.print_exc()
                self.save_current_progress()
                self.save_scraped_progress()
                self.commit_progress(f"Progress update after error in {area_name}")
        
        workbook.save(excel_filename)
        combined_json_filename = os.path.join(self.output_dir, "الاحمدي_groceries_all.json")
        with open(combined_json_filename, 'w', encoding='utf-8') as f:
            json.dump(self.scraped_progress["all_results"], f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*50}")
        print(f"SCRAPING COMPLETED")
        print(f"Excel file saved: {excel_filename}")
        print(f"Combined JSON saved: {combined_json_filename}")
        
        if len(completed_areas) == len(ahmadi_areas):
            if self.upload_to_drive(excel_filename):
                print(f"Uploaded Excel file to Google Drive")
            else:
                print(f"Failed to upload Excel file to Google Drive")
        else:
            print(f"Scraping incomplete ({len(completed_areas)}/{len(ahmadi_areas)} areas)")
        
        self.commit_progress("Final progress update after run")

def create_credentials_file():
    try:
        credentials_json = os.environ.get('TALABAT_GCLOUD_KEY_JSON')
        if not credentials_json:
            print("ERROR: TALABAT_GCLOUD_KEY_JSON not found!")
            return False
        with open('credentials.json', 'w') as f:
            f.write(credentials_json)
        print("Created credentials.json")
        return True
    except Exception as e:
        print(f"ERROR: Failed to create credentials.json: {str(e)}")
        return False

async def main():
    if not create_credentials_file():
        print("Could not create credentials.json")
        sys.exit(1)
    
    if not os.path.exists('credentials.json'):
        print("ERROR: credentials.json not found!")
        sys.exit(1)
    
    try:
        scraper = MainScraper()
        await scraper.run()
    except KeyboardInterrupt:
        print("\nInterrupted. Saving progress...")
        if 'scraper' in locals():
            scraper.save_current_progress()
            scraper.save_scraped_progress()
            scraper.commit_progress("Progress saved after interruption")
        print("Progress saved. Exiting.")
    except Exception as e:
        print(f"Critical error: {e}")
        import traceback
        traceback.print_exc()
        if 'scraper' in locals():
            scraper.save_current_progress()
            scraper.save_scraped_progress()
            scraper.commit_progress("Progress saved after critical error")
        sys.exit(1)

if __name__ == "__main__":
    scraper = MainScraper()
    scraper.print_progress_details()
    asyncio.run(scraper.run())





# import asyncio
# import json
# import os
# import re
# import nest_asyncio
# from bs4 import BeautifulSoup
# import pandas as pd
# from playwright.async_api import async_playwright
# from SavingOnDrive import SavingOnDrive

# class MainScraper:
#     def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
#         self.target_url = target_url
#         self.base_url = "https://www.talabat.com"
#         self.json_file = "talabat_groceries.json"
#         self.excel_file = "dhaher.xlsx"
#         self.groceries_data = {}
#         self.drive_uploader = SavingOnDrive('credentials.json')  # Initialize SavingOnDrive
#         print(f"Initialized MainScraper with target URL: {self.target_url}")

#         if not os.path.exists(self.json_file):
#             print(f"Creating new JSON file: {self.json_file}")
#             with open(self.json_file, 'w') as f:
#                 json.dump({}, f)
#         else:
#             print(f"Loading data from JSON file: {self.json_file}")
#             with open(self.json_file, 'r') as f:
#                 try:
#                     self.groceries_data = json.load(f)
#                     print(f"Loaded data: {self.groceries_data}")
#                 except json.JSONDecodeError:
#                     print("Error decoding JSON file. Starting with an empty dictionary.")
#                     self.groceries_data = {}

#     async def extract_grocery_info(self, page):
#         print("Attempting to extract grocery information")
#         retries = 3
#         while retries > 0:
#             try:
#                 vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
#                 print(f"Found {len(vendor_containers)} grocery vendors on page")
#                 groceries_info = []
#                 for i, container in enumerate(vendor_containers, 1):
#                     try:
#                         title_element = await container.query_selector('a div h2')
#                         grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"
#                         print(f"  Grocery title: {grocery_title}")
#                         link_element = await container.query_selector('a')
#                         grocery_link = self.base_url + await link_element.get_attribute('href') if link_element else None
#                         print(f"  Grocery link: {grocery_link}")
#                         delivery_info = await container.query_selector('div.deliveryInfo')
#                         delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"
#                         print(f"  Delivery time text: {delivery_time_text}")
#                         if delivery_time_text != "N/A":
#                             digits = re.findall(r'\d+', delivery_time_text)
#                             delivery_time = f"{digits[0]} mins" if digits else "N/A"
#                         else:
#                             delivery_time = "N/A"
#                         print(f"  Delivery time: {delivery_time}")
#                         if grocery_title and grocery_link and delivery_time:
#                             print(f"Found grocery: {grocery_title}, Delivery time: {delivery_time}")
#                             groceries_info.append({
#                                 'grocery_title': grocery_title,
#                                 'grocery_link': grocery_link,
#                                 'delivery_time': delivery_time
#                             })
#                     except Exception as e:
#                         print(f"Error processing grocery {i}: {e}")
#                 return groceries_info
#             except Exception as e:
#                 print(f"Error extracting grocery information: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return []

#     def save_to_json(self):
#         print("Saving data to JSON file")
#         try:
#             with open(self.json_file, 'w') as f:
#                 json.dump(self.groceries_data, f, indent=4)
#             print(f"Data saved to {self.json_file}")
#         except Exception as e:
#             print(f"Error saving to JSON file: {e}")
               
#     def save_to_excel(self):
#         print("Saving data to Excel file")
#         try:
#             writer = pd.ExcelWriter(self.excel_file, engine='xlsxwriter')
#             for grocery_title, grocery_data in self.groceries_data.items():
#                 flattened_data = []
#                 general_info = {
#                     'grocery_title': grocery_title,
#                     'delivery_time': grocery_data.get('delivery_time', 'N/A'),
#                     'delivery_fees': grocery_data.get('grocery_details', {}).get('delivery_fees', 'N/A'),
#                     'minimum_order': grocery_data.get('grocery_details', {}).get('minimum_order', 'N/A')
#                 }
#                 categories = grocery_data.get('grocery_details', {}).get('categories', [])
#                 for category in categories:
#                     category_name = category.get('name', 'N/A')
#                     for sub_category in category.get('sub_categories', []):
#                         sub_category_name = sub_category.get('sub_category_name', 'N/A')
#                         for item in sub_category.get('Items', []):
#                             item_data = {
#                                 **general_info,
#                                 'category': category_name,
#                                 'sub_category': sub_category_name,
#                                 'item_name': item.get('item_name', 'N/A'),
#                                 'item_price': item.get('item_price', 'N/A'),
#                                 'item_description': item.get('item_description', 'N/A'),
#                                 'item_link': item.get('item_link', 'N/A')
#                             }
#                             flattened_data.append(item_data)
#                 if not flattened_data:
#                     flattened_data.append(general_info)
#                 safe_title = re.sub(r'[\\/*?:\[\]]', '_', grocery_title)[:31]
#                 df = pd.DataFrame(flattened_data)
#                 df.to_excel(writer, sheet_name=safe_title, index=False)
#             writer.close()
#             print(f"Data saved to {self.excel_file}")
#             # Upload to Google Drive after saving
#             self.upload_to_drive()
#         except Exception as e:
#             print(f"Error saving to Excel: {e}")

#     def upload_to_drive(self):
#         """Upload the Excel file to Google Drive"""
#         print(f"\nUploading {self.excel_file} to Google Drive...")
#         try:
#             if not self.drive_uploader.authenticate():
#                 print("Failed to authenticate with Google Drive")
#                 return False
#             file_ids = self.drive_uploader.upload_to_multiple_folders(self.excel_file)
#             if len(file_ids) == len(self.drive_uploader.target_folders):
#                 print(f"Uploaded {self.excel_file} to Google Drive successfully")
#                 return True
#             else:
#                 print(f"Failed to upload {self.excel_file} to all target folders")
#                 return False
#         except Exception as e:
#             print(f"Error uploading to Google Drive: {str(e)}")
#             return False

#     async def process_grocery(self, grocery_info):
#         grocery_title = grocery_info['grocery_title']
#         grocery_link = grocery_info['grocery_link']
#         delivery_time = grocery_info['delivery_time']
#         print(f"Processing grocery: {grocery_title}")
#         if grocery_title in self.groceries_data:
#             print(f"Skipping {grocery_title} - already processed")
#             return
#         talabat_grocery = TalabatGroceries(grocery_link)
#         for browser_type in ["chromium", "firefox"]:
#             try:
#                 async with async_playwright() as p:
#                     browser = await p[browser_type].launch(headless=True)
#                     page = await browser.new_page()
#                     grocery_details = await talabat_grocery.extract_categories(page)
#                     self.groceries_data[grocery_title] = {
#                         'grocery_link': grocery_link,
#                         'delivery_time': delivery_time,
#                         'grocery_details': grocery_details
#                     }
#                     self.save_to_json()
#                     print(f"Successfully processed and saved data for {grocery_title}")
#                     break
#             except Exception as e:
#                 print(f"Error processing {grocery_title} with {browser_type}: {e}")
#                 continue
#             finally:
#                 await browser.close()

#     async def run(self):
#         print("Starting the scraper")
#         retries = 3
#         while retries > 0:
#             try:
#                 print(f"Target URL: {self.target_url}")
#                 async with async_playwright() as p:
#                     browser = await p.chromium.launch(headless=True)
#                     page = await browser.new_page()
#                     page.set_default_timeout(240000)
#                     await page.goto(self.target_url, timeout=240000)
#                     await page.wait_for_load_state("networkidle", timeout=240000)
#                     print("Page loaded successfully")
#                     try:
#                         await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=240000)
#                         print("Grocery vendor elements found")
#                     except Exception as e:
#                         print(f"Error waiting for vendor elements: {e}")
#                         CHIprint("Attempting to continue anyway...")
#                     groceries_info = await self.extract_grocery_info(page)
#                     await browser.close()
#                     print(f"Found {len(groceries_info)} groceries to process")
#                     for grocery_info in groceries_info:
#                         await self.process_grocery(grocery_info)
#                     self.save_to_excel()
#                     print("Scraping completed successfully")
#                 break
#             except Exception as e:
#                 print(f"Error in main scraper: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#                 if retries == 0:
#                     print("Failed to complete the scraping process after multiple attempts.")

# async def main():
#     if not os.path.exists('credentials.json'):
#         credentials_json = os.environ.get('TALABAT_GCLOUD_KEY_JSON')
#         if credentials_json:
#             with open('credentials.json', 'w') as f:
#                 f.write(credentials_json)
#             print("Created credentials.json from environment variable")
#         else:
#             print("ERROR: credentials.json not found and TALABAT_GCLOUD_KEY_JSON not set!")
#             return
#     scraper = MainScraper()
#     await scraper.run()

# if __name__ == "__main__":
#     asyncio.run(main())
# else:
#     asyncio.get_event_loop().run_until_complete(main())

# # class MainScraper:
# #     def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
# #         self.target_url = target_url
# #         self.base_url = "https://www.talabat.com"
# #         self.json_file = "talabat_groceries.json"
# #         self.excel_file = "dhaher.xlsx"
# #         self.groceries_data = {}
# #         print(f"Initialized MainScraper with target URL: {self.target_url}")

# #         if not os.path.exists(self.json_file):
# #             print(f"Creating new JSON file: {self.json_file}")
# #             with open(self.json_file, 'w') as f:
# #                 json.dump({}, f)
# #         else:
# #             print(f"Loading data from JSON file: {self.json_file}")
# #             with open(self.json_file, 'r') as f:
# #                 try:
# #                     self.groceries_data = json.load(f)
# #                     print(f"Loaded data: {self.groceries_data}")
# #                 except json.JSONDecodeError:
# #                     print("Error decoding JSON file. Starting with an empty dictionary.")
# #                     self.groceries_data = {}

# #     async def extract_grocery_info(self, page):
# #         print("Attempting to extract grocery information")
# #         retries = 3
# #         while retries > 0:
# #             try:
# #                 vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
# #                 print(f"Found {len(vendor_containers)} grocery vendors on page")

# #                 groceries_info = []
# #                 for i, container in enumerate(vendor_containers, 1):
# #                     try:
# #                         title_element = await container.query_selector('a div h2')
# #                         grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"
# #                         print(f"  Grocery title: {grocery_title}")

# #                         link_element = await container.query_selector('a')
# #                         grocery_link = self.base_url + await link_element.get_attribute('href') if link_element else None
# #                         print(f"  Grocery link: {grocery_link}")

# #                         delivery_info = await container.query_selector('div.deliveryInfo')
# #                         delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"
# #                         print(f"  Delivery time text: {delivery_time_text}")

# #                         if delivery_time_text != "N/A":
# #                             digits = re.findall(r'\d+', delivery_time_text)
# #                             delivery_time = f"{digits[0]} mins" if digits else "N/A"
# #                         else:
# #                             delivery_time = "N/A"
# #                         print(f"  Delivery time: {delivery_time}")

# #                         if grocery_title and grocery_link and delivery_time:
# #                             print(f"Found grocery: {grocery_title}, Delivery time: {delivery_time}")
# #                             groceries_info.append({
# #                                 'grocery_title': grocery_title,
# #                                 'grocery_link': grocery_link,
# #                                 'delivery_time': delivery_time
# #                             })
# #                     except Exception as e:
# #                         print(f"Error processing grocery {i}: {e}")
# #                 return groceries_info
# #             except Exception as e:
# #                 print(f"Error extracting grocery information: {e}")
# #                 retries -= 1
# #                 print(f"Retries left: {retries}")
# #                 await asyncio.sleep(5)
# #         return []

# #     def save_to_json(self):
# #         """Save scraped data to JSON file"""
# #         print("Saving data to JSON file")
# #         try:
# #             with open(self.json_file, 'w') as f:
# #                 json.dump(self.groceries_data, f, indent=4)
# #             print(f"Data saved to {self.json_file}")
# #         except Exception as e:
# #             print(f"Error saving to JSON file: {e}")
               
# #     def save_to_excel(self):
# #         """Save data from JSON to Excel file with each grocery in a separate sheet"""
# #         print("Saving data to Excel file")
# #         try:
# #             writer = pd.ExcelWriter(self.excel_file, engine='xlsxwriter')

# #             for grocery_title, grocery_data in self.groceries_data.items():
# #                 flattened_data = []

# #                 general_info = {
# #                     'grocery_title': grocery_title,
# #                     'delivery_time': grocery_data.get('delivery_time', 'N/A'),
# #                     'delivery_fees': grocery_data.get('grocery_details', {}).get('delivery_fees', 'N/A'),
# #                     'minimum_order': grocery_data.get('grocery_details', {}).get('minimum_order', 'N/A')
# #                 }

# #                 categories = grocery_data.get('grocery_details', {}).get('categories', [])
# #                 for category in categories:
# #                     category_name = category.get('name', 'N/A')
# #                     for sub_category in category.get('sub_categories', []):
# #                         sub_category_name = sub_category.get('sub_category_name', 'N/A')
# #                         for item in sub_category.get('Items', []):
# #                             item_data = {
# #                                 **general_info,
# #                                 'category': category_name,
# #                                 'sub_category': sub_category_name,
# #                                 'item_name': item.get('item_name', 'N/A'),
# #                                 'item_price': item.get('item_price', 'N/A'),
# #                                 'item_description': item.get('item_description', 'N/A'),
# #                                 'item_link': item.get('item_link', 'N/A')
# #                             }
# #                             flattened_data.append(item_data)

# #                 if not flattened_data:
# #                     flattened_data.append(general_info)

# #                 safe_title = re.sub(r'[\\/*?:\[\]]', '_', grocery_title)[:31]
# #                 df = pd.DataFrame(flattened_data)
# #                 df.to_excel(writer, sheet_name=safe_title, index=False)

# #             writer.close()
# #             print(f"Data saved to {self.excel_file}")
# #         except Exception as e:
# #             print(f"Error saving to Excel: {e}")

# #     async def process_grocery(self, grocery_info):
# #         """Process a single grocery using TalabatGroceries class"""
# #         grocery_title = grocery_info['grocery_title']
# #         grocery_link = grocery_info['grocery_link']
# #         delivery_time = grocery_info['delivery_time']

# #         print(f"Processing grocery: {grocery_title}")

# #         # Skip if already processed
# #         if grocery_title in self.groceries_data:
# #             print(f"Skipping {grocery_title} - already processed")
# #             return

# #         # Initialize TalabatGroceries with the grocery link
# #         talabat_grocery = TalabatGroceries(grocery_link)

# #         # Extract grocery details using Playwright
# #         for browser_type in ["chromium", "firefox"]:
# #             try:
# #                 async with async_playwright() as p:
# #                     browser = await p[browser_type].launch(headless=True)
# #                     page = await browser.new_page()

# #                     grocery_details = await talabat_grocery.extract_categories(page)

# #                     # Store data in groceries_data dictionary
# #                     self.groceries_data[grocery_title] = {
# #                         'grocery_link': grocery_link,
# #                         'delivery_time': delivery_time,
# #                         'grocery_details': grocery_details
# #                     }

# #                     # Save progress after each grocery
# #                     self.save_to_json()
# #                     print(f"Successfully processed and saved data for {grocery_title}")
# #                     break  # Exit the loop if successful
# #             except Exception as e:
# #                 print(f"Error processing {grocery_title} with {browser_type}: {e}")
# #                 continue  # Try the next browser type
# #             finally:
# #                 await browser.close()

# #     async def run(self):
# #         """Main method to run the scraper"""
# #         print("Starting the scraper")
# #         retries = 3
# #         while retries > 0:
# #             try:
# #                 print(f"Target URL: {self.target_url}")

# #                 # Initialize playwright and navigate to target URL
# #                 async with async_playwright() as p:
# #                     browser = await p.chromium.launch(headless=True)  # Always use headless mode
# #                     page = await browser.new_page()

# #                     # Set longer timeouts and wait for page load
# #                     page.set_default_timeout(240000)  # 240 seconds

# #                     # Navigate to the target URL
# #                     await page.goto(self.target_url, timeout=240000)
# #                     await page.wait_for_load_state("networkidle", timeout=240000)
# #                     print("Page loaded successfully")

# #                     # Wait for grocery vendor elements to load
# #                     try:
# #                         await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=240000)
# #                         print("Grocery vendor elements found")
# #                     except Exception as e:
# #                         print(f"Error waiting for vendor elements: {e}")
# #                         print("Attempting to continue anyway...")

# #                     # Extract grocery information directly from page
# #                     groceries_info = await self.extract_grocery_info(page)
# #                     await browser.close()

# #                     print(f"Found {len(groceries_info)} groceries to process")

# #                     # Process each grocery sequentially
# #                     for grocery_info in groceries_info:
# #                         await self.process_grocery(grocery_info)

# #                     # Save all data to Excel
# #                     self.save_to_excel()
# #                     print("Scraping completed successfully")
# #                 break  # Exit retry loop if successful
# #             except Exception as e:
# #                 print(f"Error in main scraper: {e}")
# #                 retries -= 1
# #                 print(f"Retries left: {retries}")
# #                 await asyncio.sleep(5)
# #                 if retries == 0:
# #                     print("Failed to complete the scraping process after multiple attempts.")


# # # Main execution point - now compatible with notebook environments
# # async def main():
# #     # Initialize and run the scraper
# #     scraper = MainScraper()
# #     await scraper.run()

# # # Handle both script and notebook execution
# # if __name__ == "__main__":
# #     asyncio.run(main())
# # else:
# #     # For notebook/IPython environment, use this method to run
# #     asyncio.get_event_loop().run_until_complete(main())

