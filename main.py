import asyncio
import json
import os
import re
import nest_asyncio
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import datetime

nest_asyncio.apply()

# Set up logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class SavingOnDrive:
    """Class to handle uploading files to Google Drive with date-based folders"""
    def __init__(self, credentials_path='credentials.json'):
        self.credentials_path = credentials_path
        self.drive_service = None
        self.target_folders = [
            "1NzaP1VFqfSdzkCqfPkcpI5s_43nLDLAG",  # First folder
            "1hxBqJwK5g7EXAV0JcVc0_YLtBReCkaRv"   # Second folder
        ]
    
    def authenticate(self):
        try:
            SCOPES = ['https://www.googleapis.com/auth/drive']
            credentials = Credentials.from_service_account_file(
                self.credentials_path, scopes=SCOPES)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            return True
        except Exception as e:
            logging.error(f"Authentication error: {str(e)}")
            return False
    
    def create_date_folder(self, parent_folder_id):
        try:
            today_date = datetime.datetime.now().strftime("%Y-%m-%d")
            query = f"name='{today_date}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(
                q=query, spaces='drive', fields='files(id, name)').execute()
            existing_folders = results.get('files', [])
            if existing_folders:
                logging.info(f"Folder {today_date} already exists in parent folder {parent_folder_id}")
                return existing_folders[0]['id']
            folder_metadata = {
                'name': today_date,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = self.drive_service.files().create(
                body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')
            logging.info(f"Created folder {today_date} with ID: {folder_id} in parent folder {parent_folder_id}")
            return folder_id
        except Exception as e:
            logging.error(f"Error creating date folder: {str(e)}")
            return None
    
    def upload_file(self, file_path, folder_id, file_name=None):
        try:
            if not self.drive_service and not self.authenticate():
                return None
            if file_name is None:
                file_name = os.path.basename(file_path)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            name_parts = os.path.splitext(file_name)
            unique_file_name = f"{name_parts[0]}_{timestamp}{name_parts[1]}"
            file_metadata = {'name': unique_file_name, 'parents': [folder_id]}
            media = MediaFileUpload(
                file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)
            file = self.drive_service.files().create(
                body=file_metadata, media_body=media, fields='id').execute()
            logging.info(f"File uploaded successfully to folder {folder_id}")
            return file.get('id')
        except Exception as e:
            logging.error(f"Upload error: {str(e)}")
            return None
    
    def upload_to_multiple_folders(self, file_path, file_name=None):
        if not self.authenticate():
            logging.error("Failed to authenticate with Google Drive")
            return []
        file_ids = []
        for parent_folder_id in self.target_folders:
            date_folder_id = self.create_date_folder(parent_folder_id)
            if date_folder_id:
                file_id = self.upload_file(file_path, date_folder_id, file_name)
                if file_id:
                    file_ids.append(file_id)
            else:
                logging.error(f"Failed to create/find date folder in parent folder {parent_folder_id}")
        return file_ids

class TalabatGroceries:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.talabat.com"
        logging.info(f"Initialized TalabatGroceries with URL: {self.url}")

    async def get_general_link(self, page):
        logging.info("Attempting to get general link")
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
        print("Attempting to get delivery fees")
        retries = 3
        while retries > 0:
            try:
                delivery_fees_element = await page.query_selector('//div[contains(@class, "delivery-fee")]//span')
                delivery_fees = await delivery_fees_element.inner_text() if delivery_fees_element else "N/A"
                print(f"Delivery fees: {delivery_fees}")
                return delivery_fees
            except PlaywrightTimeoutError:
                print("Timeout waiting for delivery fees")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return "N/A"

    async def get_minimum_order(self, page):
        print("Attempting to get minimum order")
        retries = 3
        while retries > 0:
            try:
                minimum_order_element = await page.query_selector('//div[contains(@class, "minimum-order")]//span')
                minimum_order = await minimum_order_element.inner_text() if minimum_order_element else "N/A"
                print(f"Minimum order: {minimum_order}")
                return minimum_order
            except PlaywrightTimeoutError:
                print("Timeout waiting for minimum order")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return "N/A"

    async def extract_category_names(self, page):
        print("Attempting to extract category names")
        retries = 3
        while retries > 0:
            try:
                await page.wait_for_selector('//span[@data-testid="category-name"]', timeout=60000)
                category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
                category_names = [await element.inner_text() for element in category_name_elements]
                print(f"Category names extracted: {category_names}")
                return category_names
            except PlaywrightTimeoutError:
                print("Timeout waiting for category names")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_category_links(self, page):
        print("Attempting to extract category links")
        retries = 3
        while retries > 0:
            try:
                await page.wait_for_selector('//a[@data-testid="category-item-container"]', timeout=60000)
                category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
                category_links = [self.base_url + await element.get_attribute('href') for element in category_link_elements]
                print(f"Category links extracted: {category_links}")
                return category_links
            except PlaywrightTimeoutError:
                print("Timeout waiting for category links")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []


    async def extract_item_details(self, item_link):
        print(f"Attempting to extract item details for {item_link}")
        default_values = {"item_price": "N/A", "item_description": "N/A", "item_delivery_time_range": "N/A", "item_images": []}
        for browser_type in ["chromium", "firefox"]:
            result = await self.extract_item_details_new_tab(item_link, browser_type)
            if result != default_values:
                return result
        return default_values

    async def extract_all_items_from_sub_category(self, sub_category_link):
        print(f"Attempting to extract all items from sub-category: {sub_category_link}")
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
                        print(f"Timeout waiting for 'category-items-container' on {sub_category_link}")
                        html_content = await sub_page.content()
                        print(f"Page HTML excerpt: {html_content[:500]}...")
                        await sub_page.wait_for_selector('//div[contains(@class, "items")] | //div[contains(@class, "products")]', timeout=60000)
    
                    pagination_element = await sub_page.query_selector('//ul[@class="paginate-wrap"]')
                    total_pages = 1
                    if pagination_element:
                        page_numbers = await pagination_element.query_selector_all('//li[contains(@class, "paginate-li")]//a')
                        total_pages = len(page_numbers) if page_numbers else 1
                    print(f"      Found {total_pages} pages in this sub-category")
    
                    items = []
                    for page_number in range(1, total_pages + 1):
                        print(f"      Processing page {page_number} of {total_pages}")
                        page_url = f"{sub_category_link}&page={page_number}" if page_number > 1 else sub_category_link
                        await sub_page.goto(page_url, timeout=120000)
                        await sub_page.wait_for_load_state("networkidle", timeout=120000)
                        # Filter for product-specific links only
                        item_elements = await sub_page.query_selector_all('//a[@data-testid="grocery-item-link-nofollow" and contains(@href, "/product/")]')
                        print(f"        Found {len(item_elements)} items on page {page_number}")
                        for i, element in enumerate(item_elements):
                            try:
                                # Broader selector for item name
                                item_name_element = await element.query_selector('div[data-test="item-name"], span[class*="name"], h3, p[class*="title"], div[class*="name"]')
                                item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {i+1}"
                                print(f"        Item name: {item_name}")
                                item_link = self.base_url + await element.get_attribute('href')
                                print(f"        Item link: {item_link}")
                                item_details = await self.extract_item_details(item_link)
                                items.append({"item_name": item_name, "item_link": item_link, **item_details})
                            except Exception as e:
                                print(f"        Error processing item {i+1}: {e}")
                    await browser.close()
                    if items:
                        return items
            except Exception as e:
                print(f"Error extracting items from sub-category {sub_category_link} using {browser_type}: {e}")
                continue
        return default_values
    
    async def extract_item_details_new_tab(self, item_link, browser_type):
        print(f"Attempting to extract item details for {item_link} using {browser_type}")
        retries = 3
        while retries > 0:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    page = await browser.new_page()
                    await page.goto(item_link, timeout=90000)  # Increased to 90s
                    await page.wait_for_load_state("networkidle", timeout=90000)
                    # Scroll to trigger dynamic content
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(5000)  # Wait 5s for content to load
    
                    # Price extraction
                    item_price_element = await page.query_selector(
                        '//div[contains(@class, "price")]//span[contains(text(), "KD")] | '
                        '//span[contains(@class, "price") and contains(text(), "KD")] | '
                        '//div[@data-testid="price"]//span | '
                        '//span[contains(@class, "amount")] | '
                        '//div[contains(@class, "price")]'
                    )
                    if item_price_element:
                        item_price = await item_price_element.inner_text()
                        # Combine currency and amount if split
                        currency_element = await page.query_selector('//span[contains(text(), "KD")]')
                        amount_element = await page.query_selector('//span[contains(@class, "amount")] | //span[not(contains(text(), "KD")) and contains(., ".")]')
                        if currency_element and amount_element:
                            currency = await currency_element.inner_text()
                            amount = await amount_element.inner_text()
                            item_price = f"{currency} {amount}".strip()
                        elif "KD" in item_price and any(c.isdigit() for c in item_price):
                            pass  # Full price already captured
                        else:
                            item_price = "N/A"
                    else:
                        item_price = "N/A"
                    print(f"Item price: {item_price}")
    
                    # Description extraction
                    item_description_element = await page.query_selector(
                        '//div[contains(@class, "description")]//p[not(.="Description")] | '
                        '//p[@data-testid="item-description"] | '
                        '//div[@data-testid="description"]//p[not(.="Description")] | '
                        '//div[contains(@class, "product-details")]//p | '
                        '//div[contains(@class, "about")]//p | '
                        '//p[contains(@class, "description") and not(.="Description")]'
                    )
                    item_description = await item_description_element.inner_text() if item_description_element else "N/A"
                    # Fallback if description is empty or too short
                    if item_description == "N/A" or len(item_description.strip()) < 5:
                        fallback_desc = await page.query_selector('//div[contains(@class, "details")]//text()[normalize-space()]')
                        item_description = await fallback_desc.inner_text() if fallback_desc else "N/A"
                    print(f"Item description: {item_description}")
    
                    # Delivery time (already working)
                    delivery_time_element = await page.query_selector(
                        '//div[@data-testid="delivery-tag"]//span | '
                        '//span[contains(@class, "delivery")] | '
                        '//div[contains(@class, "delivery-time")]//span'
                    )
                    delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"
                    print(f"Delivery time range: {delivery_time}")
    
                    # Images (already working)
                    item_image_elements = await page.query_selector_all(
                        '//img[contains(@class, "item-image")] | '
                        '//div[@data-testid="item-image"]//img | '
                        '//img[@alt="product image"]'
                    )
                    item_images = [await img.get_attribute('src') for img in item_image_elements if await img.get_attribute('src')]
                    print(f"Item images: {item_images}")
    
                    await browser.close()
                    return {
                        "item_price": item_price,
                        "item_description": item_description,
                        "item_delivery_time_range": delivery_time,
                        "item_images": item_images
                    }
            except Exception as e:
                print(f"Error extracting item details for {item_link} using {browser_type}: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return {"item_price": "N/A", "item_description": "N/A", "item_delivery_time_range": "N/A", "item_images": []}
    
    async def extract_sub_categories(self, page, category_link):
        print(f"Attempting to extract sub-categories for category: {category_link}")
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
                        print(f"    Processing sub-category: {sub_category_name}")
                        print(f"    Sub-category link: {sub_category_link}")
                        items = await self.extract_all_items_from_sub_category(sub_category_link)
                        sub_categories.append({
                            "sub_category_name": sub_category_name,
                            "sub_category_link": sub_category_link,
                            "Items": items
                        })
                    except Exception as e:
                        print(f"Error processing sub-category: {e}")
                print(f"Extracted {len(sub_categories)} sub-categories")
                return sub_categories
            except PlaywrightTimeoutError:
                print("Timeout waiting for sub-categories")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_categories(self, page):
        print(f"Processing grocery: {self.url}")
        retries = 3
        while retries > 0:
            try:
                await page.goto(self.url, timeout=60000)
                await page.wait_for_load_state("networkidle", timeout=60000)
                print("Page loaded successfully")
                delivery_fees = await self.get_delivery_fees(page)
                minimum_order = await self.get_minimum_order(page)
                view_all_link = await self.get_general_link(page)
                print(f"  Delivery fees: {delivery_fees}")
                print(f"  Minimum order: {minimum_order}")
                if view_all_link:
                    print(f"  Navigating to view all link: {view_all_link}")
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True)
                        category_page = await browser.new_page()
                        await category_page.goto(view_all_link, timeout=60000)
                        await category_page.wait_for_load_state("networkidle", timeout=60000)
                        category_names = await self.extract_category_names(category_page)
                        category_links = await self.extract_category_links(category_page)
                        print(f"  Found {len(category_names)} categories")
                        categories_data = []
                        for index, (name, link) in enumerate(zip(category_names, category_links)):
                            print(f"  Processing category {index+1}/{len(category_names)}: {name}")
                            print(f"  Category link: {link}")
                            async with async_playwright() as p:
                                browser = await p.chromium.launch(headless=True)
                                sub_category_page = await browser.new_page()
                                await sub_category_page.goto(link, timeout=60000)
                                await sub_category_page.wait_for_load_state("networkidle", timeout=60000)
                                sub_categories = await self.extract_sub_categories(sub_category_page, link)
                                await browser.close()
                            print(f"  Found {len(sub_categories)} sub-categories in {name}")
                            categories_data.append({"name": name, "link": link, "sub_categories": sub_categories})
                        await browser.close()
                return {
                    "delivery_fees": delivery_fees,
                    "minimum_order": minimum_order,
                    "categories": categories_data
                }
            except Exception as e:
                print(f"Error extracting categories: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return {"error": "Failed to extract categories after multiple attempts"}

class MainScraper:
    def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
        self.target_url = target_url
        self.base_url = "https://www.talabat.com"
        self.progress_file = "progress.json"
        self.results_file = "all_results.json"
        self.excel_file = "dhaher.xlsx"
        self.progress_data = {
            "current_area": "dhaher",
            "current_grocery": None,
            "current_category": None,
            "current_sub_category": None,
            "completed_groceries": {}
        }
        self.all_results = {}
        self.drive_uploader = SavingOnDrive('credentials.json')
        logging.info(f"Initialized MainScraper with target URL: {self.target_url}")
        self.load_progress()
        self.load_results()

    def load_progress(self):
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r') as f:
                try:
                    self.progress_data = json.load(f)
                    if not isinstance(self.progress_data, dict) or 'current_area' not in self.progress_data:
                        raise ValueError("Invalid progress data structure")
                    logging.info(f"Loaded progress from {self.progress_file}")
                except (json.JSONDecodeError, ValueError) as e:
                    logging.error(f"Error decoding or validating {self.progress_file}: {e}. Starting with default progress.")
                    self.progress_data = {
                        "current_area": "dhaher",
                        "current_grocery": None,
                        "current_category": None,
                        "current_sub_category": None,
                        "completed_groceries": {}
                    }
        else:
            logging.info(f"Creating new progress file: {self.progress_file}")
            self.save_progress()

    def load_results(self):
        if os.path.exists(self.results_file):
            with open(self.results_file, 'r') as f:
                try:
                    self.all_results = json.load(f)
                    logging.info(f"Loaded results from {self.results_file}")
                except json.JSONDecodeError:
                    logging.error(f"Error decoding {self.results_file}. Starting with empty results.")
        else:
            logging.info(f"Creating new results file: {self.results_file}")
            self.save_results()

    def save_progress(self):
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress_data, f, indent=4)
            logging.info(f"Progress saved to {self.progress_file}")
        except Exception as e:
            logging.error(f"Error saving progress: {e}")

    def save_results(self):
        try:
            with open(self.results_file, 'w') as f:
                json.dump(self.all_results, f, indent=4)
            logging.info(f"Results saved to {self.results_file}")
        except Exception as e:
            logging.error(f"Error saving results: {e}")

    def save_to_excel(self):
        print("Saving data to Excel file")
        try:
            writer = pd.ExcelWriter(self.excel_file, engine='xlsxwriter')
            for grocery_title, grocery_data in self.all_results.items():
                flattened_data = []
                general_info = {
                    'grocery_title': grocery_title,
                    'delivery_time': grocery_data.get('delivery_time', 'N/A'),
                    'delivery_fees': grocery_data.get('grocery_details', {}).get('delivery_fees', 'N/A'),
                    'minimum_order': grocery_data.get('grocery_details', {}).get('minimum_order', 'N/A')
                }
                categories = grocery_data.get('grocery_details', {}).get('categories', [])
                for category in categories:
                    category_name = category.get('name', 'N/A')
                    for sub_category in category.get('sub_categories', []):
                        sub_category_name = sub_category.get('sub_category_name', 'N/A')
                        for item in sub_category.get('Items', []):
                            flattened_data.append({
                                **general_info,
                                'category': category_name,
                                'sub_category': sub_category_name,
                                'item_name': item.get('item_name', 'N/A'),
                                'item_price': item.get('item_price', 'N/A'),
                                'item_description': item.get('item_description', 'N/A'),
                                'item_link': item.get('item_link', 'N/A')
                            })
                if not flattened_data:
                    flattened_data.append(general_info)
                safe_title = re.sub(r'[\\/*?:\[\]]', '_', grocery_title)[:31]
                df = pd.DataFrame(flattened_data)
                df.to_excel(writer, sheet_name=safe_title, index=False)
            writer.close()
            print(f"Data saved to {self.excel_file}")
            self.upload_to_drive()
        except Exception as e:
            print(f"Error saving to Excel: {e}")

    def upload_to_drive(self):
        print(f"\nUploading {self.excel_file} to Google Drive...")
        try:
            file_ids = self.drive_uploader.upload_to_multiple_folders(self.excel_file)
            if len(file_ids) == len(self.drive_uploader.target_folders):
                print(f"Uploaded {self.excel_file} to Google Drive successfully")
            else:
                print(f"Failed to upload {self.excel_file} to all target folders")
        except Exception as e:
            print(f"Error uploading to Google Drive: {str(e)}")

    async def extract_grocery_info(self, page):
        logging.info("Attempting to extract grocery information")
        retries = 3
        while retries > 0:
            try:
                await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=60000)
                vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
                logging.info(f"Found {len(vendor_containers)} grocery vendors on page")
                groceries_info = []
                for i, container in enumerate(vendor_containers, 1):
                    try:
                        title_element = await container.query_selector('a div h2')
                        grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"
                        logging.info(f"  Grocery title: {grocery_title}")
                        link_element = await container.query_selector('a')
                        grocery_link = self.base_url + await link_element.get_attribute('href') if link_element else None
                        logging.info(f"  Grocery link: {grocery_link}")
                        delivery_info = await container.query_selector('div.deliveryInfo')
                        delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"
                        logging.info(f"  Delivery time text: {delivery_time_text}")
                        delivery_time = re.findall(r'\d+', delivery_time_text)[0] + " mins" if re.findall(r'\d+', delivery_time_text) else "N/A"
                        logging.info(f"  Delivery time: {delivery_time}")
                        if grocery_title and grocery_link and delivery_time != "N/A":
                            groceries_info.append({
                                'grocery_title': grocery_title,
                                'grocery_link': grocery_link,
                                'delivery_time': delivery_time
                            })
                    except Exception as e:
                        logging.error(f"Error processing grocery {i}: {e}")
                return groceries_info
            except Exception as e:
                logging.error(f"Error extracting grocery info: {e}")
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)
        logging.warning("Failed to extract grocery info after all retries")
        return []  # Always return a list
        
    async def process_sub_category(self, grocery_title, category_name, sub_category_info, talabat_grocery, page):
        sub_category_name = sub_category_info['sub_category_name']
        sub_category_link = sub_category_info['sub_category_link']
        logging.info(f"Processing sub-category: {sub_category_name} in category: {category_name} of grocery: {grocery_title}")
        
        self.progress_data['current_sub_category'] = sub_category_name
        self.save_progress()

        items = await talabat_grocery.extract_all_items_from_sub_category(sub_category_link)
        sub_category_info['Items'] = items

        if grocery_title not in self.all_results:
            self.all_results[grocery_title] = {
                'grocery_link': talabat_grocery.url,
                'delivery_time': self.progress_data['completed_groceries'].get(grocery_title, {}).get('delivery_time', 'N/A'),
                'grocery_details': {'categories': []}
            }
        grocery_data = self.all_results[grocery_title]
        categories = grocery_data['grocery_details']['categories']
        category_exists = next((c for c in categories if c['name'] == category_name), None)
        if not category_exists:
            categories.append({'name': category_name, 'link': '', 'sub_categories': []})
            category_exists = categories[-1]
        sub_category_exists = next((s for s in category_exists['sub_categories'] if s['sub_category_name'] == sub_category_name), None)
        if not sub_category_exists:
            category_exists['sub_categories'].append(sub_category_info)
        else:
            sub_category_exists['Items'] = items

        self.save_results()
        
    async def process_category(self, grocery_title, category_info, talabat_grocery, page):
        category_name = category_info['name']
        category_link = category_info['link']
        print(f"Processing category: {category_name} in grocery: {grocery_title}")
        
        self.progress_data['current_category'] = category_name
        self.save_progress()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            sub_category_page = await browser.new_page()
            await sub_category_page.goto(category_link, timeout=60000)
            await sub_category_page.wait_for_load_state("networkidle", timeout=60000)
            sub_categories = await talabat_grocery.extract_sub_categories(sub_category_page, category_link)
            await browser.close()

        # Check progress to resume from last sub-category
        completed_sub_categories = self.progress_data['completed_groceries'].get(grocery_title, {}).get('completed_categories', {}).get(category_name, [])
        for sub_category in sub_categories:
            if sub_category['sub_category_name'] not in completed_sub_categories:
                await self.process_sub_category(grocery_title, category_name, sub_category, talabat_grocery, page)
                # Mark sub-category as completed
                if grocery_title not in self.progress_data['completed_groceries']:
                    self.progress_data['completed_groceries'][grocery_title] = {'completed_categories': {}}
                if category_name not in self.progress_data['completed_groceries'][grocery_title]['completed_categories']:
                    self.progress_data['completed_groceries'][grocery_title]['completed_categories'][category_name] = []
                self.progress_data['completed_groceries'][grocery_title]['completed_categories'][category_name].append(sub_category['sub_category_name'])
                self.save_progress()

    async def process_grocery(self, grocery_info):
        grocery_title = grocery_info['grocery_title']
        grocery_link = grocery_info['grocery_link']
        delivery_time = grocery_info['delivery_time']
        print(f"Processing grocery: {grocery_title}")
        
        self.progress_data['current_grocery'] = grocery_title
        self.progress_data['completed_groceries'][grocery_title] = self.progress_data['completed_groceries'].get(grocery_title, {})
        self.progress_data['completed_groceries'][grocery_title]['delivery_time'] = delivery_time
        self.save_progress()

        if grocery_title in self.progress_data['completed_groceries'] and 'completed' in self.progress_data['completed_groceries'][grocery_title]:
            print(f"Skipping {grocery_title} - already fully processed")
            return

        talabat_grocery = TalabatGroceries(grocery_link)
        for browser_type in ["chromium", "firefox"]:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    page = await browser.new_page()
                    grocery_details = await talabat_grocery.extract_categories(page)
                    self.all_results[grocery_title] = {
                        'grocery_link': grocery_link,
                        'delivery_time': delivery_time,
                        'grocery_details': grocery_details
                    }
                    self.save_results()

                    # Process categories incrementally
                    completed_categories = self.progress_data['completed_groceries'].get(grocery_title, {}).get('completed_categories', {})
                    for category in grocery_details['categories']:
                        if category['name'] not in completed_categories or not completed_categories[category['name']]:
                            await self.process_category(grocery_title, category, talabat_grocery, page)
                            # Mark category as fully processed if all sub-categories are done
                            if all(sub['sub_category_name'] in completed_categories.get(category['name'], []) for sub in category['sub_categories']):
                                self.progress_data['completed_groceries'][grocery_title]['completed_categories'][category['name']] = [s['sub_category_name'] for s in category['sub_categories']]

                    # Mark grocery as completed if all categories are done
                    if all(cat['name'] in completed_categories for cat in grocery_details['categories']):
                        self.progress_data['completed_groceries'][grocery_title]['completed'] = True
                    self.save_progress()
                    await browser.close()
                    break
            except Exception as e:
                print(f"Error processing {grocery_title} with {browser_type}: {e}")
                continue

    async def run(self):
        logging.info("Starting the scraper")
        retries = 3
        while retries > 0:
            try:
                logging.info(f"Target URL: {self.target_url}")
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()
                    page.set_default_timeout(60000)
                    await page.goto(self.target_url, timeout=60000)
                    await page.wait_for_load_state("networkidle", timeout=60000)
                    logging.info("Page loaded successfully")
                    groceries_info = await self.extract_grocery_info(page)
                    await browser.close()
                    if groceries_info is None:
                        logging.warning("No groceries info retrieved, treating as empty list")
                        groceries_info = []
                    logging.info(f"Found {len(groceries_info)} groceries to process")

                    start_processing = False if self.progress_data['current_grocery'] else True
                    for grocery_info in groceries_info:
                        if start_processing or grocery_info['grocery_title'] == self.progress_data['current_grocery']:
                            start_processing = True
                            await self.process_grocery(grocery_info)

                    self.save_to_excel()
                    logging.info("Scraping completed successfully")
                break
            except Exception as e:
                logging.error(f"Error in main scraper: {e}")
                retries -= 1
                logging.info(f"Retries left: {retries}")
                await asyncio.sleep(5)

async def main():
    if not os.path.exists('credentials.json'):
        credentials_json = os.environ.get('TALABAT_GCLOUD_KEY_JSON')
        if credentials_json:
            with open('credentials.json', 'w') as f:
                f.write(credentials_json)
            logging.info("Created credentials.json from environment variable")
        else:
            logging.error("ERROR: credentials.json not found and TALABAT_GCLOUD_KEY_JSON not set!")
            return
    scraper = MainScraper()
    await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())
else:
    asyncio.get_event_loop().run_until_complete(main())





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

