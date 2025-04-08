import asyncio
import json
import os
import re
import nest_asyncio
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import datetime
import tempfile
import subprocess
import sys

nest_asyncio.apply()

class SavingOnDrive:
    """Class to handle uploading files to Google Drive with date-based folders"""
    def __init__(self, credentials_path='credentials.json'):
        self.credentials_path = credentials_path
        self.drive_service = None
        self.target_folders = [
            "1NzaP1VFqfSdzkCqfPkcpI5s_43nLDLAG",
            "1hxBqJwK5g7EXAV0JcVc0_YLtBReCkaRv"
        ]
    
    def authenticate(self):
        try:
            SCOPES = ['https://www.googleapis.com/auth/drive']
            credentials = Credentials.from_service_account_file(self.credentials_path, scopes=SCOPES)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            return True
        except Exception as e:
            print(f"Authentication error: {str(e)}")
            return False
    
    def create_date_folder(self, parent_folder_id):
        try:
            today_date = datetime.datetime.now().strftime("%Y-%m-%d")
            query = f"name='{today_date}' and mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
            existing_folders = results.get('files', [])
            if existing_folders:
                print(f"Folder {today_date} already exists in parent folder {parent_folder_id}")
                return existing_folders[0]['id']
            folder_metadata = {'name': today_date, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_folder_id]}
            folder = self.drive_service.files().create(body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')
            print(f"Created folder {today_date} with ID: {folder_id} in parent folder {parent_folder_id}")
            return folder_id
        except Exception as e:
            print(f"Error creating date folder: {str(e)}")
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
            media = MediaFileUpload(file_path, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)
            file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"File uploaded successfully to folder {folder_id}")
            return file.get('id')
        except Exception as e:
            print(f"Upload error: {str(e)}")
            return None
    
    def upload_to_multiple_folders(self, file_path, file_name=None):
        if not self.authenticate():
            print("Failed to authenticate with Google Drive")
            return []
        file_ids = []
        for parent_folder_id in self.target_folders:
            date_folder_id = self.create_date_folder(parent_folder_id)
            if date_folder_id:
                file_id = self.upload_file(file_path, date_folder_id, file_name)
                if file_id:
                    file_ids.append(file_id)
            else:
                print(f"Failed to create/find date folder in parent folder {parent_folder_id}")
        return file_ids

class TalabatGroceries:
    def __init__(self, url, main_scraper):
        self.url = url
        self.base_url = "https://www.talabat.com"
        self.main_scraper = main_scraper
        print(f"Initialized TalabatGroceries with URL: {self.url}")

    async def get_general_link(self, page):
        print("Attempting to get general link")
        retries = 3
        while retries > 0:
            try:
                link_element = await page.wait_for_selector('//a[@data-testid="view-all-link"]', timeout=60000)
                if link_element:
                    full_link = self.base_url + await link_element.get_attribute('href')
                    print(f"General link found: {full_link}")
                    return full_link
                else:
                    print("General link not found")
                    return None
            except Exception as e:
                print(f"Error getting general link: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return None

    async def get_delivery_fees(self, page):
        print("Attempting to get delivery fees")
        retries = 3
        while retries > 0:
            try:
                delivery_fees_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]')
                delivery_fees = await delivery_fees_element.inner_text() if delivery_fees_element else "N/A"
                print(f"Delivery fees: {delivery_fees}")
                return delivery_fees
            except Exception as e:
                print(f"Error getting delivery fees: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return "N/A"

    async def get_minimum_order(self, page):
        print("Attempting to get minimum order")
        retries = 3
        while retries > 0:
            try:
                minimum_order_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]')
                minimum_order = await minimum_order_element.inner_text() if minimum_order_element else "N/A"
                print(f"Minimum order: {minimum_order}")
                return minimum_order
            except Exception as e:
                print(f"Error getting minimum order: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return "N/A"

    async def extract_category_names(self, page):
        print("Attempting to extract category names")
        retries = 3
        while retries > 0:
            try:
                category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
                category_names = [await element.inner_text() for element in category_name_elements]
                print(f"Category names extracted: {category_names}")
                return category_names
            except Exception as e:
                print(f"Error extracting category names: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_category_links(self, page):
        print("Attempting to extract category links")
        retries = 3
        while retries > 0:
            try:
                category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
                category_links = [self.base_url + await element.get_attribute('href') for element in category_link_elements]
                print(f"Category links extracted: {category_links}")
                return category_links
            except Exception as e:
                print(f"Error extracting category links: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_item_details_new_tab(self, item_link, browser_type):
        print(f"Attempting to extract item details in a new tab for link: {item_link} using {browser_type}")
        retries = 3
        while retries > 0:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    page = await browser.new_page()
                    await page.goto(item_link, timeout=240000)
                    await page.wait_for_load_state("networkidle", timeout=240000)
                    item_price_element = await page.query_selector('//div[@class="price"]//span[@class="currency "]')
                    item_price = await item_price_element.inner_text() if item_price_element else "N/A"
                    print(f"Item price: {item_price}")
                    item_description_element = await page.query_selector('//div[@class="description"]//p[@data-testid="item-description"]')
                    item_description = await item_description_element.inner_text() if item_description_element else "N/A"
                    print(f"Item description: {item_description}")
                    delivery_time_element = await page.query_selector('//div[@data-testid="delivery-tag"]//span')
                    delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"
                    print(f"Delivery time range: {delivery_time}")
                    item_image_elements = await page.query_selector_all('//div[@data-testid="item-image"]//img')
                    item_images = [await img.get_attribute('src') for img in item_image_elements]
                    print(f"Item images: {item_images}")
                    await browser.close()
                    return {
                        "item_price": item_price,
                        "item_description": item_description,
                        "item_delivery_time_range": delivery_time,
                        "item_images": item_images
                    }
            except Exception as e:
                print(f"Error extracting item details for {item_link} in new tab using {browser_type}: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return {
            "item_price": "N/A",
            "item_description": "N/A",
            "item_delivery_time_range": "N/A",
            "item_images": []
        }

    async def extract_item_details(self, item_link):
        print(f"Attempting to extract item details for link: {item_link}")
        default_values = {
            "item_price": "N/A",
            "item_description": "N/A",
            "item_delivery_time_range": "N/A",
            "item_images": []
        }
        for browser_type in ["chromium", "firefox"]:
            try:
                result = await self.extract_item_details_new_tab(item_link, browser_type)
                if result != default_values:
                    return result
            except Exception as e:
                print(f"Error extracting item details for {item_link} using {browser_type}: {e}")
                continue
        return default_values

    async def extract_all_items_from_sub_category(self, sub_category_link, grocery_title, category_name, sub_category_name):
        logging.info(f"Attempting to extract all items from sub-category: {sub_category_link}")
        default_values = []
        for browser_type in ["chromium", "firefox"]:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    sub_page = await browser.new_page()
                    await sub_page.goto(sub_category_link, timeout=240000)
                    await sub_page.wait_for_load_state("networkidle", timeout=240000)
                    await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)
                    pagination_element = await sub_page.query_selector('//div[@class="sc-104fa483-0 fCcIDQ"]//ul[@class="paginate-wrap"]')
                    total_pages = 1
                    if pagination_element:
                        page_numbers = await pagination_element.query_selector_all('//li[contains(@class, "paginate-li f-16 f-500")]//a')
                        total_pages = len(page_numbers) if page_numbers else 1
                    logging.info(f"Found {total_pages} pages in this sub-category")
                    self.main_scraper.current_progress["current_progress"]["total_pages"] = total_pages
                    self.main_scraper.scraped_progress["current_progress"]["total_pages"] = total_pages
                    items = []
                    for page_number in range(1, total_pages + 1):
                        if page_number < self.main_scraper.current_progress["current_progress"]["current_page"]:
                            logging.info(f"Skipping page {page_number} (already processed)")
                            continue
                        elif page_number == self.main_scraper.current_progress["current_progress"]["current_page"]:
                            logging.info(f"Resuming page {page_number}")
                        else:
                            logging.info(f"Processing page {page_number} of {total_pages}")
                            self.main_scraper.current_progress["current_progress"]["current_page"] = page_number
                            self.main_scraper.scraped_progress["current_progress"]["current_page"] = page_number
                        page_url = f"{sub_category_link}&page={page_number}"
                        await sub_page.goto(page_url, timeout=240000)
                        await sub_page.wait_for_load_state("networkidle", timeout=240000)
                        await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)
                        item_elements = await sub_page.query_selector_all('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]//a[@data-testid="grocery-item-link-nofollow"]')
                        logging.info(f"Found {len(item_elements)} items on page {page_number}")
                        self.main_scraper.current_progress["current_progress"]["total_items"] = len(item_elements)
                        self.main_scraper.scraped_progress["current_progress"]["total_items"] = len(item_elements)
                        for i, element in enumerate(item_elements):
                            item_num = i + 1
                            if page_number == self.main_scraper.current_progress["current_progress"]["current_page"] and item_num <= self.main_scraper.current_progress["current_progress"]["current_item"]:
                                logging.info(f"Skipping item {item_num}/{len(item_elements)} (already processed)")
                                continue
                            try:
                                item_name_element = await element.query_selector('div[data-test="item-name"]')
                                item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {item_num}"
                                logging.info(f"Item name: {item_name}")
                                item_link = self.base_url + await element.get_attribute('href')
                                logging.info(f"Item link: {item_link}")
                                if item_name in self.main_scraper.current_progress["current_progress"]["processed_items"]:
                                    logging.info(f"Skipping item {item_name} - already processed")
                                    continue
                                self.main_scraper.current_progress["current_progress"]["current_item"] = item_num
                                self.main_scraper.scraped_progress["current_progress"]["current_item"] = item_num
                                item_details = await self.extract_item_details(item_link)
                                items.append({
                                    "item_name": item_name,
                                    "item_link": item_link,
                                    **item_details
                                })
                                current_cat_idx = self.main_scraper.current_progress["current_progress"]["current_category"] - 1
                                current_subcat_idx = self.main_scraper.current_progress["current_progress"]["current_sub_category"] - 1
                                self.main_scraper.scraped_progress["all_results"][self.main_scraper.current_progress["current_area"]][grocery_title]["grocery_details"]["categories"][current_cat_idx]["sub_categories"][current_subcat_idx]["Items"] = items
                                self.main_scraper.current_progress["current_progress"]["processed_items"].append(item_name)
                                self.main_scraper.scraped_progress["current_progress"]["processed_items"].append(item_name)
                                self.main_scraper.save_current_progress()
                                self.main_scraper.save_scraped_progress()
                                self.main_scraper.commit_progress(f"Processed item {item_name} in sub-category {sub_category_name}, category {category_name}, grocery {grocery_title}")
                                logging.info(f"Saved progress after processing item {item_name}")
                            except Exception as e:
                                logging.error(f"Error processing item {item_num}: {e}")
                                self.main_scraper.current_progress["current_progress"]["processed_items"].append(item_name)
                                self.main_scraper.scraped_progress["current_progress"]["processed_items"].append(item_name)
                                self.main_scraper.save_current_progress()
                                self.main_scraper.save_scraped_progress()
                                self.main_scraper.commit_progress(f"Error processing item {item_name} in sub-category {sub_category_name}, category {category_name}, grocery {grocery_title}")
                        self.main_scraper.current_progress["current_progress"]["completed_pages"].append(page_number)
                        self.main_scraper.scraped_progress["current_progress"]["completed_pages"].append(page_number)
                        self.main_scraper.current_progress["current_progress"]["current_item"] = 0
                        self.main_scraper.scraped_progress["current_progress"]["current_item"] = 0
                        self.main_scraper.save_current_progress()
                        self.main_scraper.save_scraped_progress()
                        self.main_scraper.commit_progress(f"Completed page {page_number} in sub-category {sub_category_name}, category {category_name}, grocery {grocery_title}")
                    await browser.close()
                    if items:
                        return items
            except Exception as e:  # This might be line 328 or nearby
                logging.error(f"Error extracting items from sub-category {sub_category_link} using {browser_type}: {e}")
                continue
        return default_values

    
    async def extract_sub_categories(self, page, category_xpath, grocery_title, category_name):
        print(f"Attempting to extract sub-categories using XPath: {category_xpath}")
        retries = 3
        while retries > 0:
            try:
                sub_category_elements = await page.query_selector_all(f'{category_xpath}//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
                sub_categories = []
                self.main_scraper.current_progress["current_progress"]["total_sub_categories"] = len(sub_category_elements)
                self.main_scraper.scraped_progress["current_progress"]["total_sub_categories"] = len(sub_category_elements)
                for i, element in enumerate(sub_category_elements):
                    sub_cat_num = i + 1
                    if sub_cat_num <= self.main_scraper.current_progress["current_progress"]["current_sub_category"]:
                        print(f"    Skipping sub-category {sub_cat_num}/{len(sub_category_elements)} (already processed)")
                        continue
                    try:
                        sub_category_name = await element.inner_text()
                        sub_category_link = self.base_url + await element.get_attribute('href')
                        print(f"    Processing sub-category {sub_cat_num}/{len(sub_category_elements)}: {sub_category_name}")
                        print(f"    Sub-category link: {sub_category_link}")
                        if sub_category_name in self.main_scraper.current_progress["current_progress"]["processed_sub_categories"]:
                            print(f"    Skipping sub-category {sub_category_name} - already processed")
                            continue
                        self.main_scraper.current_progress["current_progress"]["current_sub_category"] = sub_cat_num
                        self.main_scraper.scraped_progress["current_progress"]["current_sub_category"] = sub_cat_num
                        self.main_scraper.current_progress["current_progress"]["current_page"] = 0  # Reset for new sub-category
                        self.main_scraper.scraped_progress["current_progress"]["current_page"] = 0
                        self.main_scraper.current_progress["current_progress"]["current_item"] = 0
                        self.main_scraper.scraped_progress["current_progress"]["current_item"] = 0
                        self.main_scraper.current_progress["current_progress"]["completed_pages"] = []
                        self.main_scraper.scraped_progress["current_progress"]["completed_pages"] = []
                        items = await self.extract_all_items_from_sub_category(sub_category_link, grocery_title, category_name, sub_category_name)
                        sub_categories.append({
                            "sub_category_name": sub_category_name,
                            "sub_category_link": sub_category_link,
                            "Items": items
                        })
                        current_cat_idx = self.main_scraper.current_progress["current_progress"]["current_category"] - 1
                        self.main_scraper.scraped_progress["all_results"][self.main_scraper.current_progress["current_area"]][grocery_title]["grocery_details"]["categories"][current_cat_idx]["sub_categories"] = sub_categories
                        self.main_scraper.current_progress["current_progress"]["processed_sub_categories"].append(sub_category_name)
                        self.main_scraper.scraped_progress["current_progress"]["processed_sub_categories"].append(sub_category_name)
                        self.main_scraper.save_current_progress()
                        self.main_scraper.save_scraped_progress()
                        self.main_scraper.commit_progress(f"Processed sub-category {sub_category_name} in category {category_name}, grocery {grocery_title}")
                        print(f"    Saved progress after processing sub-category {sub_category_name}")
                    except Exception as e:
                        print(f"Error processing sub-category {sub_cat_num}: {e}")
                        self.main_scraper.current_progress["current_progress"]["processed_sub_categories"].append(sub_category_name)
                        self.main_scraper.scraped_progress["current_progress"]["processed_sub_categories"].append(sub_category_name)
                        self.main_scraper.save_current_progress()
                        self.main_scraper.save_scraped_progress()
                        self.main_scraper.commit_progress(f"Error processing sub-category {sub_category_name} in category {category_name}, grocery {grocery_title}")
                return sub_categories
            except Exception as e:
                print(f"Error extracting sub-categories: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_categories(self, page, grocery_title):
        print(f"Processing grocery: {self.url}")
        retries = 3
        while retries > 0:
            try:
                await page.goto(self.url, timeout=240000)
                await page.wait_for_load_state("networkidle", timeout=240000)
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
                        await category_page.goto(view_all_link, timeout=240000)
                        await category_page.wait_for_load_state("networkidle", timeout=240000)
                        category_names = await self.extract_category_names(category_page)
                        category_links = await self.extract_category_links(category_page)
                        print(f"  Found {len(category_names)} categories")
                        self.main_scraper.current_progress["current_progress"]["total_categories"] = len(category_names)
                        self.main_scraper.scraped_progress["current_progress"]["total_categories"] = len(category_names)
                        categories_data = []
                        for index, (name, link) in enumerate(zip(category_names, category_links)):
                            cat_num = index + 1
                            if cat_num <= self.main_scraper.current_progress["current_progress"]["current_category"]:
                                print(f"  Skipping category {cat_num}/{len(category_names)}: {name} (already processed)")
                                continue
                            print(f"  Processing category {cat_num}/{len(category_names)}: {name}")
                            print(f"  Category link: {link}")
                            if name in self.main_scraper.current_progress["current_progress"]["processed_categories"]:
                                print(f"  Skipping category {name} - already processed")
                                continue
                            self.main_scraper.current_progress["current_progress"]["current_category"] = cat_num
                            self.main_scraper.scraped_progress["current_progress"]["current_category"] = cat_num
                            self.main_scraper.current_progress["current_progress"]["current_sub_category"] = 0  # Reset for new category
                            self.main_scraper.scraped_progress["current_progress"]["current_sub_category"] = 0
                            self.main_scraper.current_progress["current_progress"]["current_page"] = 0
                            self.main_scraper.scraped_progress["current_progress"]["current_page"] = 0
                            self.main_scraper.current_progress["current_progress"]["current_item"] = 0
                            self.main_scraper.scraped_progress["current_progress"]["current_item"] = 0
                            self.main_scraper.current_progress["current_progress"]["processed_sub_categories"] = []
                            self.main_scraper.scraped_progress["current_progress"]["processed_sub_categories"] = []
                            self.main_scraper.current_progress["current_progress"]["completed_pages"] = []
                            self.main_scraper.scraped_progress["current_progress"]["completed_pages"] = []
                            category_xpath = f'//div[@data-testid="category-item-component"][{index + 1}]'
                            async with async_playwright() as p:
                                browser = await p.chromium.launch(headless=True)
                                sub_category_page = await browser.new_page()
                                await sub_category_page.goto(link, timeout=240000)
                                await sub_category_page.wait_for_load_state("networkidle", timeout=240000)
                                sub_categories = await self.extract_sub_categories(sub_category_page, category_xpath, grocery_title, name)
                                await browser.close()
                            print(f"  Found {len(sub_categories)} sub-categories in {name}")
                            category_data = {
                                "name": name,
                                "link": link,
                                "sub_categories": sub_categories
                            }
                            categories_data.append(category_data)
                            self.main_scraper.scraped_progress["all_results"][self.main_scraper.current_progress["current_area"]][grocery_title]["grocery_details"]["categories"] = categories_data
                            self.main_scraper.current_progress["current_progress"]["processed_categories"].append(name)
                            self.main_scraper.scraped_progress["current_progress"]["processed_categories"].append(name)
                            self.main_scraper.save_current_progress()
                            self.main_scraper.save_scraped_progress()
                            self.main_scraper.commit_progress(f"Processed category {name} in grocery {grocery_title}")
                        await browser.close()
                grocery_data = {
                    "delivery_fees": delivery_fees,
                    "minimum_order": minimum_order,
                    "categories": categories_data
                }
                return grocery_data
            except Exception as e:
                print(f"Error extracting categories: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return {"error": "Failed to extract categories after multiple attempts"}

class MainScraper:
    CURRENT_PROGRESS_FILE = "current_progress.json"
    SCRAPED_PROGRESS_FILE = "scraped_progress.json"

    def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
        self.target_url = target_url
        self.base_url = "https://www.talabat.com"
        self.json_file = "talabat_groceries.json"
        self.excel_file = "groceries.xlsx"
        self.groceries_data = {}
        self.drive_uploader = SavingOnDrive('credentials.json')
        self.current_progress = self.load_current_progress()
        self.scraped_progress = self.load_scraped_progress()
        self.github_token = os.environ.get('GITHUB_TOKEN')
        print(f"Initialized MainScraper with target URL: {self.target_url}")

    def load_current_progress(self):
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
            "current_area": None,
            "current_area_index": 0,
            "last_updated": None,
            "current_progress": {
                "grocery_title": None,
                "current_grocery_index": 0,
                "total_categories": 0,
                "current_category": 0,
                "processed_categories": [],
                "total_sub_categories": 0,
                "current_sub_category": 0,
                "processed_sub_categories": [],
                "total_pages": 0,
                "current_page": 0,
                "completed_pages": [],
                "total_items": 0,
                "current_item": 0,
                "processed_items": [],
                "processed_groceries": []
            },
            "areas": {}
        }
        self.save_current_progress(default_progress)
        return default_progress

    def save_current_progress(self, progress=None):
        if progress is None:
            progress = self.current_progress
        try:
            progress["last_updated"] = datetime.datetime.now().isoformat()
            with tempfile.NamedTemporaryFile('w', delete=False, dir='.') as temp_file:
                json.dump(progress, temp_file, indent=2, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_filename = temp_file.name
            os.replace(temp_filename, self.CURRENT_PROGRESS_FILE)
            print(f"Saved current progress to {self.CURRENT_PROGRESS_FILE}")
        except Exception as e:
            print(f"Error saving current progress: {e}")

    def load_scraped_progress(self):
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
            "current_area": None,
            "current_area_index": 0,
            "last_updated": None,
            "current_progress": {
                "grocery_title": None,
                "current_grocery_index": 0,
                "total_categories": 0,
                "current_category": 0,
                "processed_categories": [],
                "total_sub_categories": 0,
                "current_sub_category": 0,
                "processed_sub_categories": [],
                "total_pages": 0,
                "current_page": 0,
                "completed_pages": [],
                "total_items": 0,
                "current_item": 0,
                "processed_items": [],
                "processed_groceries": []
            },
            "areas": {},
            "all_results": {}
        }
        self.save_scraped_progress(default_progress)
        return default_progress

    def save_scraped_progress(self, progress=None):
        if progress is None:
            progress = self.scraped_progress
        try:
            progress["last_updated"] = datetime.datetime.now().isoformat()
            with tempfile.NamedTemporaryFile('w', delete=False, dir='.') as temp_file:
                json.dump(progress, temp_file, indent=2, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_filename = temp_file.name
            os.replace(temp_filename, self.SCRAPED_PROGRESS_FILE)
            print(f"Saved scraped progress to {self.SCRAPED_PROGRESS_FILE}")
        except Exception as e:
            print(f"Error saving scraped progress: {e}")

    def commit_progress(self, message="Periodic progress update"):
        if not self.github_token:
            print("No GITHUB_TOKEN available, skipping commit")
            return
        try:
            subprocess.run(["git", "config", "--global", "user.name", "GitHub Action"], check=True)
            subprocess.run(["git", "config", "--global", "user.email", "action@github.com"], check=True)
            subprocess.run(["git", "add", self.CURRENT_PROGRESS_FILE, self.SCRAPED_PROGRESS_FILE, self.json_file, self.excel_file], check=True)
            result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
            if result.returncode != 0 and "nothing to commit" not in result.stdout:
                print(f"Commit failed: {result.stderr}")
                return
            pull_result = subprocess.run(["git", "pull", "--rebase"], capture_output=True, text=True, env={"GIT_AUTH_TOKEN": self.github_token})
            if pull_result.returncode != 0:
                print(f"Git pull --rebase failed: {pull_result.stderr}")
                force_push_result = subprocess.run(["git", "push", "--force"], capture_output=True, text=True, env={"GIT_AUTH_TOKEN": self.github_token})
                if force_push_result.returncode == 0:
                    print(f"Force pushed progress: {message}")
                else:
                    print(f"Force push failed: {force_push_result.stderr}")
                return
            push_result = subprocess.run(["git", "push"], capture_output=True, text=True, env={"GIT_AUTH_TOKEN": self.github_token})
            if push_result.returncode == 0:
                print(f"Committed progress: {message}")
            else:
                print(f"Push failed: {push_result.stderr}")
                force_push_result = subprocess.run(["git", "push", "--force"], capture_output=True, text=True, env={"GIT_AUTH_TOKEN": self.github_token})
                if force_push_result.returncode == 0:
                    print(f"Force pushed progress after retry: {message}")
                else:
                    print(f"Retry force push failed: {force_push_result.stderr}")
        except subprocess.CalledProcessError as e:
            print(f"Error committing progress: {e}")

    def print_progress_details(self):
        try:
            with open(self.CURRENT_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                current = json.load(f)
            print("\nCurrent Progress:")
            print(json.dumps(current, indent=2, ensure_ascii=False))
            with open(self.SCRAPED_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                scraped = json.load(f)
            print("\nScraped Progress with Results:")
            print(json.dumps(scraped, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"Error printing progress: {str(e)}")

    async def extract_grocery_info(self, page):
        print("Attempting to extract grocery information")
        retries = 3
        while retries > 0:
            try:
                vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
                print(f"Found {len(vendor_containers)} grocery vendors on page")
                groceries_info = []
                for i, container in enumerate(vendor_containers):
                    try:
                        title_element = await container.query_selector('a div h2')
                        grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i+1}"
                        print(f"  Grocery title: {grocery_title}")
                        link_element = await container.query_selector('a')
                        grocery_link = self.base_url + await link_element.get_attribute('href') if link_element else None
                        print(f"  Grocery link: {grocery_link}")
                        delivery_info = await container.query_selector('div.deliveryInfo')
                        delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"
                        print(f"  Delivery time text: {delivery_time_text}")
                        if delivery_time_text != "N/A":
                            digits = re.findall(r'\d+', delivery_time_text)
                            delivery_time = f"{digits[0]} mins" if digits else "N/A"
                        else:
                            delivery_time = "N/A"
                        print(f"  Delivery time: {delivery_time}")
                        if grocery_title and grocery_link and delivery_time:
                            print(f"Found grocery: {grocery_title}, Delivery time: {delivery_time}")
                            groceries_info.append({
                                'grocery_title': grocery_title,
                                'grocery_link': grocery_link,
                                'delivery_time': delivery_time
                            })
                    except Exception as e:
                        print(f"Error processing grocery {i+1}: {e}")
                return groceries_info
            except Exception as e:
                print(f"Error extracting grocery information: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    def save_to_json(self):
        print("Saving data to JSON file")
        try:
            with open(self.json_file, 'w') as f:
                json.dump(self.groceries_data, f, indent=4)
            print(f"Data saved to {self.json_file}")
            self.commit_progress(f"Saved groceries data to {self.json_file}")
        except Exception as e:
            print(f"Error saving to JSON file: {e}")

    def save_to_excel(self):
        print("Saving data to Excel file")
        try:
            writer = pd.ExcelWriter(self.excel_file, engine='xlsxwriter')
            for area, area_data in self.scraped_progress["all_results"].items():
                for grocery_title, grocery_data in area_data.items():
                    flattened_data = []
                    general_info = {
                        'area': area,
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
                                item_data = {
                                    **general_info,
                                    'category': category_name,
                                    'sub_category': sub_category_name,
                                    'item_name': item.get('item_name', 'N/A'),
                                    'item_price': item.get('item_price', 'N/A'),
                                    'item_description': item.get('item_description', 'N/A'),
                                    'item_link': item.get('item_link', 'N/A')
                                }
                                flattened_data.append(item_data)
                    if not flattened_data:
                        flattened_data.append(general_info)
                    safe_title = re.sub(r'[\\/*?:\[\]]', '_', f"{area}_{grocery_title}")[:31]
                    df = pd.DataFrame(flattened_data)
                    df.to_excel(writer, sheet_name=safe_title, index=False)
            writer.close()
            print(f"Data saved to {self.excel_file}")
            self.commit_progress(f"Saved data to Excel file {self.excel_file}")
            self.upload_to_drive()
        except Exception as e:
            print(f"Error saving to Excel: {e}")

    def upload_to_drive(self):
        print(f"\nUploading {self.excel_file} to Google Drive...")
        try:
            if not self.drive_uploader.authenticate():
                print("Failed to authenticate with Google Drive")
                return False
            file_ids = self.drive_uploader.upload_to_multiple_folders(self.excel_file)
            if len(file_ids) == len(self.drive_uploader.target_folders):
                print(f"Uploaded {self.excel_file} to Google Drive successfully")
                self.commit_progress(f"Uploaded {self.excel_file} to Google Drive")
                return True
            else:
                print(f"Failed to upload {self.excel_file} to all target folders")
                return False
        except Exception as e:
            print(f"Error uploading to Google Drive: {str(e)}")
            return False

    async def process_grocery(self, grocery_info, area_name, grocery_index, total_groceries):
        grocery_title = grocery_info['grocery_title']
        grocery_link = grocery_info['grocery_link']
        delivery_time = grocery_info['delivery_time']
        print(f"Processing grocery {grocery_index + 1}/{total_groceries}: {grocery_title}")
        if grocery_index < self.current_progress["current_progress"]["current_grocery_index"]:
            print(f"Skipping {grocery_title} - already processed")
            return
        if grocery_title in self.current_progress["current_progress"]["processed_groceries"]:
            print(f"Skipping {grocery_title} - already processed")
            return
        talabat_grocery = TalabatGroceries(grocery_link, self)
        self.current_progress["current_progress"]["grocery_title"] = grocery_title
        self.scraped_progress["current_progress"]["grocery_title"] = grocery_title
        self.current_progress["current_progress"]["current_grocery_index"] = grocery_index
        self.scraped_progress["current_progress"]["current_grocery_index"] = grocery_index
        self.current_progress["current_progress"]["current_category"] = 0  # Reset for new grocery
        self.scraped_progress["current_progress"]["current_category"] = 0
        self.current_progress["current_progress"]["current_sub_category"] = 0
        self.scraped_progress["current_progress"]["current_sub_category"] = 0
        self.current_progress["current_progress"]["current_page"] = 0
        self.scraped_progress["current_progress"]["current_page"] = 0
        self.current_progress["current_progress"]["current_item"] = 0
        self.scraped_progress["current_progress"]["current_item"] = 0
        self.current_progress["current_progress"]["processed_categories"] = []
        self.scraped_progress["current_progress"]["processed_categories"] = []
        self.current_progress["current_progress"]["processed_sub_categories"] = []
        self.scraped_progress["current_progress"]["processed_sub_categories"] = []
        self.current_progress["current_progress"]["completed_pages"] = []
        self.scraped_progress["current_progress"]["completed_pages"] = []
        self.save_current_progress()
        self.save_scraped_progress()
        self.commit_progress(f"Started processing grocery {grocery_title} in area {area_name}")
        for browser_type in ["chromium", "firefox"]:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    page = await browser.new_page()
                    grocery_details = await talabat_grocery.extract_categories(page, grocery_title)
                    if area_name not in self.scraped_progress["all_results"]:
                        self.scraped_progress["all_results"][area_name] = {}
                    self.scraped_progress["all_results"][area_name][grocery_title] = {
                        'grocery_link': grocery_link,
                        'delivery_time': delivery_time,
                        'grocery_details': grocery_details
                    }
                    self.current_progress["current_progress"]["processed_groceries"].append(grocery_title)
                    self.scraped_progress["current_progress"]["processed_groceries"].append(grocery_title)
                    if area_name not in self.current_progress["areas"]:
                        self.current_progress["areas"][area_name] = []
                    if grocery_title not in self.current_progress["areas"][area_name]:
                        self.current_progress["areas"][area_name].append(grocery_title)
                    self.save_current_progress()
                    self.save_scraped_progress()
                    self.commit_progress(f"Completed processing grocery {grocery_title} in area {area_name}")
                    self.groceries_data[grocery_title] = self.scraped_progress["all_results"][area_name][grocery_title]
                    self.save_to_json()
                    print(f"Successfully processed and saved data for {grocery_title}")
                    break
            except Exception as e:
                print(f"Error processing {grocery_title} with {browser_type}: {e}")
                self.current_progress["current_progress"]["processed_groceries"].append(grocery_title)
                self.scraped_progress["current_progress"]["processed_groceries"].append(grocery_title)
                if area_name not in self.current_progress["areas"]:
                    self.current_progress["areas"][area_name] = []
                if grocery_title not in self.current_progress["areas"][area_name]:
                    self.current_progress["areas"][area_name].append(grocery_title)
                self.save_current_progress()
                self.save_scraped_progress()
                self.commit_progress(f"Error processing grocery {grocery_title} in area {area_name}")
                continue
            finally:
                await browser.close()

    async def run(self):
        print("Starting the scraper")
        areas = [
            ("Dhaher", "https://www.talabat.com/kuwait/groceries/59/dhaher"),
            ("Riqqa", "https://www.talabat.com/kuwait/groceries/37/riqqa"),
            ("Hadiya", "https://www.talabat.com/kuwait/groceries/30/hadiya"),
            ("Mangaf", "https://www.talabat.com/kuwait/groceries/32/mangaf"),
            ("Abu Halifa", "https://www.talabat.com/kuwait/groceries/2/abu-halifa"),
            ("Fintas", "https://www.talabat.com/kuwait/groceries/38/fintas"),
            ("Egaila", "https://www.talabat.com/kuwait/groceries/79/egaila"),
            ("Sabahiya", "https://www.talabat.com/kuwait/groceries/31/sabahiya"),
            ("Al Ahmadi", "https://www.talabat.com/kuwait/groceries/3/al-ahmadi"),
            ("Fahaheel", "https://www.talabat.com/kuwait/groceries/5/fahaheel"),
        ]
        completed_areas = self.current_progress["completed_areas"]
        current_area_index = self.current_progress["current_area_index"]
        print(f"Starting from area index {current_area_index}")
        print(f"Already completed areas: {', '.join(completed_areas) if completed_areas else 'None'}")
        resuming_area = self.current_progress["current_area"]
        if resuming_area:
            for idx, (area_name, _) in enumerate(areas):
                if area_name == resuming_area:
                    print(f"Resuming from area {resuming_area} (index {idx})")
                    current_area_index = idx
                    self.current_progress["current_area_index"] = idx
                    self.scraped_progress["current_area_index"] = idx
                    self.save_current_progress()
                    self.save_scraped_progress()
                    self.commit_progress(f"Resuming from area {resuming_area}")
                    break
        for idx, (area_name, area_url) in enumerate(areas):
            if area_name in completed_areas and area_name != resuming_area:
                print(f"Skipping completed area: {area_name}")
                continue
            if idx < current_area_index:
                print(f"Skipping area {area_name} (index {idx} < {current_area_index})")
                continue
            self.current_progress["current_area"] = area_name
            self.scraped_progress["current_area"] = area_name
            self.current_progress["current_area_index"] = idx
            self.scraped_progress["current_area_index"] = idx
            self.save_current_progress()
            self.save_scraped_progress()
            self.commit_progress(f"Started processing area {area_name}")
            retries = 3
            while retries > 0:
                try:
                    print(f"Target URL: {area_url}")
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True)
                        page = await browser.new_page()
                        page.set_default_timeout(240000)
                        await page.goto(area_url, timeout=240000)
                        await page.wait_for_load_state("networkidle", timeout=240000)
                        print("Page loaded successfully")
                        try:
                            await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=240000)
                            print("Grocery vendor elements found")
                        except Exception as e:
                            print(f"Error waiting for vendor elements: {e}")
                            print("Attempting to continue anyway...")
                        groceries_info = await self.extract_grocery_info(page)
                        await browser.close()
                        print(f"Found {len(groceries_info)} groceries to process in {area_name}")
                        for grocery_idx, grocery_info in enumerate(groceries_info):
                            await self.process_grocery(grocery_info, area_name, grocery_idx, len(groceries_info))
                        self.current_progress["completed_areas"].append(area_name)
                        self.scraped_progress["completed_areas"].append(area_name)
                        self.current_progress["current_area"] = None
                        self.scraped_progress["current_area"] = None
                        self.current_progress["current_progress"] = {
                            "grocery_title": None,
                            "current_grocery_index": 0,
                            "total_categories": 0,
                            "current_category": 0,
                            "processed_categories": [],
                            "total_sub_categories": 0,
                            "current_sub_category": 0,
                            "processed_sub_categories": [],
                            "total_pages": 0,
                            "current_page": 0,
                            "completed_pages": [],
                            "total_items": 0,
                            "current_item": 0,
                            "processed_items": [],
                            "processed_groceries": []
                        }
                        self.scraped_progress["current_progress"] = self.current_progress["current_progress"].copy()
                        self.save_current_progress()
                        self.save_scraped_progress()
                        self.commit_progress(f"Completed area {area_name}")
                        self.save_to_excel()
                        print(f"Scraping completed successfully for area {area_name}")
                    break
                except Exception as e:
                    print(f"Error in main scraper for {area_name}: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(5)
                    if retries == 0:
                        print(f"Failed to complete the scraping process for {area_name} after multiple attempts.")
                        self.save_current_progress()
                        self.save_scraped_progress()
                        self.commit_progress(f"Failed processing area {area_name} after retries")

async def main():
    if not os.path.exists('credentials.json'):
        credentials_json = os.environ.get('TALABAT_GCLOUD_KEY_JSON')
        if credentials_json:
            with open('credentials.json', 'w') as f:
                f.write(credentials_json)
            print("Created credentials.json from environment variable")
        else:
            print("ERROR: credentials.json not found and TALABAT_GCLOUD_KEY_JSON not set!")
            return
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

