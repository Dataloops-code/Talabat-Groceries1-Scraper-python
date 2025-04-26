import asyncio
import json
import logging
import random
import re
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError
import os
import aiofiles

class MainScraper:
    def __init__(self):
        # Default structure for current_progress
        self.current_progress = {"current_progress": {"completed_areas": [], "completed_groceries": {}}}
        self.scraped_data = {}
        self.load_progress()

    def load_progress(self):
        # Default structure to merge with loaded data
        default_progress = {"current_progress": {"completed_areas": [], "completed_groceries": {}}}
        try:
            if os.path.exists("current_progress.json"):
                with open("current_progress.json", "r", encoding="utf-8") as f:
                    loaded_progress = json.load(f)
                    # Merge loaded progress with default structure
                    self.current_progress = default_progress
                    self.current_progress["current_progress"].update(
                        loaded_progress.get("current_progress", {})
                    )
                    # Ensure completed_areas and completed_groceries exist
                    self.current_progress["current_progress"].setdefault("completed_areas", [])
                    self.current_progress["current_progress"].setdefault("completed_groceries", {})
            if os.path.exists("scraped_data.json"):
                with open("scraped_data.json", "r", encoding="utf-8") as f:
                    self.scraped_data = json.load(f)
        except Exception as e:
            print(f"Error loading progress: {e}")
            logging.error(f"Error loading progress: {e}")
            # Fallback to default structure on error
            self.current_progress = default_progress

    def save_current_progress(self):
        with open("current_progress.json", "w", encoding="utf-8") as f:
            json.dump(self.current_progress, f, indent=2, ensure_ascii=False)

    def save_scraped_progress(self):
        with open("scraped_data.json", "w", encoding="utf-8") as f:
            json.dump(self.scraped_data, f, indent=2, ensure_ascii=False)

class TalabatGroceries:
    def __init__(self, url, browser, main_scraper):
        self.url = url
        self.base_url = "https://www.talabat.com"
        self.browser = browser
        self.main_scraper = main_scraper
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0"
        ]
        print(f"Initialized TalabatGroceries with URL: {self.url}")

    async def extract_category_names(self, page):
        print("Attempting to extract category names")
        retries = 3
        selectors = [
            'div[data-testid="category-item-component"] span[data-testid="category-name"]',
            'div[class*="category-item"] span[class*="category-name"]',
            '//div[contains(@class, "category")]//span[contains(@class, "name")]',
            '//div[@role="button"]//span[contains(@class, "title")]'
        ]
        while retries > 0:
            try:
                for selector in selectors:
                    category_elements = await page.query_selector_all(selector)
                    if category_elements:
                        category_names = [await el.inner_text() for el in category_elements]
                        print(f"Category names extracted with selector {selector}: {category_names}")
                        return category_names
                print("No category names found with any selector")
                html_content = await page.content()
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                with open(f"category_names_failed_{timestamp}.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                return []
            except Exception as e:
                print(f"Error extracting category names: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_category_links(self, page):
        print("Attempting to extract category links")
        retries = 3
        selectors = [
            'div[data-testid="category-item-component"]',
            'div[class*="category-item"]',
            '//div[contains(@class, "category")]',
            '//div[@role="button" and contains(@class, "category")]'
        ]
        while retries > 0:
            try:
                for selector in selectors:
                    category_elements = await page.query_selector_all(selector)
                    if category_elements:
                        category_links = []
                        for el in category_elements:
                            link_element = await el.query_selector('a')
                            if link_element:
                                href = await link_element.get_attribute('href')
                                if href:
                                    full_link = self.base_url + href if href.startswith('/') else href
                                    category_links.append(full_link)
                        print(f"Category links extracted with selector {selector}: {category_links}")
                        return category_links
                print("No category links found with any selector")
                html_content = await page.content()
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                with open(f"category_links_failed_{timestamp}.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                return []
            except Exception as e:
                print(f"Error extracting category links: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_categories(self, page):
        category_names = await self.extract_category_names(page)
        category_links = await self.extract_category_links(page)
        categories = []
        for name, link in zip(category_names, category_links):
            categories.append({"category_name": name, "category_link": link, "sub_categories": []})
        return categories

    async def extract_sub_categories(self, page, category_link, grocery_title, category_info):
        print(f"Attempting to extract sub-categories for: {category_link}")
        retries = 3
        sub_categories = []
        completed_sub_categories = self.main_scraper.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {}).get("completed sub-categories", [])
        current_sub_category = self.main_scraper.current_progress["current_progress"].get("current_sub_category")
        category_name = category_info["category_name"]
        start_processing = not current_sub_category

        while retries > 0:
            try:
                await page.goto(category_link, timeout=240000)
                await page.wait_for_load_state("networkidle", timeout=240000)
                await page.wait_for_timeout(10000)  # Wait for dynamic content
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_load_state("networkidle", timeout=240000)

                category_elements = await page.query_selector_all('div[data-testid="category-item-component"], div[class*="category-item"], //div[contains(@class, "category")]')
                
                target_category = None
                for element in category_elements:
                    name_element = await element.query_selector('span[data-testid="category-name"], span[class*="category-name"], //span[contains(@class, "name")]')
                    if name_element and await name_element.inner_text() == category_name:
                        target_category = element
                        break
                
                if not target_category:
                    print(f"Category {category_name} not found on page")
                    html_content = await page.content()
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    with open(f"category_{category_name}_failed_{timestamp}.html", "w", encoding="utf-8") as f:
                        f.write(html_content)
                    return sub_categories

                sub_category_selectors = [
                    'div[data-testid="subcategory-item-component"]',
                    'div[class*="subcategory-item"]',
                    '//div[contains(@class, "subcategory")]',
                    '//div[@role="button" and contains(@class, "subcategory")]'
                ]
                for selector in sub_category_selectors:
                    sub_category_elements = await page.query_selector_all(selector)
                    if sub_category_elements:
                        for sub_cat in sub_category_elements:
                            name_element = await sub_cat.query_selector('span[data-testid="subcategory-name"], span[class*="subcategory-name"], //span[contains(@class, "name")]')
                            link_element = await sub_cat.query_selector('a')
                            if name_element and link_element:
                                sub_cat_name = await name_element.inner_text()
                                href = await link_element.get_attribute('href')
                                sub_cat_link = self.base_url + href if href.startswith('/') else href
                                if sub_cat_name not in completed_sub_categories or start_processing:
                                    sub_categories.append({
                                        "sub_category_name": sub_cat_name,
                                        "sub_category_link": sub_cat_link,
                                        "items": []
                                    })
                        print(f"Sub-categories extracted with selector {selector}: {[sc['sub_category_name'] for sc in sub_categories]}")
                        return sub_categories
                print("No sub-categories found with any selector")
                html_content = await page.content()
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                with open(f"sub_categories_failed_{timestamp}.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                return sub_categories
            except Exception as e:
                print(f"Error extracting sub-categories: {e}")
                logging.error(f"Error extracting sub-categories: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(10)
        return sub_categories

    async def extract_all_items_from_sub_category(self, sub_category_link):
        print(f"Attempting to extract all items from sub-category: {sub_category_link}")
        retries = 3
        items = []
        sub_page = None
        while retries > 0:
            try:
                sub_page = await self.browser.new_page(
                    user_agent=random.choice(self.user_agents),
                    extra_http_headers={
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
                    }
                )
                await sub_page.goto(sub_category_link, timeout=240000)
                await sub_page.wait_for_load_state("networkidle", timeout=240000)
                await sub_page.wait_for_timeout(10000)
                await sub_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await sub_page.wait_for_load_state("networkidle", timeout=240000)

                await sub_page.wait_for_selector(
                    '//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]',
                    timeout=60000
                )

                item_container_selectors = [
                    '//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]',
                    '//div[contains(@class, "category-items-container")]//div[contains(@class, "col-")]',
                    '//div[@data-testid="grocery-item"]',
                    '//div[contains(@class, "product-item")]',
                    '//div[@role="button" and contains(@class, "item")]'
                ]
                item_elements = None
                for selector in item_container_selectors:
                    try:
                        item_elements = await sub_page.query_selector_all(selector)
                        if item_elements:
                            print(f"Found items using selector: {selector}")
                            break
                    except Exception as e:
                        print(f"Selector '{selector}' failed: {e}")

                if not item_elements:
                    print("No items found with any selector")
                    html_content = await sub_page.content()
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    html_filename = f"sub_category_{sub_category_link.split('/')[-1].replace('?aid=37', '')}_failed_{timestamp}.html"
                    with open(html_filename, "w", encoding="utf-8") as f:
                        f.write(html_content)
                    print(f"Saved sub-category HTML to {html_filename} for debugging")
                    logging.error(f"No items found for {sub_category_link}. Selectors tried: {item_container_selectors}")
                    await sub_page.close()
                    return []

                for item in item_elements:
                    name_element = await item.query_selector('h3, span[class*="title"], //span[contains(@class, "name")]')
                    link_element = await item.query_selector('a')
                    image_element = await item.query_selector('img')
                    item_name = await name_element.inner_text() if name_element else "N/A"
                    item_link = self.base_url + await link_element.get_attribute('href') if link_element and await link_element.get_attribute('href') else None
                    item_image = await image_element.get_attribute('src') if image_element else None
                    if item_name != "N/A" and item_link:
                        items.append({
                            "item_name": item_name,
                            "item_link": item_link,
                            "item_image": item_image,
                            "item_details": {}
                        })
                print(f"Extracted {len(items)} items from {sub_category_link}")
                await sub_page.close()
                return items
            except Exception as e:
                print(f"Error extracting items from sub-category {sub_category_link}: {e}")
                logging.error(f"Error extracting items from sub-category {sub_category_link}: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(10)
                if sub_page:
                    await sub_page.close()
        return []

    async def extract_item_details(self, item_link):
        print(f"Attempting to extract item details for link: {item_link}")
        retries = 3
        page = None
        while retries > 0:
            try:
                page = await self.browser.new_page(
                    user_agent=random.choice(self.user_agents),
                    extra_http_headers={
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
                    }
                )
                await page.goto(item_link, timeout=240000)
                await page.wait_for_load_state("networkidle", timeout=240000)
                await page.wait_for_timeout(10000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_load_state("networkidle", timeout=240000)

                # Price extraction
                price_selectors = [
                    'div[data-testid="product-price"] span[data-testid="price"]',
                    'div[class*="price"] span[class*="currency"]',
                    'div[class*="price"]',
                    '//span[contains(text(), "KWD")]',
                    '//div[contains(@class, "price")]//span',
                    '//span[contains(@class, "amount")]'
                ]
                item_price = "N/A"
                for selector in price_selectors:
                    price_element = await page.query_selector(selector)
                    if price_element:
                        price_text = await price_element.inner_text()
                        match = re.search(r'(\d+\.?\d*)', price_text)
                        item_price = match.group(0) if match else price_text.strip().replace('KWD', '').strip()
                        if item_price:
                            break
                    print(f"Price selector '{selector}' not found")

                # Old price extraction
                old_price_selectors = [
                    'div[data-testid="product-price"] span[data-testid="old-price"]',
                    'div[class*="old-price"] span',
                    '//span[contains(@class, "old-price")]',
                    '//div[contains(@class, "price")]//span[contains(@class, "strike")]'
                ]
                item_old_price = None
                for selector in old_price_selectors:
                    old_price_element = await page.query_selector(selector)
                    if old_price_element:
                        old_price_text = await old_price_element.inner_text()
                        match = re.search(r'(\d+\.?\d*)', old_price_text)
                        item_old_price = match.group(0) if match else old_price_text.strip().replace('KWD', '').strip()
                        break

                # Offer extraction
                offer_selectors = [
                    'div[data-testid="product-offer"]',
                    'div[class*="offer"]',
                    '//div[contains(@class, "offer")]',
                    '//span[contains(@class, "discount Penal Code section 21.3.2(b) requires that the defendant knew or should have known that the victim was a peace officer or firefighter and was engaged in the performance of their duties. The prosecution must prove this knowledge beyond a reasonable doubt.

                # Description extraction
                desc_selectors = [
                    'div[data-testid="item-description"] p',
                    'div[class*="description"] p',
                    '//p[contains(@class, "description")]',
                    '//div[contains(@class, "product-details")]//p',
                    '//div[contains(@class, "description")]'
                ]
                item_description = "N/A"
                for selector in desc_selectors:
                    desc_element = await page.query_selector(selector)
                    if desc_element:
                        item_description = await desc_element.inner_text()
                        if item_description.strip():
                            break
                    print(f"Description selector '{selector}' not found")

                # Delivery time extraction
                delivery_selectors = [
                    'div[data-testid="delivery-time"]',
                    'div[class*="delivery-time"]',
                    '//div[contains(@class, "delivery")]',
                    '//span[contains(text(), "min")]'
                ]
                delivery_time = "N/A"
                for selector in delivery_selectors:
                    delivery_element = await page.query_selector(selector)
                    if delivery_element:
                        delivery_time = await delivery_element.inner_text()
                        break

                # Images extraction
                image_selectors = [
                    'img[data-testid="product-image"]',
                    'img[class*="product-image"]',
                    '//img[contains(@class, "image")]',
                    '//div[contains(@class, "gallery")]//img'
                ]
                item_images = []
                for selector in image_selectors:
                    image_elements = await page.query_selector_all(selector)
                    for img in image_elements:
                        src = await img.get_attribute('src')
                        if src and src not in item_images:
                            item_images.append(src)
                    if item_images:
                        break

                await page.close()
                return {
                    "item_price": item_price,
                    "item_old_price": item_old_price,
                    "item_offer": item_offer,
                    "item_description": item_description,
                    "item_delivery_time_range": delivery_time,
                    "item_images": item_images
                }
            except Exception as e:
                print(f"Error extracting item details for {item_link}: {e}")
                logging.error(f"Error extracting item details for {item_link}: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(10)
                if page:
                    await page.close()
        return {
            "item_price": "N/A",
            "item_old_price": None,
            "item_offer": None,
            "item_description": "N/A",
            "item_delivery_time_range": "N/A",
            "item_images": []
        }

    async def process_category(self, grocery_title, category_info, talabat_grocery, page):
        category_name = category_info["category_name"]
        category_link = category_info["category_link"]
        print(f"Starting extraction of sub-categories for category: {category_name}")
        category_page = await self.browser.new_page(
            user_agent=random.choice(self.user_agents),
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            }
        )
        try:
            await category_page.goto(category_link, timeout=240000)
            await category_page.wait_for_load_state("networkidle", timeout=240000)
            await category_page.wait_for_timeout(10000)
            sub_categories = await talabat_grocery.extract_sub_categories(category_page, category_link, grocery_title, category_info)
            category_info["sub_categories"] = sub_categories
            for sub_cat in sub_categories:
                sub_cat_name = sub_cat["sub_category_name"]
                sub_cat_link = sub_cat["sub_category_link"]
                if sub_cat_name not in self.main_scraper.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {}).get("completed sub-categories", []):
                    print(f"Processing sub-category: {sub_cat_name}")
                    items = await talabat_grocery.extract_all_items_from_sub_category(sub_cat_link)
                    sub_cat["items"] = items
                    for item in items:
                        item_details = await talabat_grocery.extract_item_details(item["item_link"])
                        item["item_details"] = item_details
                    self.main_scraper.current_progress["current_progress"]["current_sub_category"] = sub_cat_name
                    self.main_scraper.current_progress["current_progress"]["completed_groceries"].setdefault(grocery_title, {"completed sub-categories": []})["completed sub-categories"].append(sub_cat_name)
                    self.main_scraper.save_current_progress()
                    self.main_scraper.save_scraped_progress()
                    await asyncio.sleep(5)  # Delay between sub-categories
            self.main_scraper.current_progress["current_progress"]["current_sub_category"] = None
        finally:
            await category_page.close()
        self.main_scraper.save_current_progress()
        self.main_scraper.save_scraped_progress()

    async def process_grocery_categories(self, grocery_title, grocery_details, talabat_grocery, page, groceries_on_page, grocery_idx):
        print(f"Processing grocery: {grocery_title}")
        categories = await talabat_grocery.extract_categories(page)
        grocery_details["categories"] = categories
        self.main_scraper.scraped_data.setdefault(grocery_title, {}).update(grocery_details)
        for idx, category in enumerate(categories):
            await self.process_category(grocery_title, category, talabat_grocery, page)
            await asyncio.sleep(5)  # Delay between categories
        self.main_scraper.current_progress["current_progress"]["completed_groceries"][grocery_title] = {"completed sub-categories": []}
        self.main_scraper.save_current_progress()
        self.main_scraper.save_scraped_progress()

async def main():
    logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        main_scraper = MainScraper()
        page = await browser.new_page(
            user_agent=random.choice(TalabatGroceries("", browser, main_scraper).user_agents),
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            }
        )

        areas = [
            {"area_name": "الظهر", "area_link": "https://www.talabat.com/kuwait/groceries/59/dhaher"},
            {"area_name": "الرقه", "area_link": "https://www.talabat.com/kuwait/groceries/37/riqqa"},
            {"area_name": "هدية", "area_link": "https://www.talabat.com/kuwait/groceries/30/hadiya"}
        ]

        for area in areas:
            area_name = area["area_name"]
            area_link = area["area_link"]
            if area_name in main_scraper.current_progress["current_progress"]["completed_areas"]:
                print(f"Skipping completed area: {area_name}")
                continue

            print(f"Processing area: {area_name}")
            await page.goto(area_link, timeout=240000)
            await page.wait_for_load_state("networkidle", timeout=240000)
            await page.wait_for_timeout(10000)

            grocery_elements = await page.query_selector_all('div[data-testid="grocery-card"] a')
            groceries = []
            for el in grocery_elements:
                href = await el.get_attribute('href')
                title = await (await el.query_selector('h3')).inner_text()
                groceries.append({"title": title, "link": "https://www.talabat.com" + href})

            for idx, grocery in enumerate(groceries):
                grocery_title = grocery["title"]
                grocery_link = grocery["link"]
                if grocery_title in main_scraper.current_progress["current_progress"]["completed_groceries"]:
                    print(f"Skipping completed grocery: {grocery_title}")
                    continue

                print(f"Navigating to grocery: {grocery_title}")
                await page.goto(grocery_link, timeout=240000)
                await page.wait_for_load_state("networkidle", timeout=240000)
                await page.wait_for_timeout(10000)

                talabat_grocery = TalabatGroceries(grocery_link, browser, main_scraper)
                grocery_details = {"grocery_link": grocery_link, "categories": []}
                await talabat_grocery.process_grocery_categories(grocery_title, grocery_details, talabat_grocery, page, groceries, idx)

            main_scraper.current_progress["current_progress"]["completed_areas"].append(area_name)
            main_scraper.save_current_progress()
            main_scraper.save_scraped_progress()

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())







# import asyncio
# import json
# import os
# import tempfile
# import sys
# import subprocess
# import re
# from typing import Dict, List
# import pandas as pd
# from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
# from SavingOnDrive import SavingOnDrive
# import logging
# from datetime import datetime
# from retry import retry

# # Set up logging
# logging.basicConfig(
#     filename='scraper.log',
#     level=logging.DEBUG,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

# class TalabatGroceries:
#     def __init__(self, url, browser, main_scraper):
#         self.url = url
#         self.base_url = "https://www.talabat.com"
#         self.browser = browser
#         self.main_scraper = main_scraper
#         print(f"Initialized TalabatGroceries with URL: {self.url}")

#     async def get_general_link(self, page):
#         print("Attempting to get general link")
#         retries = 3
#         while retries > 0:
#             try:
#                 link_element = await page.wait_for_selector('//a[@data-testid="view-all-link"]', timeout=60000)
#                 if link_element:
#                     full_link = self.base_url + await link_element.get_attribute('href')
#                     print(f"General link found: {full_link}")
#                     return full_link
#                 else:
#                     print("General link not found")
#                     return None
#             except Exception as e:
#                 print(f"Error getting general link: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return None

#     async def get_delivery_fees(self, page):
#         print("Attempting to get delivery fees")
#         retries = 3
#         while retries > 0:
#             try:
#                 delivery_fees_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]')
#                 delivery_fees = await delivery_fees_element.inner_text() if delivery_fees_element else "N/A"
#                 print(f"Delivery fees: {delivery_fees}")
#                 return delivery_fees
#             except Exception as e:
#                 print(f"Error getting delivery fees: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return "N/A"

#     async def get_minimum_order(self, page):
#         print("Attempting to get minimum order")
#         retries = 3
#         while retries > 0:
#             try:
#                 minimum_order_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]')
#                 minimum_order = await minimum_order_element.inner_text() if minimum_order_element else "N/A"
#                 print(f"Minimum order: {minimum_order}")
#                 return minimum_order
#             except Exception as e:
#                 print(f"Error getting minimum order: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return "N/A"

#     async def extract_category_names(self, page):
#         print("Attempting to extract category names")
#         retries = 3
#         while retries > 0:
#             try:
#                 category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
#                 category_names = [await element.inner_text() for element in category_name_elements]
#                 print(f"Category names extracted: {category_names}")
#                 return category_names
#             except Exception as e:
#                 print(f"Error extracting category names: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return []

#     async def extract_category_links(self, page):
#         print("Attempting to extract category links")
#         retries = 3
#         while retries > 0:
#             try:
#                 category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
#                 category_links = [self.base_url + await element.get_attribute('href') for element in category_link_elements]
#                 print(f"Category links extracted: {category_links}")
#                 return category_links
#             except Exception as e:
#                 print(f"Error extracting category links: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return []

#     async def extract_sub_categories(self, page, category_link, grocery_title, category_name):
#         print(f"Attempting to extract sub-categories for: {category_link}")
#         retries = 3
#         sub_categories = []
#         completed_sub_categories = self.main_scraper.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {}).get("completed sub-categories", [])
#         current_sub_category = self.main_scraper.current_progress["current_progress"].get("current_sub_category")
#         start_processing = not current_sub_category
    
#         while retries > 0:
#             try:
#                 await page.goto(category_link, timeout=240000)
#                 await page.wait_for_load_state("networkidle", timeout=240000)
#                 sub_category_elements = await page.query_selector_all('//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
#                 sub_category_names = [await el.inner_text() for el in sub_category_elements]
#                 sub_category_links = [self.base_url + await el.get_attribute('href') for el in sub_category_elements]
    
#                 for idx, (sub_category_name, sub_category_link) in enumerate(zip(sub_category_names, sub_category_links)):
#                     if sub_category_name in completed_sub_categories:
#                         print(f"    Skipping completed sub-category: {sub_category_name}")
#                         continue
    
#                     if current_sub_category and not start_processing:
#                         if sub_category_name == current_sub_category:
#                             print(f"    Found current sub-category: {sub_category_name}, starting processing")
#                             start_processing = True
#                         else:
#                             print(f"    Skipping sub-category {sub_category_name}, waiting for {current_sub_category}")
#                             continue
    
#                     print(f"    Processing sub-category: {sub_category_name}")
#                     print(f"    Sub-category link: {sub_category_link}")
#                     self.main_scraper.current_progress["current_progress"]["current_sub_category"] = sub_category_name
#                     self.main_scraper.current_progress["current_progress"]["current_category"] = category_name  # Ensure correct category
#                     self.main_scraper.scraped_progress["current_progress"]["current_sub_category"] = sub_category_name
#                     self.main_scraper.scraped_progress["current_progress"]["current_category"] = category_name  # Ensure correct category
#                     self.main_scraper.save_current_progress()
#                     self.main_scraper.save_scraped_progress()
#                     items = await self.extract_all_items_from_sub_category(sub_category_link)
#                     sub_category_data = {
#                         "sub_category_name": sub_category_name,
#                         "sub_category_link": sub_category_link,
#                         "items": items
#                     }
#                     sub_categories.append(sub_category_data)
    
#                     # Mark sub-category as completed
#                     completed_groceries = self.main_scraper.current_progress["current_progress"]["completed_groceries"].setdefault(grocery_title, {})
#                     completed_groceries.setdefault("completed sub-categories", []).append(sub_category_name)
#                     self.main_scraper.current_progress["current_progress"]["completed_groceries"][grocery_title] = completed_groceries
#                     self.main_scraper.scraped_progress["current_progress"]["completed_groceries"] = self.main_scraper.current_progress["current_progress"]["completed_groceries"]
#                     self.main_scraper.save_current_progress()
#                     self.main_scraper.save_scraped_progress()
#                     self.main_scraper.commit_progress(f"Processed sub-category {sub_category_name} for {grocery_title} in {category_name}")
    
#                 # Check if category is complete
#                 if all(sub_cat_name in completed_sub_categories + [s["sub_category_name"] for s in sub_categories] for sub_cat_name in sub_category_names):
#                     completed_groceries = self.main_scraper.current_progress["current_progress"]["completed_groceries"].setdefault(grocery_title, {})
#                     completed_groceries.setdefault("completed categories", []).append(category_name)
#                     self.main_scraper.current_progress["current_progress"]["completed_groceries"][grocery_title] = completed_groceries
#                     self.main_scraper.scraped_progress["current_progress"]["completed_groceries"] = self.main_scraper.current_progress["current_progress"]["completed_groceries"]
#                     self.main_scraper.current_progress["current_progress"]["current_sub_category"] = None
#                     self.main_scraper.scraped_progress["current_progress"]["current_sub_category"] = None
#                     self.main_scraper.current_progress["current_progress"]["current_category"] = None  # Reset category when complete
#                     self.main_scraper.scraped_progress["current_progress"]["current_category"] = None
#                     self.main_scraper.save_current_progress()
#                     self.main_scraper.save_scraped_progress()
#                     self.main_scraper.commit_progress(f"Completed category {category_name} for {grocery_title}")
    
#                 # Update results
#                 area_name = self.main_scraper.current_progress["current_progress"]["area_name"]
#                 if area_name:
#                     grocery_data = self.main_scraper.scraped_progress["all_results"].setdefault(area_name, {}).setdefault(grocery_title, {
#                         "grocery_link": self.url,
#                         "delivery_time": "N/A",
#                         "grocery_details": {"delivery_fees": "N/A", "minimum_order": "N/A", "categories": {}}
#                     })
#                     grocery_data["grocery_details"]["categories"][category_name] = {
#                         "category_link": category_link,
#                         "sub_categories": sub_categories
#                     }
#                     self.main_scraper.scraped_progress["all_results"][area_name][grocery_title] = grocery_data
#                     self.main_scraper.save_scraped_progress()
#                     self.main_scraper.commit_progress(f"Updated results for {category_name} in {grocery_title}")
    
#                 return sub_categories
#             except Exception as e:
#                 print(f"Error extracting sub-categories: {e}")
#                 logging.error(f"Error extracting sub-categories: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return sub_categories
    
#     async def verify_sub_categories(self, page, category_link, grocery_title, category_name):
#         print(f"Verifying sub-categories for category: {category_name} at {category_link}")
#         retries = 3
#         missing_sub_categories = []
#         completed_sub_categories = self.main_scraper.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {}).get("completed sub-categories", [])

#         while retries > 0:
#             try:
#                 await page.goto(category_link, timeout=240000)
#                 await page.wait_for_load_state("networkidle", timeout=240000)
#                 sub_category_elements = await page.query_selector_all('//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
#                 sub_category_names = [await el.inner_text() for el in sub_category_elements]
#                 sub_category_links = [self.base_url + await el.get_attribute('href') for el in sub_category_elements]

#                 for name, link in zip(sub_category_names, sub_category_links):
#                     if name not in completed_sub_categories:
#                         print(f"Found missing sub-category: {name}")
#                         missing_sub_categories.append({"sub_category_name": name, "sub_category_link": link})
#                 return missing_sub_categories
#             except Exception as e:
#                 print(f"Error verifying sub-categories for {category_link}: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return missing_sub_categories

#     async def extract_item_details(self, item_link):
#         print(f"Attempting to extract item details for link: {item_link}")
#         retries = 3
#         while retries > 0:
#             try:
#                 # Create a new browser context for isolation
#                 context = await self.browser.new_context()
#                 page = await context.new_page()
    
#                 # Navigate with domcontentloaded to avoid waiting for all resources
#                 await page.goto(item_link, timeout=240000, wait_until="domcontentloaded")
                
#                 # Wait for a critical element (e.g., price or item name) to ensure content is loaded
#                 critical_selector = '//div[@class="price"] | //div[@data-testid="item-image"] | //p[@data-testid="item-description"]'
#                 await page.wait_for_selector(critical_selector, timeout=30000)
    
#                 # Scroll to bottom to trigger any lazy-loaded content
#                 await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
#                 await page.wait_for_timeout(2000)  # Short pause for dynamic content
    
#                 # Extract price (new price)
#                 price_selectors = [
#                     '//div[@class="price"]//span[@class="currency "]',
#                     '//span[contains(@class, "price")]',
#                     '//div[contains(@class, "price")]//span',
#                     '//div[contains(@class, "price")]//text()',
#                     '//span[@data-testid="price"]',
#                 ]
#                 item_price = "N/A"
#                 for selector in price_selectors:
#                     price_elements = await page.query_selector_all(selector)
#                     for element in price_elements:
#                         text = await element.inner_text()
#                         if text.strip() and text != "N/A":
#                             item_price = text.strip()
#                             break
#                     if item_price != "N/A":
#                         break
    
#                 # Extract old price
#                 old_price_selectors = [
#                     '//div[@class="price"]//p//span[@class="currency "]',
#                     '//span[contains(@class, "old-price")]',
#                     '//div[contains(@class, "price")]//p//span',
#                 ]
#                 item_old_price = None
#                 for selector in old_price_selectors:
#                     old_price_element = await page.query_selector(selector)
#                     if old_price_element:
#                         item_old_price = await old_price_element.inner_text()
#                         print(f"Item old price: {item_old_price}")
#                         break
#                 if not item_old_price:
#                     print("Item old price: None")
    
#                 # Extract offer
#                 offer_selectors = [
#                     '//div[@class="offer"]//div[@data-testid="offer-tag"]//span',
#                     '//span[contains(@class, "offer")]',
#                     '//div[contains(@class, "offer")]//span',
#                 ]
#                 item_offer = None
#                 for selector in offer_selectors:
#                     offer_element = await page.query_selector(selector)
#                     if offer_element:
#                         item_offer = await offer_element.inner_text()
#                         print(f"Item offer: {item_offer}")
#                         break
#                 if not item_offer:
#                     print("Item offer: None")
    
#                 # Extract description
#                 desc_selectors = [
#                     '//div[@class="description"]//p[@data-testid="item-description"]',
#                     '//div[contains(@class, "description")]//p',
#                     '//p[contains(@class, "description")]',
#                     '//div[@data-testid="item-description"]//p',
#                     '//section[contains(@class, "description")]//p',
#                 ]
#                 item_description = "N/A"
#                 for selector in desc_selectors:
#                     desc_element = await page.query_selector(selector)
#                     if desc_element:
#                         item_description = await desc_element.inner_text()
#                         if item_description.strip():
#                             break
#                 if item_description == "N/A":
#                     print("Item description: N/A")
    
#                 # Extract delivery time
#                 delivery_time_selectors = [
#                     '//div[@data-testid="delivery-tag"]//span',
#                     '//span[contains(@class, "delivery-time")]',
#                     '//div[contains(@class, "delivery-info")]//span',
#                 ]
#                 delivery_time = "N/A"
#                 for selector in delivery_time_selectors:
#                     delivery_time_element = await page.query_selector(selector)
#                     if delivery_time_element:
#                         delivery_time = await delivery_time_element.inner_text()
#                         break
    
#                 # Extract images
#                 image_selectors = [
#                     '//div[@data-testid="item-image"]//img',
#                     '//img[contains(@class, "item-image")]',
#                     '//img[@alt="product image"]',
#                     '//img[contains(@class, "product-image")]',
#                 ]
#                 item_images = []
#                 for selector in image_selectors:
#                     item_image_elements = await page.query_selector_all(selector)
#                     if item_image_elements:
#                         item_images = [await img.get_attribute('src') for img in item_image_elements if await img.get_attribute('src')]
#                         break
    
#                 # If critical data is missing, refresh the page once
#                 if item_price == "N/A" and item_description == "N/A" and not item_images:
#                     print("Critical data missing, refreshing page...")
#                     await page.reload(timeout=30000, wait_until="domcontentloaded")
#                     await page.wait_for_selector(critical_selector, timeout=30000)
#                     await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
#                     await page.wait_for_timeout(2000)
    
#                     # Retry price
#                     for selector in price_selectors:
#                         price_elements = await page.query_selector_all(selector)
#                         for element in price_elements:
#                             text = await element.inner_text()
#                             if text.strip() and text != "N/A":
#                                 item_price = text.strip()
#                                 break
#                         if item_price != "N/A":
#                             break
    
#                     # Retry description
#                     for selector in desc_selectors:
#                         desc_element = await page.query_selector(selector)
#                         if desc_element:
#                             item_description = await desc_element.inner_text()
#                             if item_description.strip():
#                                 break
    
#                     # Retry images
#                     for selector in image_selectors:
#                         item_image_elements = await page.query_selector_all(selector)
#                         if item_image_elements:
#                             item_images = [await img.get_attribute('src') for img in item_image_elements if await img.get_attribute('src')]
#                             break
    
#                 # Print in the desired order
#                 print(f"Item price: {item_price}")
#                 print(f"Item description: {item_description}")
#                 print(f"Item images: {item_images}")
#                 print(f"Delivery time range: {delivery_time}")
    
#                 # Safely close page and context
#                 await page.close()
#                 await context.close()
#                 return {
#                     "item_price": item_price,
#                     "item_old_price": item_old_price,
#                     "item_offer": item_offer,
#                     "item_description": item_description,
#                     "item_delivery_time_range": delivery_time,
#                     "item_images": item_images
#                 }
#             except Exception as e:
#                 print(f"Error extracting item details for {item_link}: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 # Safely close page and context if they exist
#                 if 'page' in locals():
#                     await page.close()
#                 if 'context' in locals():
#                     await context.close()
#                 await asyncio.sleep(5)
#         print(f"Failed to extract details for {item_link} after all retries")
#         return {
#             "item_price": "N/A",
#             "item_old_price": None,
#             "item_offer": None,
#             "item_description": "N/A",
#             "item_delivery_time_range": "N/A",
#             "item_images": []
#         }
    
#     async def extract_all_items_from_sub_category(self, sub_category_link):
#         print(f"Attempting to extract all items from sub-category: {sub_category_link}")
#         retries = 3
#         while retries > 0:
#             try:
#                 context = await self.browser.new_context()
#                 sub_page = await context.new_page()
#                 await sub_page.goto(sub_category_link, timeout=240000, wait_until="domcontentloaded")
#                 await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=30000)
    
#                 # Save HTML for debugging
#                 html_content = await sub_page.content()
#                 html_filename = f"sub_category_{sub_category_link.split('/')[-1].replace('?aid=37', '')}.html"
#                 with open(html_filename, "w", encoding="utf-8") as f:
#                     f.write(html_content)
#                 print(f"      Saved sub-category HTML to {html_filename} for debugging")
    
#                 pagination_element = await sub_page.query_selector('//div[@class="sc-104fa483-0 fCcIDQ"]//ul[@class="paginate-wrap"]')
#                 total_pages = 1
#                 if pagination_element:
#                     page_numbers = await pagination_element.query_selector_all('//li[contains(@class, "paginate-li f-16 f-500")]//a')
#                     total_pages = len(page_numbers) if page_numbers else 1
#                 print(f"      Found {total_pages} pages in this sub-category")
    
#                 items = []
#                 for page_number in range(1, total_pages + 1):
#                     print(f"      Processing page {page_number} of {total_pages}")
#                     page_url = f"{sub_category_link}&page={page_number}" if page_number > 1 else sub_category_link
#                     await sub_page.goto(page_url, timeout=240000, wait_until="domcontentloaded")
#                     await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=30000)
    
#                     item_elements = await sub_page.query_selector_all('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]//a[@data-testid="grocery-item-link-nofollow"]')
#                     print(f"        Found {len(item_elements)} items on page {page_number}")
    
#                     for i, element in enumerate(item_elements):
#                         try:
#                             name_selectors = [
#                                 'div[data-test="item-name"]',
#                                 'span[data-test="item-name"]',
#                                 'div[data-testid="product-name"]',
#                                 'span[data-testid="product-title"]',
#                                 'div[class*="product-name"]',
#                                 'span[class*="product-title"]',
#                                 'h3[class*="product-title"]'
#                             ]
#                             item_name = None
#                             for selector in name_selectors:
#                                 item_name_element = await element.query_selector(selector)
#                                 if item_name_element:
#                                     item_name = await item_name_element.inner_text()
#                                     if item_name and item_name.strip():
#                                         invalid_names = ['currency', 'kiki', 'market', 'grocery', 'mahboula']
#                                         if not any(invalid.lower() in item_name.lower() for invalid in invalid_names):
#                                             print(f"        Item name: {item_name}")
#                                             break
#                                         else:
#                                             print(f"        Selector '{selector}' found invalid name: {item_name} (matched: {[invalid for invalid in invalid_names if invalid.lower() in item_name.lower()]})")
#                                             item_name = None
#                                     else:
#                                         print(f"        Selector '{selector}' found empty or invalid name")
#                                 else:
#                                     print(f"        Selector '{selector}' not found")
    
#                             if not item_name or not item_name.strip():
#                                 item_name = f"Unknown Item {i+1}"
#                                 print(f"        No valid item name found, using default: {item_name}")
    
#                             item_link = self.base_url + await element.get_attribute('href')
#                             print(f"        Item link: {item_link}")
    
#                             item_details = await self.extract_item_details(item_link)
#                             items.append({
#                                 "item_name": item_name.strip(),
#                                 "item_link": item_link,
#                                 **item_details
#                             })
    
#                             # Add a slight delay to avoid server overload
#                             await asyncio.sleep(1)
#                         except Exception as e:
#                             print(f"        Error processing item {i+1}: {e}")
#                             logging.error(f"Error processing item {i+1} in {sub_category_link}: {e}")
#                 await sub_page.close()
#                 await context.close()
#                 return items
#             except Exception as e:
#                 print(f"Error extracting items from sub-category {sub_category_link}: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 if 'sub_page' in locals():
#                     await sub_page.close()
#                 if 'context' in locals():
#                     await context.close()
#                 await asyncio.sleep(5)
#         return []

#     async def extract_categories(self, page):
#         print(f"Processing grocery: {self.url}")
#         retries = 3
#         while retries > 0:
#             try:
#                 await page.goto(self.url, timeout=240000)
#                 await page.wait_for_load_state("networkidle", timeout=240000)
#                 print("Page loaded successfully")

#                 delivery_fees = await self.get_delivery_fees(page)
#                 minimum_order = await self.get_minimum_order(page)
#                 view_all_link = await self.get_general_link(page)

#                 print(f"  Delivery fees: {delivery_fees}")
#                 print(f"  Minimum order: {minimum_order}")

#                 categories_data = {}
#                 if view_all_link:
#                     print(f"  Navigating to view all link: {view_all_link}")
#                     category_page = await self.browser.new_page()
#                     await category_page.goto(view_all_link, timeout=240000)
#                     await category_page.wait_for_load_state("networkidle", timeout=240000)

#                     category_names = await self.extract_category_names(category_page)
#                     category_links = await self.extract_category_links(category_page)

#                     print(f"  Found {len(category_names)} categories")

#                     for name, link in zip(category_names, category_links):
#                         print(f"  Category: {name}, Link: {link}")
#                         categories_data[name] = {
#                             "category_link": link,
#                             "sub_categories": []
#                         }
#                     await category_page.close()

#                 return {
#                     "delivery_fees": delivery_fees,
#                     "minimum_order": minimum_order,
#                     "categories": categories_data
#                 }
#             except Exception as e:
#                 print(f"Error extracting categories: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return {"error": "Failed to extract categories after multiple attempts"}

# class MainScraper:
#     CURRENT_PROGRESS_FILE = "current_progress.json"
#     SCRAPED_PROGRESS_FILE = "scraped_progress.json"

#     def __init__(self):
#         self.output_dir = "output"
#         credentials_json = os.environ.get('TALABAT_GCLOUD_KEY_JSON')
#         if not credentials_json:
#             logging.warning("TALABAT_GCLOUD_KEY_JSON is not set. Google Drive uploads will fail.")
#             self.drive_uploader = SavingOnDrive(credentials_json=None)
#         else:
#             try:
#                 credentials_dict = json.loads(credentials_json)
#                 if not credentials_dict.get('type') == 'service_account':
#                     logging.warning("TALABAT_GCLOUD_KEY_JSON is not a valid service account key. Google Drive uploads will fail.")
#                     self.drive_uploader = SavingOnDrive(credentials_json=None)
#                 else:
#                     self.drive_uploader = SavingOnDrive(credentials_json=credentials_json)
#             except json.JSONDecodeError as e:
#                 logging.warning(f"TALABAT_GCLOUD_KEY_JSON is invalid JSON: {str(e)}. Google Drive uploads will fail.")
#                 self.drive_uploader = SavingOnDrive(credentials_json=None)
        
#         os.makedirs(self.output_dir, exist_ok=True)
#         self.current_progress = self.load_current_progress()
#         self.scraped_progress = self.load_scraped_progress()
#         self.github_token = os.environ.get('GITHUB_TOKEN')
#         if not self.github_token:
#             logging.warning("GITHUB_TOKEN is not set. Git pushes will be skipped.")
#         self.ensure_playwright_browsers()
#         self.commit_progress("Initialized progress files at scraper start")

#     def ensure_playwright_browsers(self):
#         subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)

#     def load_current_progress(self) -> Dict:
#         if os.path.exists(self.CURRENT_PROGRESS_FILE):
#             try:
#                 with open(self.CURRENT_PROGRESS_FILE, 'r', encoding='utf-8') as f:
#                     progress = json.load(f)
#                 if "current_progress" not in progress:
#                     progress["current_progress"] = {}
#                 current = progress["current_progress"]
#                 current.setdefault("processed_groceries", [])
#                 current.setdefault("completed_groceries", {})
#                 current.setdefault("area_name", None)
#                 current.setdefault("current_grocery", 0)
#                 current.setdefault("current_grocery_title", None)
#                 current.setdefault("current_grocery_link", None)
#                 current.setdefault("current_category", None)
#                 current.setdefault("current_sub_category", None)
#                 current.setdefault("total_groceries", 0)
#                 current["processed_groceries"] = list(set(current["processed_groceries"]))
#                 progress["completed_areas"] = list(set(progress.get("completed_areas", [])))
#                 progress["last_updated"] = progress.get("last_updated", datetime.now().isoformat())
#                 logging.info(f"Loaded current_progress.json: {json.dumps(progress, indent=2)}")
#                 return progress
#             except Exception as e:
#                 logging.error(f"Error loading current_progress.json: {e}")
        
#         default_progress = {
#             "completed_areas": [],
#             "current_area_index": 0,
#             "last_updated": datetime.now().isoformat(),
#             "current_progress": {
#                 "area_name": None,
#                 "current_grocery": 0,
#                 "current_grocery_title": None,
#                 "current_grocery_link": None,
#                 "total_groceries": 0,
#                 "processed_groceries": [],
#                 "current_category": None,
#                 "current_sub_category": None,
#                 "completed_groceries": {}
#             }
#         }
#         logging.info("current_progress.json not found, creating default")
#         self.save_current_progress(default_progress)
#         self.commit_progress("Created default current_progress.json")
#         return default_progress

#     def save_current_progress(self, progress: Dict = None):
#         progress = progress or self.current_progress
#         try:
#             progress["last_updated"] = datetime.now().isoformat()
#             if "current_progress" in progress:
#                 progress["current_progress"]["processed_groceries"] = list(set(progress["current_progress"].get("processed_groceries", [])))
#                 progress["completed_areas"] = list(set(progress.get("completed_areas", [])))
#             with tempfile.NamedTemporaryFile('w', delete=False, dir='.', encoding='utf-8') as temp_file:
#                 json.dump(progress, temp_file, indent=2, ensure_ascii=False)
#                 temp_file.flush()
#                 os.fsync(temp_file.fileno())
#                 temp_filename = temp_file.name
#             os.replace(temp_filename, self.CURRENT_PROGRESS_FILE)
#             logging.info(f"Saved current_progress.json: {self.CURRENT_PROGRESS_FILE}")
#         except Exception as e:
#             logging.error(f"Error saving current_progress.json: {e}")

#     def load_scraped_progress(self) -> Dict:
#         if os.path.exists(self.SCRAPED_PROGRESS_FILE):
#             try:
#                 with open(self.SCRAPED_PROGRESS_FILE, 'r', encoding='utf-8') as f:
#                     progress = json.load(f)
#                 if "current_progress" not in progress:
#                     progress["current_progress"] = {}
#                 if "all_results" not in progress:
#                     progress["all_results"] = {}
#                 current = progress["current_progress"]
#                 current.setdefault("processed_groceries", [])
#                 current.setdefault("completed_groceries", {})
#                 current.setdefault("area_name", None)
#                 current.setdefault("current_grocery", 0)
#                 current.setdefault("current_grocery_title", None)
#                 current.setdefault("current_grocery_link", None)
#                 current.setdefault("current_category", None)
#                 current.setdefault("current_sub_category", None)
#                 current.setdefault("total_groceries", 0)
#                 current["processed_groceries"] = list(set(current["processed_groceries"]))
#                 progress["completed_areas"] = list(set(progress.get("completed_areas", [])))
#                 progress["last_updated"] = progress.get("last_updated", datetime.now().isoformat())
#                 logging.info(f"Loaded scraped_progress.json: {json.dumps(progress, indent=2)}")
#                 return progress
#             except Exception as e:
#                 logging.error(f"Error loading scraped_progress.json: {e}")
        
#         default_progress = {
#             "completed_areas": [],
#             "current_area_index": 0,
#             "last_updated": datetime.now().isoformat(),
#             "current_progress": {
#                 "area_name": None,
#                 "current_grocery": 0,
#                 "current_grocery_title": None,
#                 "current_grocery_link": None,
#                 "total_groceries": 0,
#                 "processed_groceries": [],
#                 "current_category": None,
#                 "current_sub_category": None,
#                 "completed_groceries": {}
#             },
#             "all_results": {}
#         }
#         logging.info("scraped_progress.json not found, creating default")
#         self.save_scraped_progress(default_progress)
#         self.commit_progress("Created default scraped_progress.json")
#         return default_progress

#     def save_scraped_progress(self, progress: Dict = None):
#         progress = progress or self.scraped_progress
#         try:
#             progress["last_updated"] = datetime.now().isoformat()
#             if "current_progress" in progress:
#                 progress["current_progress"]["processed_groceries"] = list(set(progress["current_progress"].get("processed_groceries", [])))
#                 progress["completed_areas"] = list(set(progress.get("completed_areas", [])))
#             with tempfile.NamedTemporaryFile('w', delete=False, dir='.', encoding='utf-8') as temp_file:
#                 json.dump(progress, temp_file, indent=2, ensure_ascii=False)
#                 temp_file.flush()
#                 os.fsync(temp_file.fileno())
#                 temp_filename = temp_file.name
#             os.replace(temp_filename, self.SCRAPED_PROGRESS_FILE)
#             logging.info(f"Saved scraped_progress.json: {self.SCRAPED_PROGRESS_FILE}")
#         except Exception as e:
#             logging.error(f"Error saving scraped_progress.json: {e}")

#     @retry(tries=3, delay=2, backoff=2)
#     def commit_progress(self, message: str = "Periodic progress update"):
#         if not self.github_token:
#             logging.warning("No GITHUB_TOKEN, skipping commit")
#             return
#         try:
#             logging.info(f"Attempting to commit progress: {message}")
#             subprocess.run(["git", "add", self.CURRENT_PROGRESS_FILE, self.SCRAPED_PROGRESS_FILE, self.output_dir], check=True)
#             result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
#             if result.returncode == 0:
#                 subprocess.run(["git", "push"], check=True, env={"GIT_AUTH_TOKEN": self.github_token, **os.environ})
#                 logging.info(f"Successfully committed and pushed: {message}")
#             else:
#                 logging.info(f"No changes to commit: {result.stdout}")
#         except subprocess.CalledProcessError as e:
#             logging.error(f"Error committing progress: {e}")
#             raise

#     async def process_grocery_categories(self, grocery_title, grocery_details, talabat_grocery, page, groceries_on_page, grocery_idx):
#         completed_groceries = self.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {})
#         completed_categories = completed_groceries.get("completed categories", [])
#         current_category = self.current_progress["current_progress"].get("current_category")
#         current_sub_category = self.current_progress["current_progress"].get("current_sub_category")
#         categories = grocery_details.get("categories", {})
    
#         if not categories:
#             print(f"No categories found for {grocery_title}, marking as complete")
#             self.current_progress["current_progress"]["processed_groceries"].append(grocery_title)
#             self.scraped_progress["current_progress"]["processed_groceries"].append(grocery_title)
#             self.update_to_next_grocery(groceries_on_page, grocery_idx)
#             self.save_current_progress()
#             self.save_scraped_progress()
#             self.commit_progress(f"No categories for {grocery_title}, marked as complete")
#             return
    
#         start_processing = not current_category or not current_sub_category
#         category_names = list(categories.keys())
    
#         # If resuming, find the correct category for the current sub-category
#         if current_sub_category and current_category:
#             found = False
#             for category_name in category_names:
#                 sub_categories = await talabat_grocery.extract_sub_categories(page, categories[category_name]["category_link"], grocery_title, category_name)
#                 sub_category_names = [sub["sub_category_name"] for sub in sub_categories]
#                 if current_sub_category in sub_category_names:
#                     self.current_progress["current_progress"]["current_category"] = category_name
#                     self.scraped_progress["current_progress"]["current_category"] = category_name
#                     self.save_current_progress()
#                     self.save_scraped_progress()
#                     found = True
#                     break
#             if not found:
#                 print(f"Warning: Current sub-category {current_sub_category} not found in any category, resetting current_category")
#                 self.current_progress["current_progress"]["current_category"] = None
#                 self.scraped_progress["current_progress"]["current_category"] = None
#                 start_processing = True
    
#         for idx, category_name in enumerate(category_names):
#             if category_name in completed_categories:
#                 print(f"Category {category_name} already completed, skipping")
#                 continue
    
#             if current_category and not start_processing:
#                 if category_name == current_category:
#                     start_processing = True
#                 else:
#                     print(f"Skipping category {category_name}, waiting for {current_category}")
#                     continue
    
#             print(f"Processing category {idx + 1}/{len(category_names)}: {category_name}")
#             self.current_progress["current_progress"]["current_category"] = category_name
#             self.scraped_progress["current_progress"]["current_category"] = category_name
#             self.save_current_progress()
#             self.save_scraped_progress()
    
#             temp_page = await self.browser.new_page()
#             await self.process_category(grocery_title, categories[category_name], category_name, talabat_grocery, temp_page)
#             await temp_page.close()
    
#             # Move to next category if current one is complete
#             completed_groceries = self.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {})
#             if category_name in completed_groceries.get("completed categories", []):
#                 self.move_to_next_category(category_names, idx, grocery_title, completed_categories)
    
#         # Verify sub-categories after processing all categories
#         await self.verify_and_scrape_missing_sub_categories(grocery_title, grocery_details, talabat_grocery, page)
    
#         # Mark grocery as complete if all categories are done
#         completed_groceries = self.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {})
#         if all(cat in completed_groceries.get("completed categories", []) for cat in category_names):
#             self.current_progress["current_progress"]["processed_groceries"].append(grocery_title)
#             self.scraped_progress["current_progress"]["processed_groceries"].append(grocery_title)
#             self.update_to_next_grocery(groceries_on_page, grocery_idx)
#             self.save_current_progress()
#             self.save_scraped_progress()
#             self.commit_progress(f"Completed all categories for {grocery_title}")
    
#     async def verify_and_scrape_missing_sub_categories(self, grocery_title, grocery_details, talabat_grocery, page):
#         print(f"Verifying sub-categories for grocery: {grocery_title}")
#         area_name = self.current_progress["current_progress"]["area_name"]
#         completed_groceries = self.current_progress["current_progress"]["completed_groceries"].setdefault(grocery_title, {})
#         completed_sub_categories = completed_groceries.get("completed sub-categories", [])
    
#         for category_name, category_data in grocery_details.get("categories", {}).items():
#             category_link = category_data["category_link"]
#             print(f"Checking category: {category_name}")
    
#             missing_sub_categories = await talabat_grocery.verify_sub_categories(page, category_link, grocery_title, category_name)
#             if missing_sub_categories:
#                 print(f"Found {len(missing_sub_categories)} missing sub-categories in {category_name}")
#                 for missing_sub in missing_sub_categories:
#                     sub_category_name = missing_sub["sub_category_name"]
#                     sub_category_link = missing_sub["sub_category_link"]
#                     print(f"Scraping missing sub-category: {sub_category_name}")
#                     self.current_progress["current_progress"]["current_category"] = category_name  # Set correct category
#                     self.current_progress["current_progress"]["current_sub_category"] = sub_category_name
#                     self.scraped_progress["current_progress"]["current_category"] = category_name  # Set correct category
#                     self.scraped_progress["current_progress"]["current_sub_category"] = sub_category_name
#                     self.save_current_progress()
#                     self.save_scraped_progress()
#                     items = await talabat_grocery.extract_all_items_from_sub_category(sub_category_link)
#                     sub_category_data = {
#                         "sub_category_name": sub_category_name,
#                         "sub_category_link": sub_category_link,
#                         "items": items
#                     }
    
#                     # Update category sub-categories
#                     grocery_details["categories"][category_name]["sub_categories"].append(sub_category_data)
    
#                     # Update completed sub-categories
#                     completed_sub_categories.append(sub_category_name)
#                     completed_groceries["completed sub-categories"] = completed_sub_categories
#                     self.current_progress["current_progress"]["completed_groceries"][grocery_title] = completed_groceries
#                     self.scraped_progress["current_progress"]["completed_groceries"] = self.current_progress["current_progress"]["completed_groceries"]
    
#                     # Update scraped results
#                     grocery_data = self.scraped_progress["all_results"].setdefault(area_name, {}).setdefault(grocery_title, {
#                         "grocery_link": talabat_grocery.url,
#                         "delivery_time": "N/A",
#                         "grocery_details": grocery_details
#                     })
#                     grocery_data["grocery_details"] = grocery_details
#                     self.scraped_progress["all_results"][area_name][grocery_title] = grocery_data
    
#                     # Save progress and commit
#                     self.current_progress["current_progress"]["current_sub_category"] = None
#                     self.scraped_progress["current_progress"]["current_sub_category"] = None
#                     self.current_progress["current_progress"]["current_category"] = None  # Reset category after sub-category
#                     self.scraped_progress["current_progress"]["current_category"] = None
#                     self.save_current_progress()
#                     self.save_scraped_progress()
#                     self.commit_progress(f"Scraped missing sub-category {sub_category_name} for {grocery_title} in {category_name}")
    
#         # Update JSON file
#         json_filename = os.path.join(self.output_dir, f"{area_name}.json")
#         with open(json_filename, 'w', encoding='utf-8') as f:
#             json.dump(self.scraped_progress["all_results"].get(area_name, {}), f, indent=2, ensure_ascii=False)
#         if self.upload_to_drive(json_filename):
#             logging.info(f"Uploaded updated {json_filename} to Google Drive")
#         else:
#             logging.warning(f"Failed to upload updated {json_filename} to Google Drive")
    
#         # Wait 30 seconds and update Excel
#         print(f"Waiting 30 seconds before updating Excel for {area_name}...")
#         await asyncio.sleep(30)
#         await self.convert_json_to_excel(area_name, json_filename)
    
#     def move_to_next_category(self, category_names, current_idx, grocery_title, completed_categories):
#         next_idx = current_idx + 1
#         self.current_progress["current_progress"]["current_category"] = None
#         self.scraped_progress["current_progress"]["current_category"] = None
#         self.current_progress["current_progress"]["current_sub_category"] = None
#         self.scraped_progress["current_progress"]["current_sub_category"] = None
#         while next_idx < len(category_names):
#             next_category = category_names[next_idx]
#             if next_category not in completed_categories:
#                 self.current_progress["current_progress"]["current_category"] = next_category
#                 self.scraped_progress["current_progress"]["current_category"] = next_category
#                 break
#             next_idx += 1
#         self.save_current_progress()
#         self.save_scraped_progress()
#         self.commit_progress(f"Moved to next category after {category_names[current_idx]} for {grocery_title}")

#     def update_to_next_grocery(self, groceries_on_page, current_idx):
#         processed_grocery_titles = set(self.current_progress["current_progress"]["processed_groceries"])
#         next_idx = current_idx + 1
#         self.current_progress["current_progress"].update({
#             "current_grocery": 0,
#             "current_grocery_title": None,
#             "current_grocery_link": None,
#             "current_category": None,
#             "current_sub_category": None
#         })
#         self.scraped_progress["current_progress"].update({
#             "current_grocery": 0,
#             "current_grocery_title": None,
#             "current_grocery_link": None,
#             "current_category": None,
#             "current_sub_category": None
#         })
#         while next_idx < len(groceries_on_page):
#             next_grocery = groceries_on_page[next_idx]
#             if next_grocery["grocery_title"] not in processed_grocery_titles:
#                 self.current_progress["current_progress"].update({
#                     "current_grocery": next_idx + 1,
#                     "current_grocery_title": next_grocery["grocery_title"],
#                     "current_grocery_link": next_grocery["grocery_link"]
#                 })
#                 self.scraped_progress["current_progress"].update({
#                     "current_grocery": next_idx + 1,
#                     "current_grocery_title": next_grocery["grocery_title"],
#                     "current_grocery_link": next_grocery["grocery_link"]
#                 })
#                 break
#             next_idx += 1
#         self.save_current_progress()
#         self.save_scraped_progress()
#         self.commit_progress(f"Updated to next grocery after index {current_idx}")

#     async def convert_json_to_excel(self, area_name: str, json_filename: str):
#         try:
#             with open(json_filename, 'r', encoding='utf-8') as f:
#                 data = json.load(f)
            
#             if not data:
#                 logging.warning(f"No data to write to Excel for area: {area_name}")
#                 return

#             excel_filename = os.path.join(self.output_dir, f"{area_name}_detailed.xlsx")
#             with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
#                 for grocery_title, grocery_data in data.items():
#                     sheet_name = re.sub(r'[\\\/:*?"<>|]', '_', grocery_title)[:31]
#                     general_info = {
#                         "Grocery Title": grocery_title,
#                         "Delivery Time": grocery_data.get("delivery_time", "N/A"),
#                         "Delivery Fees": grocery_data.get("grocery_details", {}).get("delivery_fees", "N/A"),
#                         "Minimum Order": grocery_data.get("grocery_details", {}).get("minimum_order", "N/A"),
#                         "URL": grocery_data.get("grocery_link", "N/A")
#                     }
#                     simplified_data = []
#                     for category_name, category_data in grocery_data.get("grocery_details", {}).get("categories", {}).items():
#                         for sub_category in category_data.get("sub_categories", []):
#                             items_list = [
#                                 {
#                                     "Item Name": item.get("item_name", "N/A"),
#                                     "Item Price": item.get("item_price", "N/A"),
#                                     "Item Old Price": item.get("item_old_price", None),
#                                     "Item Offer": item.get("item_offer", None),
#                                     "Item Description": item.get("item_description", "N/A"),
#                                     "Item Link": item.get("item_link", "N/A")
#                                 }
#                                 for item in sub_category.get("items", [])
#                             ]
#                             items_json = json.dumps(items_list, ensure_ascii=False)
#                             simplified_data.append({
#                                 **general_info,
#                                 "Category": category_name,
#                                 "Category Link": category_data.get("category_link", "N/A"),
#                                 "Sub-Category": sub_category.get("sub_category_name", "N/A"),
#                                 "Sub-Category Link": sub_category.get("sub_category_link", "N/A"),
#                                 "Items": items_json
#                             })
                    
#                     if simplified_data:
#                         df = pd.DataFrame(simplified_data)
#                         df.to_excel(writer, sheet_name=sheet_name, index=False)
#                         logging.info(f"Added sheet '{sheet_name}' to Excel: {excel_filename}")
#                     else:
#                         logging.warning(f"No data for grocery '{grocery_title}' in area: {area_name}")

#             logging.info(f"Successfully created Excel: {excel_filename}")
#             if self.upload_to_drive(excel_filename):
#                 logging.info(f"Uploaded {excel_filename} to Google Drive")
#             else:
#                 logging.warning(f"Failed to upload {excel_filename} to Google Drive")
#         except Exception as e:
#             logging.error(f"Error converting JSON to Excel for {area_name}: {e}")

#     async def scrape_and_save_area(self, area_name: str, area_url: str, browser) -> List[Dict]:
#         self.browser = browser
#         print(f"\n{'='*50}\nSCRAPING AREA: {area_name}\nURL: {area_url}\n{'='*50}")
#         all_area_results = self.scraped_progress["all_results"].get(area_name, {})
#         current_progress = self.current_progress["current_progress"]
#         scraped_current_progress = self.scraped_progress["current_progress"]

#         if current_progress["area_name"] != area_name:
#             current_progress.update({
#                 "area_name": area_name,
#                 "current_grocery": 0,
#                 "current_grocery_title": None,
#                 "current_grocery_link": None,
#                 "total_groceries": 0,
#                 "processed_groceries": [],
#                 "current_category": None,
#                 "current_sub_category": None,
#                 "completed_groceries": {}
#             })
#             scraped_current_progress.update(current_progress)
#             self.save_current_progress()
#             self.save_scraped_progress()
#             self.commit_progress(f"Started scraping {area_name}")

#         # Fetch initial groceries
#         page = await browser.new_page()
#         await page.goto(area_url, timeout=60000)
#         groceries_on_page = await self.get_page_groceries(page)
#         current_progress["total_groceries"] = len(groceries_on_page)
#         scraped_current_progress["total_groceries"] = len(groceries_on_page)
#         print(f"Found {len(groceries_on_page)} groceries")
#         await page.close()

#         processed_grocery_titles = set(current_progress["processed_groceries"])
#         current_grocery_title = current_progress.get("current_grocery_title")
#         current_grocery_link = current_progress.get("current_grocery_link")

#         # Process groceries
#         for grocery_idx, grocery in enumerate(groceries_on_page):
#             grocery_num = grocery_idx + 1
#             grocery_title = grocery["grocery_title"]
#             grocery_link = grocery["grocery_link"]

#             if grocery_title in processed_grocery_titles:
#                 print(f"Skipping already processed grocery: {grocery_title} (link: {grocery_link})")
#                 continue

#             if current_grocery_title and current_grocery_title != grocery_title:
#                 print(f"Skipping grocery {grocery_title}, waiting for current grocery {current_grocery_title} ({current_grocery_link})")
#                 continue

#             current_progress["current_grocery"] = grocery_num
#             current_progress["current_grocery_title"] = grocery_title
#             current_progress["current_grocery_link"] = grocery_link
#             scraped_current_progress["current_grocery"] = grocery_num
#             scraped_current_progress["current_grocery_title"] = grocery_title
#             scraped_current_progress["current_grocery_link"] = grocery_link
#             self.save_current_progress()
#             self.save_scraped_progress()
#             print(f"Processing grocery {grocery_num}/{len(groceries_on_page)}: {grocery_title} (link: {grocery_link})")

#             grocery_page = await browser.new_page()
#             talabat_grocery = TalabatGroceries(grocery_link, browser, self)
#             grocery_details = await talabat_grocery.extract_categories(grocery_page)
#             all_area_results[grocery_title] = {
#                 "grocery_link": grocery_link,
#                 "delivery_time": grocery["delivery_time"],
#                 "grocery_details": grocery_details
#             }
#             self.scraped_progress["all_results"][area_name] = all_area_results
#             self.save_scraped_progress()

#             await self.process_grocery_categories(grocery_title, grocery_details, talabat_grocery, grocery_page, groceries_on_page, grocery_idx)
#             await grocery_page.close()

#             # Exit loop if we processed the current grocery
#             if current_grocery_title == grocery_title:
#                 break

#         # Verify missing groceries
#         print(f"Verifying groceries for area: {area_name}")
#         page = await browser.new_page()
#         await page.goto(area_url, timeout=60000)
#         current_groceries = await self.get_page_groceries(page)
#         await page.close()

#         missing_groceries = [g for g in current_groceries if g["grocery_title"] not in processed_grocery_titles]
#         if missing_groceries:
#             print(f"Found {len(missing_groceries)} missing groceries in {area_name}")
#             for grocery_idx, grocery in enumerate(missing_groceries):
#                 grocery_num = len(groceries_on_page) + grocery_idx + 1
#                 grocery_title = grocery["grocery_title"]
#                 grocery_link = grocery["grocery_link"]
#                 print(f"Processing missing grocery {grocery_num}: {grocery_title} (link: {grocery_link})")

#                 current_progress["current_grocery"] = grocery_num
#                 current_progress["current_grocery_title"] = grocery_title
#                 current_progress["current_grocery_link"] = grocery_link
#                 scraped_current_progress["current_grocery"] = grocery_num
#                 scraped_current_progress["current_grocery_title"] = grocery_title
#                 scraped_current_progress["current_grocery_link"] = grocery_link
#                 self.save_current_progress()
#                 self.save_scraped_progress()

#                 grocery_page = await browser.new_page()
#                 talabat_grocery = TalabatGroceries(grocery_link, browser, self)
#                 grocery_details = await talabat_grocery.extract_categories(grocery_page)
#                 all_area_results[grocery_title] = {
#                     "grocery_link": grocery_link,
#                     "delivery_time": grocery["delivery_time"],
#                     "grocery_details": grocery_details
#                 }
#                 self.scraped_progress["all_results"][area_name] = all_area_results
#                 self.save_scraped_progress()

#                 await self.process_grocery_categories(grocery_title, grocery_details, talabat_grocery, grocery_page, groceries_on_page + missing_groceries, grocery_idx)
#                 await grocery_page.close()

#         # Save JSON file
#         json_filename = os.path.join(self.output_dir, f"{area_name}.json")
#         with open(json_filename, 'w', encoding='utf-8') as f:
#             json.dump(all_area_results, f, indent=2, ensure_ascii=False)
#         if self.upload_to_drive(json_filename):
#             logging.info(f"Uploaded {json_filename} to Google Drive")
#         else:
#             logging.warning(f"Failed to upload {json_filename} to Google Drive")

#         # Mark area as complete if all groceries processed
#         processed_grocery_titles = set(current_progress["processed_groceries"])
#         if all(g["grocery_title"] in processed_grocery_titles for g in current_groceries):
#             current_progress.update({
#                 "area_name": None,
#                 "current_grocery": 0,
#                 "current_grocery_title": None,
#                 "current_grocery_link": None,
#                 "total_groceries": 0,
#                 "processed_groceries": [],
#                 "current_category": None,
#                 "current_sub_category": None,
#                 "completed_groceries": {}
#             })
#             scraped_current_progress.update(current_progress)
#             self.current_progress["completed_areas"].append(area_name)
#             self.scraped_progress["completed_areas"].append(area_name)

#         self.save_current_progress()
#         self.save_scraped_progress()
#         self.commit_progress(f"Completed {area_name}")

#         # Wait 30 seconds and create Excel
#         print(f"Waiting 30 seconds before converting {area_name}.json to Excel...")
#         await asyncio.sleep(30)
#         await self.convert_json_to_excel(area_name, json_filename)

#         return list(all_area_results.values())

#     async def process_category(self, grocery_title, category_data, category_name, talabat_grocery, page):
#         sub_categories = await talabat_grocery.extract_sub_categories(page, category_data["category_link"], grocery_title, category_name)
#         category_data["sub_categories"] = sub_categories
#         self.save_current_progress()
#         self.save_scraped_progress()

#     async def get_page_groceries(self, page) -> List[Dict]:
#         logging.info("Extracting grocery information")
#         try:
#             await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=30000)
#             vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
#             groceries_info = []
#             for container in vendor_containers:
#                 title_element = await container.query_selector('a div h2')
#                 title = await title_element.inner_text() if title_element else "Unknown Grocery"
#                 link_element = await container.query_selector('a')
#                 link = "https://www.talabat.com" + await link_element.get_attribute('href') if link_element else None
#                 delivery_info = await container.query_selector('div.deliveryInfo')
#                 delivery_time_text = await delivery_info.inner_text() if delivery_info else ""
#                 delivery_time = re.findall(r'\d+', delivery_time_text)[0] + " mins" if re.findall(r'\d+', delivery_time_text) else "N/A"
#                 if link:
#                     groceries_info.append({"grocery_title": title, "grocery_link": link, "delivery_time": delivery_time})
#             logging.info(f"Extracted {len(groceries_info)} groceries: {[g['grocery_title'] for g in groceries_info]}")
#             return groceries_info
#         except Exception as e:
#             logging.error(f"Error extracting groceries: {e}")
#             return []

#     @retry(tries=3, delay=2, backoff=2)
#     def upload_to_drive(self, file_path):
#         logging.info(f"Uploading {file_path} to Google Drive...")
#         try:
#             if not self.drive_uploader.credentials_json:
#                 logging.error("TALABAT_GCLOUD_KEY_JSON is not set or invalid.")
#                 return False
#             if not self.drive_uploader.authenticate():
#                 logging.error("Failed to authenticate with Google Drive. Check TALABAT_GCLOUD_KEY_JSON validity.")
#                 return False
#             file_ids = self.drive_uploader.upload_to_multiple_folders(file_path)
#             success = len(file_ids) == 2
#             if success:
#                 logging.info(f"Successfully uploaded {file_path} to Google Drive")
#             else:
#                 logging.error(f"Failed to upload {file_path}: Incomplete upload to folders")
#             return success
#         except Exception as e:
#             if "429" in str(e) or "rate limit" in str(e).lower():
#                 logging.error(f"Rate limit exceeded while uploading to Google Drive: {str(e)}")
#             else:
#                 logging.error(f"Error uploading to Google Drive: {str(e)}")
#             return False

#     async def run(self):
#         ahmadi_areas = [
#             ("الظهر", "https://www.talabat.com/kuwait/groceries/59/dhaher"),
#             ("الرقه", "https://www.talabat.com/kuwait/groceries/37/riqqa"),
#             ("هدية", "https://www.talabat.com/kuwait/groceries/30/hadiya"),
#             ("المنقف", "https://www.talabat.com/kuwait/groceries/32/mangaf"),
#             ("أبو حليفة", "https://www.talabat.com/kuwait/groceries/2/abu-halifa"),
#             ("الفنطاس", "https://www.talabat.com/kuwait/groceries/38/fintas"),
#             ("العقيلة", "https://www.talabat.com/kuwait/groceries/79/egaila"),
#             ("الصباحية", "https://www.talabat.com/kuwait/groceries/31/sabahiya"),
#             ("الأحمدي", "https://www.talabat.com/kuwait/groceries/3/al-ahmadi"),
#             ("الفحيحيل", "https://www.talabat.com/kuwait/groceries/5/fahaheel"),
#             ("شرق الأحمدي", "https://www.talabat.com/kuwait/groceries/3/al-ahmadi"),
#             ("ضاحية علي صباح السالم", "https://www.talabat.com/kuwait/groceries/82/ali-sabah-al-salem-umm-al-hayman"),
#             ("ميناء عبد الله", "https://www.talabat.com/kuwait/groceries/100/mina-abdullah"),
#             ("بنيدر", "https://www.talabat.com/kuwait/groceries/6650/bnaider"),
#             ("الزور", "https://www.talabat.com/kuwait/groceries/2053/zour"),
#             ("الجليعة", "https://www.talabat.com/kuwait/groceries/6860/al-julaiaa"),
#             ("المهبولة", "https://www.talabat.com/kuwait/groceries/24/mahboula"),
#             ("النويصيب", "https://www.talabat.com/kuwait/groceries/2054/nuwaiseeb"),
#             ("الخيران", "https://www.talabat.com/kuwait/groceries/2726/khairan"),
#             ("الوفرة", "https://www.talabat.com/kuwait/groceries/2057/wafra-farms"),
#             ("ضاحية فهد الأحمد", "https://www.talabat.com/kuwait/groceries/98/fahad-al-ahmed"),
#             ("ضاحية جابر العلي", "https://www.talabat.com/kuwait/groceries/60/jaber-al-ali"),
#             ("مدينة صباح الأحمد السكنية", "https://www.talabat.com/kuwait/groceries/6931/sabah-al-ahmad-2"),
#             ("مدينة صباح الأحمد البحرية", "https://www.talabat.com/kuwait/groceries/2726/khairan"),
#             ("ميناء الأحمدي", "https://www.talabat.com/kuwait/groceries/3/al-ahmadi")
#         ]

#         async with async_playwright() as p:
#             browser = await p.chromium.launch(headless=True)
#             current_area_index = self.current_progress["current_area_index"]
#             for idx, (area_name, area_url) in enumerate(ahmadi_areas):
#                 if idx < current_area_index or area_name in self.current_progress["completed_areas"]:
#                     print(f"Skipping already completed or earlier area: {area_name}")
#                     continue
#                 self.current_progress["current_area_index"] = idx
#                 self.scraped_progress["current_area_index"] = idx
#                 await self.scrape_and_save_area(area_name, area_url, browser)
#                 self.save_current_progress()
#                 self.save_scraped_progress()
#                 self.commit_progress(f"Completed {area_name}")
#             await browser.close()

#         print("SCRAPING COMPLETED")

# async def main():
#     scraper = MainScraper()
#     await scraper.run()

# if __name__ == "__main__":
#     asyncio.run(main())








# # import asyncio
# # import json
# # import os
# # import re
# # import nest_asyncio
# # from bs4 import BeautifulSoup
# # import pandas as pd
# # from playwright.async_api import async_playwright
# # from SavingOnDrive import SavingOnDrive

# # class MainScraper:
# #     def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
# #         self.target_url = target_url
# #         self.base_url = "https://www.talabat.com"
# #         self.json_file = "talabat_groceries.json"
# #         self.excel_file = "dhaher.xlsx"
# #         self.groceries_data = {}
# #         self.drive_uploader = SavingOnDrive('credentials.json')  # Initialize SavingOnDrive
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
# #         print("Saving data to JSON file")
# #         try:
# #             with open(self.json_file, 'w') as f:
# #                 json.dump(self.groceries_data, f, indent=4)
# #             print(f"Data saved to {self.json_file}")
# #         except Exception as e:
# #             print(f"Error saving to JSON file: {e}")
               
# #     def save_to_excel(self):
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
# #             # Upload to Google Drive after saving
# #             self.upload_to_drive()
# #         except Exception as e:
# #             print(f"Error saving to Excel: {e}")

# #     def upload_to_drive(self):
# #         """Upload the Excel file to Google Drive"""
# #         print(f"\nUploading {self.excel_file} to Google Drive...")
# #         try:
# #             if not self.drive_uploader.authenticate():
# #                 print("Failed to authenticate with Google Drive")
# #                 return False
# #             file_ids = self.drive_uploader.upload_to_multiple_folders(self.excel_file)
# #             if len(file_ids) == len(self.drive_uploader.target_folders):
# #                 print(f"Uploaded {self.excel_file} to Google Drive successfully")
# #                 return True
# #             else:
# #                 print(f"Failed to upload {self.excel_file} to all target folders")
# #                 return False
# #         except Exception as e:
# #             print(f"Error uploading to Google Drive: {str(e)}")
# #             return False

# #     async def process_grocery(self, grocery_info):
# #         grocery_title = grocery_info['grocery_title']
# #         grocery_link = grocery_info['grocery_link']
# #         delivery_time = grocery_info['delivery_time']
# #         print(f"Processing grocery: {grocery_title}")
# #         if grocery_title in self.groceries_data:
# #             print(f"Skipping {grocery_title} - already processed")
# #             return
# #         talabat_grocery = TalabatGroceries(grocery_link)
# #         for browser_type in ["chromium", "firefox"]:
# #             try:
# #                 async with async_playwright() as p:
# #                     browser = await p[browser_type].launch(headless=True)
# #                     page = await browser.new_page()
# #                     grocery_details = await talabat_grocery.extract_categories(page)
# #                     self.groceries_data[grocery_title] = {
# #                         'grocery_link': grocery_link,
# #                         'delivery_time': delivery_time,
# #                         'grocery_details': grocery_details
# #                     }
# #                     self.save_to_json()
# #                     print(f"Successfully processed and saved data for {grocery_title}")
# #                     break
# #             except Exception as e:
# #                 print(f"Error processing {grocery_title} with {browser_type}: {e}")
# #                 continue
# #             finally:
# #                 await browser.close()

# #     async def run(self):
# #         print("Starting the scraper")
# #         retries = 3
# #         while retries > 0:
# #             try:
# #                 print(f"Target URL: {self.target_url}")
# #                 async with async_playwright() as p:
# #                     browser = await p.chromium.launch(headless=True)
# #                     page = await browser.new_page()
# #                     page.set_default_timeout(240000)
# #                     await page.goto(self.target_url, timeout=240000)
# #                     await page.wait_for_load_state("networkidle", timeout=240000)
# #                     print("Page loaded successfully")
# #                     try:
# #                         await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=240000)
# #                         print("Grocery vendor elements found")
# #                     except Exception as e:
# #                         print(f"Error waiting for vendor elements: {e}")
# #                         CHIprint("Attempting to continue anyway...")
# #                     groceries_info = await self.extract_grocery_info(page)
# #                     await browser.close()
# #                     print(f"Found {len(groceries_info)} groceries to process")
# #                     for grocery_info in groceries_info:
# #                         await self.process_grocery(grocery_info)
# #                     self.save_to_excel()
# #                     print("Scraping completed successfully")
# #                 break
# #             except Exception as e:
# #                 print(f"Error in main scraper: {e}")
# #                 retries -= 1
# #                 print(f"Retries left: {retries}")
# #                 await asyncio.sleep(5)
# #                 if retries == 0:
# #                     print("Failed to complete the scraping process after multiple attempts.")

# # async def main():
# #     if not os.path.exists('credentials.json'):
# #         credentials_json = os.environ.get('TALABAT_GCLOUD_KEY_JSON')
# #         if credentials_json:
# #             with open('credentials.json', 'w') as f:
# #                 f.write(credentials_json)
# #             print("Created credentials.json from environment variable")
# #         else:
# #             print("ERROR: credentials.json not found and TALABAT_GCLOUD_KEY_JSON not set!")
# #             return
# #     scraper = MainScraper()
# #     await scraper.run()

# # if __name__ == "__main__":
# #     asyncio.run(main())
# # else:
# #     asyncio.get_event_loop().run_until_complete(main())

# # # class MainScraper:
# # #     def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
# # #         self.target_url = target_url
# # #         self.base_url = "https://www.talabat.com"
# # #         self.json_file = "talabat_groceries.json"
# # #         self.excel_file = "dhaher.xlsx"
# # #         self.groceries_data = {}
# # #         print(f"Initialized MainScraper with target URL: {self.target_url}")

# # #         if not os.path.exists(self.json_file):
# # #             print(f"Creating new JSON file: {self.json_file}")
# # #             with open(self.json_file, 'w') as f:
# # #                 json.dump({}, f)
# # #         else:
# # #             print(f"Loading data from JSON file: {self.json_file}")
# # #             with open(self.json_file, 'r') as f:
# # #                 try:
# # #                     self.groceries_data = json.load(f)
# # #                     print(f"Loaded data: {self.groceries_data}")
# # #                 except json.JSONDecodeError:
# # #                     print("Error decoding JSON file. Starting with an empty dictionary.")
# # #                     self.groceries_data = {}

# # #     async def extract_grocery_info(self, page):
# # #         print("Attempting to extract grocery information")
# # #         retries = 3
# # #         while retries > 0:
# # #             try:
# # #                 vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
# # #                 print(f"Found {len(vendor_containers)} grocery vendors on page")

# # #                 groceries_info = []
# # #                 for i, container in enumerate(vendor_containers, 1):
# # #                     try:
# # #                         title_element = await container.query_selector('a div h2')
# # #                         grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"
# # #                         print(f"  Grocery title: {grocery_title}")

# # #                         link_element = await container.query_selector('a')
# # #                         grocery_link = self.base_url + await link_element.get_attribute('href') if link_element else None
# # #                         print(f"  Grocery link: {grocery_link}")

# # #                         delivery_info = await container.query_selector('div.deliveryInfo')
# # #                         delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"
# # #                         print(f"  Delivery time text: {delivery_time_text}")

# # #                         if delivery_time_text != "N/A":
# # #                             digits = re.findall(r'\d+', delivery_time_text)
# # #                             delivery_time = f"{digits[0]} mins" if digits else "N/A"
# # #                         else:
# # #                             delivery_time = "N/A"
# # #                         print(f"  Delivery time: {delivery_time}")

# # #                         if grocery_title and grocery_link and delivery_time:
# # #                             print(f"Found grocery: {grocery_title}, Delivery time: {delivery_time}")
# # #                             groceries_info.append({
# # #                                 'grocery_title': grocery_title,
# # #                                 'grocery_link': grocery_link,
# # #                                 'delivery_time': delivery_time
# # #                             })
# # #                     except Exception as e:
# # #                         print(f"Error processing grocery {i}: {e}")
# # #                 return groceries_info
# # #             except Exception as e:
# # #                 print(f"Error extracting grocery information: {e}")
# # #                 retries -= 1
# # #                 print(f"Retries left: {retries}")
# # #                 await asyncio.sleep(5)
# # #         return []

# # #     def save_to_json(self):
# # #         """Save scraped data to JSON file"""
# # #         print("Saving data to JSON file")
# # #         try:
# # #             with open(self.json_file, 'w') as f:
# # #                 json.dump(self.groceries_data, f, indent=4)
# # #             print(f"Data saved to {self.json_file}")
# # #         except Exception as e:
# # #             print(f"Error saving to JSON file: {e}")
               
# # #     def save_to_excel(self):
# # #         """Save data from JSON to Excel file with each grocery in a separate sheet"""
# # #         print("Saving data to Excel file")
# # #         try:
# # #             writer = pd.ExcelWriter(self.excel_file, engine='xlsxwriter')

# # #             for grocery_title, grocery_data in self.groceries_data.items():
# # #                 flattened_data = []

# # #                 general_info = {
# # #                     'grocery_title': grocery_title,
# # #                     'delivery_time': grocery_data.get('delivery_time', 'N/A'),
# # #                     'delivery_fees': grocery_data.get('grocery_details', {}).get('delivery_fees', 'N/A'),
# # #                     'minimum_order': grocery_data.get('grocery_details', {}).get('minimum_order', 'N/A')
# # #                 }

# # #                 categories = grocery_data.get('grocery_details', {}).get('categories', [])
# # #                 for category in categories:
# # #                     category_name = category.get('name', 'N/A')
# # #                     for sub_category in category.get('sub_categories', []):
# # #                         sub_category_name = sub_category.get('sub_category_name', 'N/A')
# # #                         for item in sub_category.get('Items', []):
# # #                             item_data = {
# # #                                 **general_info,
# # #                                 'category': category_name,
# # #                                 'sub_category': sub_category_name,
# # #                                 'item_name': item.get('item_name', 'N/A'),
# # #                                 'item_price': item.get('item_price', 'N/A'),
# # #                                 'item_description': item.get('item_description', 'N/A'),
# # #                                 'item_link': item.get('item_link', 'N/A')
# # #                             }
# # #                             flattened_data.append(item_data)

# # #                 if not flattened_data:
# # #                     flattened_data.append(general_info)

# # #                 safe_title = re.sub(r'[\\/*?:\[\]]', '_', grocery_title)[:31]
# # #                 df = pd.DataFrame(flattened_data)
# # #                 df.to_excel(writer, sheet_name=safe_title, index=False)

# # #             writer.close()
# # #             print(f"Data saved to {self.excel_file}")
# # #         except Exception as e:
# # #             print(f"Error saving to Excel: {e}")

# # #     async def process_grocery(self, grocery_info):
# # #         """Process a single grocery using TalabatGroceries class"""
# # #         grocery_title = grocery_info['grocery_title']
# # #         grocery_link = grocery_info['grocery_link']
# # #         delivery_time = grocery_info['delivery_time']

# # #         print(f"Processing grocery: {grocery_title}")

# # #         # Skip if already processed
# # #         if grocery_title in self.groceries_data:
# # #             print(f"Skipping {grocery_title} - already processed")
# # #             return

# # #         # Initialize TalabatGroceries with the grocery link
# # #         talabat_grocery = TalabatGroceries(grocery_link)

# # #         # Extract grocery details using Playwright
# # #         for browser_type in ["chromium", "firefox"]:
# # #             try:
# # #                 async with async_playwright() as p:
# # #                     browser = await p[browser_type].launch(headless=True)
# # #                     page = await browser.new_page()

# # #                     grocery_details = await talabat_grocery.extract_categories(page)

# # #                     # Store data in groceries_data dictionary
# # #                     self.groceries_data[grocery_title] = {
# # #                         'grocery_link': grocery_link,
# # #                         'delivery_time': delivery_time,
# # #                         'grocery_details': grocery_details
# # #                     }

# # #                     # Save progress after each grocery
# # #                     self.save_to_json()
# # #                     print(f"Successfully processed and saved data for {grocery_title}")
# # #                     break  # Exit the loop if successful
# # #             except Exception as e:
# # #                 print(f"Error processing {grocery_title} with {browser_type}: {e}")
# # #                 continue  # Try the next browser type
# # #             finally:
# # #                 await browser.close()

# # #     async def run(self):
# # #         """Main method to run the scraper"""
# # #         print("Starting the scraper")
# # #         retries = 3
# # #         while retries > 0:
# # #             try:
# # #                 print(f"Target URL: {self.target_url}")

# # #                 # Initialize playwright and navigate to target URL
# # #                 async with async_playwright() as p:
# # #                     browser = await p.chromium.launch(headless=True)  # Always use headless mode
# # #                     page = await browser.new_page()

# # #                     # Set longer timeouts and wait for page load
# # #                     page.set_default_timeout(240000)  # 240 seconds

# # #                     # Navigate to the target URL
# # #                     await page.goto(self.target_url, timeout=240000)
# # #                     await page.wait_for_load_state("networkidle", timeout=240000)
# # #                     print("Page loaded successfully")

# # #                     # Wait for grocery vendor elements to load
# # #                     try:
# # #                         await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=240000)
# # #                         print("Grocery vendor elements found")
# # #                     except Exception as e:
# # #                         print(f"Error waiting for vendor elements: {e}")
# # #                         print("Attempting to continue anyway...")

# # #                     # Extract grocery information directly from page
# # #                     groceries_info = await self.extract_grocery_info(page)
# # #                     await browser.close()

# # #                     print(f"Found {len(groceries_info)} groceries to process")

# # #                     # Process each grocery sequentially
# # #                     for grocery_info in groceries_info:
# # #                         await self.process_grocery(grocery_info)

# # #                     # Save all data to Excel
# # #                     self.save_to_excel()
# # #                     print("Scraping completed successfully")
# # #                 break  # Exit retry loop if successful
# # #             except Exception as e:
# # #                 print(f"Error in main scraper: {e}")
# # #                 retries -= 1
# # #                 print(f"Retries left: {retries}")
# # #                 await asyncio.sleep(5)
# # #                 if retries == 0:
# # #                     print("Failed to complete the scraping process after multiple attempts.")


# # # # Main execution point - now compatible with notebook environments
# # # async def main():
# # #     # Initialize and run the scraper
# # #     scraper = MainScraper()
# # #     await scraper.run()

# # # # Handle both script and notebook execution
# # # if __name__ == "__main__":
# # #     asyncio.run(main())
# # # else:
# # #     # For notebook/IPython environment, use this method to run
# # #     asyncio.get_event_loop().run_until_complete(main())

