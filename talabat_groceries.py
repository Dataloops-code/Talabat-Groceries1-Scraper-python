import asyncio
import json
import os
import re
import nest_asyncio
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright
from SavingOnDrive import SavingOnDrive

nest_asyncio.apply()

class TalabatGroceries:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.talabat.com"
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

    async def extract_sub_categories(self, page, category_xpath):
        print(f"Attempting to extract sub-categories using XPath: {category_xpath}")
        retries = 3
        while retries > 0:
            try:
                sub_category_elements = await page.query_selector_all(f'{category_xpath}//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
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
                return sub_categories
            except Exception as e:
                print(f"Error extracting sub-categories: {e}")
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

    async def extract_all_items_from_sub_category(self, sub_category_link):
        print(f"Attempting to extract all items from sub-category: {sub_category_link}")
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
                    print(f"      Found {total_pages} pages in this sub-category")
                    items = []
                    for page_number in range(1, total_pages + 1):
                        print(f"      Processing page {page_number} of {total_pages}")
                        page_url = f"{sub_category_link}&page={page_number}"
                        await sub_page.goto(page_url, timeout=240000)
                        await sub_page.wait_for_load_state("networkidle", timeout=240000)
                        await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)
                        item_elements = await sub_page.query_selector_all('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]//a[@data-testid="grocery-item-link-nofollow"]')
                        print(f"        Found {len(item_elements)} items on page {page_number}")
                        for i, element in enumerate(item_elements):
                            try:
                                item_name_element = await element.query_selector('div[data-test="item-name"]')
                                item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {i+1}"
                                print(f"        Item name: {item_name}")
                                item_link = self.base_url + await element.get_attribute('href')
                                print(f"        Item link: {item_link}")
                                item_details = await self.extract_item_details(item_link)
                                items.append({
                                    "item_name": item_name,
                                    "item_link": item_link,
                                    **item_details
                                })
                            except Exception as e:
                                print(f"        Error processing item {i+1}: {e}")
                    await browser.close()
                    if items != default_values:
                        return items
            except Exception as e:
                print(f"Error extracting items from sub-category {sub_category_link} using {browser_type}: {e}")
                continue
        return default_values

    async def extract_categories(self, page):
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
                        categories_data = []
                        for index, (name, link) in enumerate(zip(category_names, category_links)):
                            print(f"  Processing category {index+1}/{len(category_names)}: {name}")
                            print(f"  Category link: {link}")
                            category_xpath = f'//div[@data-testid="category-item-component"][{index + 1}]'
                            async with async_playwright() as p:
                                browser = await p.chromium.launch(headless=True)
                                sub_category_page = await browser.new_page()
                                await sub_category_page.goto(link, timeout=240000)
                                await sub_category_page.wait_for_load_state("networkidle", timeout=240000)
                                sub_categories = await self.extract_sub_categories(sub_category_page, category_xpath)
                                await browser.close()
                            print(f"  Found {len(sub_categories)} sub-categories in {name}")
                            category_data = {
                                "name": name,
                                "link": link,
                                "sub_categories": sub_categories
                            }
                            categories_data.append(category_data)
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


# class TalabatGroceries:
#     def __init__(self, url):
#         self.url = url
#         self.base_url = "https://www.talabat.com"
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

#     async def extract_sub_categories(self, page, category_xpath):
#         print(f"Attempting to extract sub-categories using XPath: {category_xpath}")
#         retries = 3
#         while retries > 0:
#             try:
#                 sub_category_elements = await page.query_selector_all(f'{category_xpath}//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
#                 sub_categories = []
#                 for element in sub_category_elements:
#                     try:
#                         sub_category_name = await element.inner_text()
#                         sub_category_link = self.base_url + await element.get_attribute('href')
#                         print(f"    Processing sub-category: {sub_category_name}")
#                         print(f"    Sub-category link: {sub_category_link}")
    
#                         # Try extracting items from each sub-category link using different browsers
#                         items = await self.extract_all_items_from_sub_category(sub_category_link)
    
#                         sub_categories.append({
#                             "sub_category_name": sub_category_name,
#                             "sub_category_link": sub_category_link,
#                             "Items": items
#                         })
#                     except Exception as e:
#                         print(f"Error processing sub-category: {e}")
#                 return sub_categories
#             except Exception as e:
#                 print(f"Error extracting sub-categories: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return []
    
#     async def extract_item_details_new_tab(self, item_link, browser_type):
#         print(f"Attempting to extract item details in a new tab for link: {item_link} using {browser_type}")
#         retries = 3
#         while retries > 0:
#             try:
#                 async with async_playwright() as p:
#                     browser = await p[browser_type].launch(headless=True)
#                     page = await browser.new_page()
#                     await page.goto(item_link, timeout=240000)

#                     await page.wait_for_load_state("networkidle", timeout=240000)

#                     item_price_element = await page.query_selector('//div[@class="price"]//span[@class="currency "]')
#                     item_price = await item_price_element.inner_text() if item_price_element else "N/A"
#                     print(f"Item price: {item_price}")

#                     item_description_element = await page.query_selector('//div[@class="description"]//p[@data-testid="item-description"]')
#                     item_description = await item_description_element.inner_text() if item_description_element else "N/A"
#                     print(f"Item description: {item_description}")

#                     delivery_time_element = await page.query_selector('//div[@data-testid="delivery-tag"]//span')
#                     delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"
#                     print(f"Delivery time range: {delivery_time}")

#                     item_image_elements = await page.query_selector_all('//div[@data-testid="item-image"]//img')
#                     item_images = [await img.get_attribute('src') for img in item_image_elements]
#                     print(f"Item images: {item_images}")

#                     await browser.close()

#                     return {
#                         "item_price": item_price,
#                         "item_description": item_description,
#                         "item_delivery_time_range": delivery_time,
#                         "item_images": item_images
#                     }
#             except Exception as e:
#                 print(f"Error extracting item details for {item_link} in new tab using {browser_type}: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return {
#             "item_price": "N/A",
#             "item_description": "N/A",
#             "item_delivery_time_range": "N/A",
#             "item_images": []
#         }

#     async def extract_item_details(self, item_link):
#         print(f"Attempting to extract item details for link: {item_link}")
#         default_values = {
#             "item_price": "N/A",
#             "item_description": "N/A",
#             "item_delivery_time_range": "N/A",
#             "item_images": []
#         }

#         for browser_type in ["chromium", "firefox"]:
#             try:
#                 result = await self.extract_item_details_new_tab(item_link, browser_type)
#                 if result != default_values:
#                     return result
#             except Exception as e:
#                 print(f"Error extracting item details for {item_link} using {browser_type}: {e}")
#                 continue  # Try the next browser type
#         return default_values

#     async def extract_all_items_from_sub_category(self, sub_category_link):
#         print(f"Attempting to extract all items from sub-category: {sub_category_link}")
#         default_values = []
#         for browser_type in ["chromium", "firefox"]:
#             try:
#                 async with async_playwright() as p:
#                     browser = await p[browser_type].launch(headless=True)
#                     sub_page = await browser.new_page()
#                     await sub_page.goto(sub_category_link, timeout=240000)
#                     await sub_page.wait_for_load_state("networkidle", timeout=240000)
#                     await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)

#                     pagination_element = await sub_page.query_selector('//div[@class="sc-104fa483-0 fCcIDQ"]//ul[@class="paginate-wrap"]')
#                     total_pages = 1

#                     if pagination_element:
#                         page_numbers = await pagination_element.query_selector_all('//li[contains(@class, "paginate-li f-16 f-500")]//a')
#                         total_pages = len(page_numbers) if page_numbers else 1
#                     print(f"      Found {total_pages} pages in this sub-category")

#                     items = []
#                     for page_number in range(1, total_pages + 1):
#                         print(f"      Processing page {page_number} of {total_pages}")
#                         page_url = f"{sub_category_link}&page={page_number}"
#                         await sub_page.goto(page_url, timeout=240000)
#                         await sub_page.wait_for_load_state("networkidle", timeout=240000)

#                         await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)

#                         item_elements = await sub_page.query_selector_all('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]//a[@data-testid="grocery-item-link-nofollow"]')
#                         print(f"        Found {len(item_elements)} items on page {page_number}")

#                         for i, element in enumerate(item_elements):
#                             try:
#                                 item_name_element = await element.query_selector('div[data-test="item-name"]')
#                                 item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {i+1}"
#                                 print(f"        Item name: {item_name}")

#                                 item_link = self.base_url + await element.get_attribute('href')
#                                 print(f"        Item link: {item_link}")

#                                 item_details = await self.extract_item_details(item_link)

#                                 items.append({
#                                     "item_name": item_name,
#                                     "item_link": item_link,
#                                     **item_details
#                                 })
#                             except Exception as e:
#                                 print(f"        Error processing item {i+1}: {e}")

#                     await browser.close()
#                     if items != default_values:
#                         return items
#             except Exception as e:
#                 print(f"Error extracting items from sub-category {sub_category_link} using {browser_type}: {e}")
#                 continue  # Try the next browser type
#         return default_values

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

#                 if view_all_link:
#                     print(f"  Navigating to view all link: {view_all_link}")
#                     async with async_playwright() as p:
#                         browser = await p.chromium.launch(headless=True)
#                         category_page = await browser.new_page()
#                         await category_page.goto(view_all_link, timeout=240000)
#                         await category_page.wait_for_load_state("networkidle", timeout=240000)

#                         category_names = await self.extract_category_names(category_page)
#                         category_links = await self.extract_category_links(category_page)

#                         print(f"  Found {len(category_names)} categories")

#                         categories_data = []
#                         for index, (name, link) in enumerate(zip(category_names, category_links)):
#                             print(f"  Processing category {index+1}/{len(category_names)}: {name}")
#                             print(f"  Category link: {link}")
#                             category_xpath = f'//div[@data-testid="category-item-component"][{index + 1}]'
#                             async with async_playwright() as p:
#                                 browser = await p.chromium.launch(headless=True)
#                                 sub_category_page = await browser.new_page()
#                                 await sub_category_page.goto(link, timeout=240000)
#                                 await sub_category_page.wait_for_load_state("networkidle", timeout=240000)

#                                 sub_categories = await self.extract_sub_categories(sub_category_page, category_xpath)
#                                 await browser.close()

#                             print(f"  Found {len(sub_categories)} sub-categories in {name}")
#                             category_data = {
#                                 "name": name,
#                                 "link": link,
#                                 "sub_categories": sub_categories
#                             }
#                             categories_data.append(category_data)

#                         await browser.close()

#                 grocery_data = {
#                     "delivery_fees": delivery_fees,
#                     "minimum_order": minimum_order,
#                     "categories": categories_data
#                 }

#                 return grocery_data
#             except Exception as e:
#                 print(f"Error extracting categories: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return {"error": "Failed to extract categories after multiple attempts"}
