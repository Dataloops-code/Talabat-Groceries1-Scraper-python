import asyncio
import json
import os
import re
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright


class TalabatGroceries:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.talabat.com"

    async def get_general_link(self, page):
        try:
            link_element = await page.get_attribute('//a[@data-testid="view-all-link"]', 'href')
            if link_element:
                full_link = self.base_url + link_element
                return full_link
            else:
                return None
        except Exception as e:
            print(f"Error getting general link: {e}")
            return None

    async def get_delivery_fees(self, page):
        try:
            delivery_fees_element = await page.query_selector(
                'xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]')
            delivery_fees = await delivery_fees_element.inner_text() if delivery_fees_element else "N/A"
            return delivery_fees
        except Exception as e:
            print(f"Error getting delivery fees: {e}")
            return "N/A"

    async def get_minimum_order(self, page):
        try:
            minimum_order_element = await page.query_selector(
                'xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]')
            minimum_order = await minimum_order_element.inner_text() if minimum_order_element else "N/A"
            return minimum_order
        except Exception as e:
            print(f"Error getting minimum order: {e}")
            return "N/A"

    async def extract_category_names(self, page):
        try:
            category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
            category_names = [await element.inner_text() for element in category_name_elements]
            return category_names
        except Exception as e:
            print(f"Error extracting category names: {e}")
            return []

    async def extract_category_links(self, page):
        try:
            category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
            category_links = [self.base_url + await element.get_attribute('href') for element in category_link_elements]
            return category_links
        except Exception as e:
            print(f"Error extracting category links: {e}")
            return []

    async def extract_sub_categories(self, page, category_xpath):
        try:
            sub_category_elements = await page.query_selector_all(
                f'{category_xpath}//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
            sub_categories = []
            for element in sub_category_elements:
                sub_category_name = await element.inner_text()
                sub_category_link = self.base_url + await element.get_attribute('href')
                print(f"    Processing sub-category: {sub_category_name}")
                items = await self.extract_all_items_from_sub_category(sub_category_link)
                sub_categories.append({
                    "sub_category_name": sub_category_name,
                    "sub_category_link": sub_category_link,
                    "Items": items
                })
            return sub_categories
        except Exception as e:
            print(f"Error extracting sub-categories: {e}")
            return []

    async def extract_item_details(self, item_link):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(item_link, timeout=60000)

                # Wait for the page to load
                await page.wait_for_load_state("networkidle", timeout=60000)

                item_price_element = await page.query_selector('//div[@class="price"]//span[@class="currency "]')
                item_price = await item_price_element.inner_text() if item_price_element else "N/A"

                item_description_element = await page.query_selector(
                    '//div[@class="description"]//p[@data-testid="item-description"]')
                item_description = await item_description_element.inner_text() if item_description_element else "N/A"

                delivery_time_element = await page.query_selector('//div[@data-testid="delivery-tag"]//span')
                delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"

                item_image_elements = await page.query_selector_all('//div[@data-testid="item-image"]//img')
                item_images = [await img.get_attribute('src') for img in item_image_elements]

                await browser.close()

                return {
                    "item_price": item_price,
                    "item_description": item_description,
                    "item_delivery_time_range": delivery_time,
                    "item_images": item_images
                }
        except Exception as e:
            print(f"Error extracting item details for {item_link}: {e}")
            return {
                "item_price": "N/A",
                "item_description": "N/A",
                "item_delivery_time_range": "N/A",
                "item_images": []
            }

    async def extract_all_items_from_sub_category(self, sub_category_link):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(sub_category_link, timeout=60000)

                # Wait for the page to load
                await page.wait_for_load_state("networkidle", timeout=60000)

                # Check for pagination
                pagination_element = await page.query_selector(
                    '//div[@class="sc-104fa483-0 fCcIDQ"]//ul[@class="paginate-wrap"]')
                total_pages = 1

                if pagination_element:
                    page_numbers = await pagination_element.query_selector_all(
                        '//li[contains(@class, "paginate-li f-16 f-500")]//a')
                    total_pages = len(page_numbers) if page_numbers else 1

                print(f"      Found {total_pages} pages in this sub-category")

                items = []
                for page_number in range(1, total_pages + 1):
                    print(f"      Processing page {page_number} of {total_pages}")
                    page_url = f"{sub_category_link}&page={page_number}"
                    await page.goto(page_url, timeout=60000)
                    await page.wait_for_load_state("networkidle", timeout=60000)

                    item_elements = await page.query_selector_all(
                        '//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]//a[@data-testid="grocery-item-link-nofollow"]')
                    print(f"        Found {len(item_elements)} items on page {page_number}")

                    for i, element in enumerate(item_elements):
                        try:
                            item_name_element = await element.query_selector('div[data-test="item-name"]')
                            item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {i + 1}"

                            item_link = self.base_url + await element.get_attribute('href')
                            print(f"        Processing item {i + 1}/{len(item_elements)}: {item_name}")

                            item_details = await self.extract_item_details(item_link)

                            items.append({
                                "item_name": item_name,
                                "item_link": item_link,
                                **item_details
                            })
                        except Exception as e:
                            print(f"        Error processing item {i + 1}: {e}")

                await browser.close()
                return items
        except Exception as e:
            print(f"Error extracting items from sub-category {sub_category_link}: {e}")
            return []

    async def extract_categories(self, page):
        try:
            print(f"Processing grocery: {self.url}")
            await page.goto(self.url, timeout=60000)
            await page.wait_for_load_state("networkidle", timeout=60000)

            # Get general information
            delivery_fees = await self.get_delivery_fees(page)
            minimum_order = await self.get_minimum_order(page)
            view_all_link = await self.get_general_link(page)

            print(f"  Delivery fees: {delivery_fees}")
            print(f"  Minimum order: {minimum_order}")

            # Extract categories
            if view_all_link:
                print(f"  Navigating to view all link: {view_all_link}")
                await page.goto(view_all_link, timeout=60000)
                await page.wait_for_load_state("networkidle", timeout=60000)

            category_names = await self.extract_category_names(page)
            category_links = await self.extract_category_links(page)

            print(f"  Found {len(category_names)} categories")

            categories_data = []
            for index, (name, link) in enumerate(zip(category_names, category_links)):
                print(f"  Processing category {index + 1}/{len(category_names)}: {name}")
                category_xpath = f'//div[@data-testid="category-item-component"][{index + 1}]'
                sub_categories = await self.extract_sub_categories(page, category_xpath)
                category_data = {
                    "name": name,
                    "link": link,
                    "sub_categories": sub_categories
                }
                categories_data.append(category_data)

            grocery_data = {
                "delivery_fees": delivery_fees,
                "minimum_order": minimum_order,
                "categories": categories_data
            }

            return grocery_data
        except Exception as e:
            print(f"Error extracting categories: {e}")
            return {"error": str(e)}