import asyncio
import json
import os
import tempfile
import sys
import subprocess
import re
from typing import Dict, List
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from SavingOnDrive import SavingOnDrive
import logging
from datetime import datetime
from retry import retry
import argparse
import aiohttp
import psutil
import random
from concurrent.futures import ThreadPoolExecutor


# Set up logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class TalabatGroceries:
    def __init__(self, url, browser, main_scraper):
        self.url = url
        self.base_url = "https://www.talabat.com"
        self.browser = browser
        self.main_scraper = main_scraper
        print(f"Initialized TalabatGroceries with URL: {self.url}")

    async def get_general_link(self, page):
        print("Attempting to get general link")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    link_element = await page.wait_for_selector('//a[@data-testid="view-all-link"]', timeout=90000)
                    if link_element:
                        full_link = self.base_url + await link_element.get_attribute('href')
                        print(f"General link found: {full_link}")
                        return full_link
                    else:
                        print("General link not found")
                        return None
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error getting general link: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    print(f"Unexpected error getting general link: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return None

    async def get_delivery_fees(self, page):
        print("Attempting to get delivery fees")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    delivery_fees_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]')
                    delivery_fees = await delivery_fees_element.inner_text() if delivery_fees_element else "N/A"
                    print(f"Delivery fees: {delivery_fees}")
                    return delivery_fees
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error getting delivery fees: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    print(f"Unexpected error getting delivery fees: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return "N/A"

    async def get_minimum_order(self, page):
        print("Attempting to get minimum order")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    minimum_order_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]')
                    minimum_order = await minimum_order_element.inner_text() if minimum_order_element else "N/A"
                    print(f"Minimum order: {minimum_order}")
                    return minimum_order
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error getting minimum order: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    print(f"Unexpected error getting minimum order: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return "N/A"

    async def extract_category_names(self, page):
        print("Attempting to extract category names")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
                    category_names = [await element.inner_text() for element in category_name_elements]
                    print(f"Category names extracted: {category_names}")
                    return category_names
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error extracting category names: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    print(f"Unexpected error extracting category names: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return []

    async def extract_category_links(self, page):
        print("Attempting to extract category links")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
                    category_links = [self.base_url + await element.get_attribute('href') for element in category_link_elements]
                    print(f"Category links extracted: {category_links}")
                    return category_links
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error extracting category links: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    print(f"Unexpected error extracting category links: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return []

    async def extract_sub_categories(self, page, category_link, grocery_title, category_name):
        print(f"Attempting to extract sub-categories for: {category_link}")
        async with self.main_scraper.semaphore:
            retries = 3
            sub_categories = []
            completed_sub_categories = self.main_scraper.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {}).get("completed sub-categories", [])
            current_sub_category = self.main_scraper.current_progress["current_progress"].get("current_sub_category")
            start_processing = not current_sub_category

            while retries > 0:
                try:
                    response = await page.goto(category_link, timeout=240000, wait_until="domcontentloaded")
                    if response and response.status == 429:
                        print(f"Rate limit hit (429) for {category_link}, waiting before retry...")
                        retries -= 1
                        await asyncio.sleep(30)
                        continue
                    elif response and response.status >= 500:
                        print(f"Server error ({response.status}) for {category_link}, retrying...")
                        retries -= 1
                        await asyncio.sleep(10)
                        continue

                    await page.wait_for_load_state("networkidle", timeout=240000)
                    sub_category_elements = await page.query_selector_all('//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
                    sub_category_names = [await el.inner_text() for el in sub_category_elements]
                    sub_category_links = [self.base_url + await el.get_attribute('href') for el in sub_category_elements]

                    for idx, (sub_category_name, sub_category_link) in enumerate(zip(sub_category_names, sub_category_links)):
                        if sub_category_name in completed_sub_categories:
                            print(f"    Skipping completed sub-category: {sub_category_name}")
                            continue

                        if current_sub_category and not start_processing:
                            if sub_category_name == current_sub_category:
                                print(f"    Found current sub-category: {sub_category_name}, starting processing")
                                start_processing = True
                            else:
                                print(f"    Skipping sub-category {sub_category_name}, waiting for {current_sub_category}")
                                continue

                        print(f"    Processing sub-category: {sub_category_name}")
                        print(f"    Sub-category link: {sub_category_link}")
                        self.main_scraper.current_progress["current_progress"]["current_sub_category"] = sub_category_name
                        self.main_scraper.current_progress["current_progress"]["current_category"] = category_name
                        self.main_scraper.scraped_progress["current_progress"]["current_sub_category"] = sub_category_name
                        self.main_scraper.scraped_progress["current_progress"]["current_category"] = category_name
                        self.main_scraper.save_current_progress()
                        self.main_scraper.save_scraped_progress()
                        items = await self.extract_all_items_from_sub_category(sub_category_link)
                        sub_category_data = {
                            "sub_category_name": sub_category_name,
                            "sub_category_link": sub_category_link,
                            "items": items
                        }
                        sub_categories.append(sub_category_data)

                        completed_groceries = self.main_scraper.current_progress["current_progress"]["completed_groceries"].setdefault(grocery_title, {})
                        completed_groceries.setdefault("completed sub-categories", []).append(sub_category_name)
                        self.main_scraper.current_progress["current_progress"]["completed_groceries"][grocery_title] = completed_groceries
                        self.main_scraper.scraped_progress["current_progress"]["completed_groceries"] = self.main_scraper.current_progress["current_progress"]["completed_groceries"]
                        self.main_scraper.save_current_progress()
                        self.main_scraper.save_scraped_progress()
                        self.main_scraper.commit_progress(f"Processed sub-category {sub_category_name} for {grocery_title} in {category_name}")

                    if all(sub_cat_name in completed_sub_categories + [s["sub_category_name"] for s in sub_categories] for sub_cat_name in sub_category_names):
                        completed_groceries = self.main_scraper.current_progress["current_progress"]["completed_groceries"].setdefault(grocery_title, {})
                        completed_groceries.setdefault("completed categories", []).append(category_name)
                        self.main_scraper.current_progress["current_progress"]["completed_groceries"][grocery_title] = completed_groceries
                        self.main_scraper.scraped_progress["current_progress"]["completed_groceries"] = self.main_scraper.current_progress["current_progress"]["completed_groceries"]
                        self.main_scraper.current_progress["current_progress"]["current_sub_category"] = None
                        self.main_scraper.scraped_progress["current_progress"]["current_sub_category"] = None
                        self.main_scraper.current_progress["current_progress"]["current_category"] = None
                        self.main_scraper.scraped_progress["current_progress"]["current_category"] = None
                        self.main_scraper.save_current_progress()
                        self.main_scraper.save_scraped_progress()
                        self.main_scraper.commit_progress(f"Completed category {category_name} for {grocery_title}")

                    area_name = self.main_scraper.current_progress["current_progress"]["area_name"]
                    if area_name:
                        grocery_data = self.main_scraper.scraped_progress["all_results"].setdefault(area_name, {}).setdefault(grocery_title, {
                            "grocery_link": self.url,
                            "delivery_time": "N/A",
                            "grocery_details": {"delivery_fees": "N/A", "minimum_order": "N/A", "categories": {}}
                        })
                        grocery_data["grocery_details"]["categories"][category_name] = {
                            "category_link": category_link,
                            "sub_categories": sub_categories
                        }
                        self.main_scraper.scraped_progress["all_results"][area_name][grocery_title] = grocery_data
                        self.main_scraper.save_scraped_progress()
                        self.main_scraper.commit_progress(f"Updated results for {category_name} in {grocery_title}")

                    return sub_categories
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error extracting sub-categories: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    print(f"Unexpected error extracting sub-categories: {e}")
                    logging.error(f"Error extracting sub-categories: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return sub_categories

    async def verify_sub_categories(self, page, category_link, grocery_title, category_name):
        print(f"Verifying sub-categories for category: {category_name} at {category_link}")
        async with self.main_scraper.semaphore:
            retries = 3
            missing_sub_categories = []
            completed_sub_categories = self.main_scraper.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {}).get("completed sub-categories", [])

            while retries > 0:
                try:
                    response = await page.goto(category_link, timeout=240000, wait_until="domcontentloaded")
                    if response and response.status == 429:
                        print(f"Rate limit hit (429) for {category_link}, waiting before retry...")
                        retries -= 1
                        await asyncio.sleep(30)
                        continue
                    elif response and response.status >= 500:
                        print(f"Server error ({response.status}) for {category_link}, retrying...")
                        retries -= 1
                        await asyncio.sleep(10)
                        continue

                    await page.wait_for_load_state("networkidle", timeout=240000)
                    sub_category_elements = await page.query_selector_all('//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
                    sub_category_names = [await el.inner_text() for el in sub_category_elements]
                    sub_category_links = [self.base_url + await el.get_attribute('href') for el in sub_category_elements]

                    for name, link in zip(sub_category_names, sub_category_links):
                        if name not in completed_sub_categories:
                            print(f"Found missing sub-category: {name}")
                            missing_sub_categories.append({"sub_category_name": name, "sub_category_link": link})
                    return missing_sub_categories
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error verifying sub-categories for {category_link}: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    print(f"Unexpected error verifying sub-categories for {category_link}: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return missing_sub_categories

    async def extract_item_details(self, item_link):
        """Extract item details with enhanced rate limit handling and proxy rotation."""
        print(f"Attempting to extract item details for link: {item_link}")
        async with self.main_scraper.semaphore:
            retries = 3
            context = None
            page = None

            for attempt in range(retries):
                try:
                    async with self.main_scraper.context_semaphore:
                        self.main_scraper.active_contexts += 1
                        browser = await self.main_scraper.get_browser()
                        proxy = random.choice(self.main_scraper.proxies) if self.main_scraper.proxies else None
                        context = await browser.new_context(
                            user_agent=random.choice(self.main_scraper.user_agents),
                            viewport={"width": 1920, "height": 1080},
                            java_script_enabled=True,
                            bypass_csp=True,
                            proxy=proxy,
                            extra_http_headers={
                                "Accept-Language": "en-US,en;q=0.9",
                                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                                "Connection": "keep-alive",
                                "Cache-Control": "no-cache",
                            }
                        )
                        page = await context.new_page()

                        domain = item_link.split('/')[2]
                        if domain in self.main_scraper.rate_limit_delays:
                            await asyncio.sleep(self.main_scraper.rate_limit_delays[domain])

                        response = await page.goto(item_link, timeout=20000, wait_until="domcontentloaded")
                        if response and response.status == 429:
                            print(f"Rate limit hit (429) for {item_link}, attempt {attempt + 1}/{retries}")
                            delay = 60 * (1.5 ** attempt)
                            self.main_scraper.rate_limit_delays[domain] = delay
                            await asyncio.sleep(delay)
                            continue
                        elif response and response.status >= 500:
                            print(f"Server error ({response.status}) for {item_link}, attempt {attempt + 1}/{retries}")
                            await asyncio.sleep(10 * (1.5 ** attempt))
                            continue

                        await page.wait_for_load_state("networkidle", timeout=20000)

                        anti_bot = await page.query_selector('//h1[contains(text(), "Access Denied")] | //div[contains(text(), "Please verify you are not a robot")]')
                        if anti_bot:
                            print(f"Anti-bot page detected for {item_link}")
                            return {
                                "item_price": "N/A",
                                "item_old_price": None,
                                "item_offer": None,
                                "item_description": "Anti-bot page detected",
                                "item_delivery_time_range": "N/A",
                                "item_images": []
                            }

                        critical_selectors = [
                            '[data-testid="product-price"]',
                            '[class*="price"]',
                            '[data-testid="item-image"]',
                            '[data-testid="item-description"]',
                        ]
                        critical_element = None
                        for selector in critical_selectors:
                            try:
                                critical_element = await page.wait_for_selector(selector, timeout=10000)
                                if critical_element:
                                    print(f"Found critical element with selector: {selector}")
                                    break
                            except PlaywrightTimeoutError:
                                print(f"Selector {selector} timed out")

                        if not critical_element:
                            print(f"No critical elements found on {item_link}")
                            html_content = await page.content()
                            debug_file = f"debug_item_page_{item_link.split('/')[-1]}.html"
                            with open(debug_file, "w", encoding="utf-8") as f:
                                f.write(html_content)
                            screenshot_file = f"debug_screenshot_{item_link.split('/')[-1]}.png"
                            await page.screenshot(path=screenshot_file, full_page=True)
                            print(f"Saved debug artifacts for {item_link}")
                            return {
                                "item_price": "N/A",
                                "item_old_price": None,
                                "item_offer": None,
                                "item_description": "No critical elements found",
                                "item_delivery_time_range": "N/A",
                                "item_images": []
                            }

                        item_price = await page.evaluate("""
                            () => {
                                const selectors = [
                                    '[data-testid="product-price"]',
                                    '[class*="price"]',
                                    '.currency',
                                    '[class*="amount"]'
                                ];
                                for (let sel of selectors) {
                                    const el = document.querySelector(sel);
                                    if (el?.innerText.trim()) return el.innerText.trim();
                                }
                                return "N/A";
                            }
                        """)
                        print(f"Item price: {item_price}")

                        item_old_price = await page.evaluate("""
                            () => {
                                const selectors = [
                                    '[class*="old-price"]',
                                    '.price-strike',
                                    '[data-testid="original-price"]'
                                ];
                                for (let sel of selectors) {
                                    const el = document.querySelector(sel);
                                    if (el?.innerText.trim()) return el.innerText.trim();
                                }
                                return null;
                            }
                        """)
                        print(f"Item old price: {item_old_price}")

                        item_offer = await page.evaluate("""
                            () => {
                                const selectors = [
                                    '[data-testid="offer-tag"]',
                                    '[class*="offer"]',
                                    '.promotion'
                                ];
                                for (let sel of selectors) {
                                    const el = document.querySelector(sel);
                                    if (el?.innerText.trim()) return el.innerText.trim();
                                }
                                return null;
                            }
                        """)
                        print(f"Item offer: {item_offer}")

                        item_description = await page.evaluate("""
                            () => {
                                const selectors = [
                                    '[data-testid="item-description"]',
                                    '[class*="description"]',
                                    '.product-details p'
                                ];
                                for (let sel of selectors) {
                                    const el = document.querySelector(sel);
                                    if (el?.innerText.trim()) return el.innerText.trim();
                                }
                                return "N/A";
                            }
                        """)
                        print(f"Item description: {item_description}")

                        delivery_time = await page.evaluate("""
                            () => {
                                const selectors = [
                                    '[data-testid="delivery-tag"] span',
                                    '[class*="delivery-time"]',
                                    '.delivery-info span'
                                ];
                                for (let sel of selectors) {
                                    const el = document.querySelector(sel);
                                    if (el?.innerText.trim()) return el.innerText.trim();
                                }
                                return "N/A";
                            }
                        """)
                        print(f"Delivery time range: {delivery_time}")

                        item_images = await page.evaluate("""
                            () => {
                                const selectors = [
                                    '[data-testid="item-image"] img',
                                    'img[class*="product-image"]',
                                    'img[alt*="product"]'
                                ];
                                let images = [];
                                for (let sel of selectors) {
                                    const imgs = document.querySelectorAll(sel);
                                    imgs.forEach(img => {
                                        if (img.src && !images.includes(img.src)) images.push(img.src);
                                    });
                                    if (images.length) break;
                                }
                                return images;
                            }
                        """)
                        print(f"Item images: {item_images}")

                        if item_price == "N/A" and item_description == "N/A" and not item_images:
                            print(f"Critical data missing for {item_link}, likely rate limited")
                            return {
                                "item_price": "N/A",
                                "item_old_price": None,
                                "item_offer": None,
                                "item_description": "Failed due to rate limiting",
                                "item_delivery_time_range": "N/A",
                                "item_images": []
                            }

                        await page.close()
                        await context.close()
                        return {
                            "item_price": item_price,
                            "item_old_price": item_old_price,
                            "item_offer": item_offer,
                            "item_description": item_description,
                            "item_delivery_time_range": delivery_time,
                            "item_images": item_images
                        }
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error extracting item details for {item_link}: {e}")
                    if page:
                        try:
                            html_content = await page.content()
                            debug_file = f"debug_item_page_{item_link.split('/')[-1]}.html"
                            with open(debug_file, "w", encoding="utf-8") as f:
                                f.write(html_content)
                            screenshot_file = f"debug_screenshot_{item_link.split('/')[-1]}.png"
                            await page.screenshot(path=screenshot_file, full_page=True)
                            print(f"Saved debug artifacts for {item_link}")
                        except Exception as debug_e:
                            print(f"Failed to save debug artifacts: {debug_e}")
                except Exception as e:
                    print(f"Unexpected error extracting item details for {item_link}: {e}")
                finally:
                    if page:
                        await page.close()
                    if context:
                        await context.close()
                    self.main_scraper.active_contexts -= 1
                    await asyncio.sleep(random.uniform(5, 10))
                    if attempt < retries - 1:
                        print(f"Attempt {attempt + 1}/{retries}, retrying...")
            print(f"Failed to extract details for {item_link} after all retries")
            return {
                "item_price": "N/A",
                "item_old_price": None,
                "item_offer": None,
                "item_description": "Failed due to rate limiting or timeout",
                "item_delivery_time_range": "N/A",
                "item_images": []
            }

    async def extract_all_items_from_sub_category(self, sub_category_link):
        """Extract all items from a sub-category with proxy rotation."""
        print(f"Attempting to extract all items from sub-category: {sub_category_link}")
        async with self.main_scraper.semaphore:
            retries = 3
            context = None
            sub_page = None

            for attempt in range(retries):
                try:
                    async with self.main_scraper.context_semaphore:
                        self.main_scraper.active_contexts += 1
                        browser = await self.main_scraper.get_browser()
                        proxy = random.choice(self.main_scraper.proxies) if self.main_scraper.proxies else None
                        context = await browser.new_context(
                            user_agent=random.choice(self.main_scraper.user_agents),
                            viewport={"width": 1920, "height": 1080},
                            proxy=proxy
                        )
                        sub_page = await context.new_page()

                        domain = sub_category_link.split('/')[2]
                        if domain in self.main_scraper.rate_limit_delays:
                            await asyncio.sleep(self.main_scraper.rate_limit_delays[domain])

                        response = await sub_page.goto(sub_category_link, timeout=60000, wait_until="domcontentloaded")
                        if response and response.status == 429:
                            print(f"Rate limit hit (429) for {sub_category_link}, attempt {attempt + 1}/{retries}")
                            delay = 60 * (1.5 ** attempt)
                            self.main_scraper.rate_limit_delays[domain] = delay
                            await asyncio.sleep(delay)
                            continue
                        elif response and response.status >= 500:
                            print(f"Server error ({response.status}) for {sub_category_link}, attempt {attempt + 1}/{retries}")
                            await asyncio.sleep(10 * (1.5 ** attempt))
                            continue

                        await sub_page.wait_for_load_state("networkidle", timeout=60000)

                        empty_message = await sub_page.query_selector('//div[contains(text(), "No items") or contains(text(), "empty") or contains(@class, "no-results")]')
                        if empty_message:
                            print(f"Sub-category {sub_category_link} is empty")
                            await sub_page.close()
                            await context.close()
                            return []

                        item_container_selectors = [
                            '[data-testid="grocery-item-container"]',
                            '[class*="category-items-container"] [class*="col-"]',
                            '.category-items-container .col-8.col-sm-4'
                        ]
                        item_elements = None
                        for selector in item_container_selectors:
                            try:
                                await sub_page.wait_for_selector(selector, timeout=30000)
                                item_elements = await sub_page.query_selector_all(selector)
                                if item_elements:
                                    print(f"Found item containers using selector: {selector}")
                                    break
                            except PlaywrightTimeoutError:
                                print(f"Selector {selector} timed out")

                        if not item_elements:
                            print(f"No item containers found on {sub_category_link}")
                            html_content = await sub_page.content()
                            debug_file = f"debug_sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.html"
                            with open(debug_file, "w", encoding="utf-8") as f:
                                f.write(html_content)
                            screenshot_file = f"debug_screenshot_sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.png"
                            await sub_page.screenshot(path=screenshot_file, full_page=True)
                            print(f"Saved debug artifacts for {sub_category_link}")
                            await sub_page.close()
                            await context.close()
                            return []

                        html_content = await sub_page.content()
                        html_filename = f"sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.html"
                        with open(html_filename, "w", encoding="utf-8") as f:
                            f.write(html_content)
                        print(f"Saved sub-category HTML to {html_filename} for debugging")

                        pagination_element = await sub_page.query_selector('[class*="paginate-wrap"]')
                        total_pages = 1
                        if pagination_element:
                            page_numbers = await pagination_element.query_selector_all('li.paginate-li a')
                            total_pages = len(page_numbers) if page_numbers else 1
                        print(f"Found {total_pages} pages in this sub-category")

                        items = []
                        for page_number in range(1, total_pages + 1):
                            print(f"Processing page {page_number} of {total_pages}")
                            page_url = f"{sub_category_link}&page={page_number}" if page_number > 1 else sub_category_link
                            response = await sub_page.goto(page_url, timeout=60000, wait_until="domcontentloaded")
                            if response and response.status == 429:
                                print(f"Rate limit hit (429) for page {page_number}, attempt {attempt + 1}/{retries}")
                                delay = 60 * (1.5 ** attempt)
                                self.main_scraper.rate_limit_delays[domain] = delay
                                await asyncio.sleep(delay)
                                continue
                            await sub_page.wait_for_load_state("networkidle", timeout=60000)

                            item_link_elements = await sub_page.query_selector_all('[data-testid="grocery-item-link-nofollow"]')
                            print(f"Found {len(item_link_elements)} items on page {page_number}")

                            for i, element in enumerate(item_link_elements):
                                try:
                                    name_selectors = [
                                        '[data-testid="product-name"]',
                                        '[data-test="item-name"]',
                                        '[class*="product-title"]',
                                        'h3[class*="product-title"]'
                                    ]
                                    item_name = None
                                    for selector in name_selectors:
                                        item_name_element = await element.query_selector(selector)
                                        if item_name_element:
                                            item_name = await item_name_element.inner_text()
                                            if item_name and item_name.strip():
                                                invalid_names = ['currency', 'kiki', 'market', 'grocery', 'mahboula']
                                                if not any(invalid.lower() in item_name.lower() for invalid in invalid_names):
                                                    print(f"Item name: {item_name}")
                                                    break
                                                else:
                                                    item_name = None

                                    if not item_name or not item_name.strip():
                                        item_name = f"Unknown Item {i+1}"
                                        print(f"No valid item name found, using default: {item_name}")

                                    item_link = self.base_url + await element.get_attribute('href')
                                    print(f"Item link: {item_link}")

                                    item_details = await self.extract_item_details(item_link)
                                    items.append({
                                        "item_name": item_name.strip(),
                                        "item_link": item_link,
                                        **item_details
                                    })

                                    await asyncio.sleep(random.uniform(3, 6))  # Increased delay
                                except Exception as e:
                                    print(f"Error processing item {i+1}: {e}")
                                    logging.error(f"Error processing item {i+1} in {sub_category_link}: {e}")

                        await sub_page.close()
                        await context.close()
                        return items
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error extracting items from sub-category {sub_category_link}: {e}")
                    if sub_page:
                        try:
                            html_content = await sub_page.content()
                            debug_file = f"debug_sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.html"
                            with open(debug_file, "w", encoding="utf-8") as f:
                                f.write(html_content)
                            screenshot_file = f"debug_screenshot_sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.png"
                            await sub_page.screenshot(path=screenshot_file, full_page=True)
                            print(f"Saved debug artifacts for {sub_category_link}")
                        except Exception as debug_e:
                            print(f"Failed to save debug artifacts: {debug_e}")
                except Exception as e:
                    print(f"Unexpected error extracting items from sub-category {sub_category_link}: {e}")
                    logging.error(f"Unexpected error in sub-category {sub_category_link}: {e}")
                finally:
                    if sub_page:
                        await sub_page.close()
                    if context:
                        await context.close()
                    self.main_scraper.active_contexts -= 1
                    await asyncio.sleep(random.uniform(5, 10))
                    if attempt < retries - 1:
                        print(f"Attempt {attempt + 1}/{retries}, retrying...")
            print(f"Failed to extract items from sub-category {sub_category_link} after all retries")
            return []
            
    async def extract_categories(self, page):
        print(f"Processing grocery: {self.url}")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    response = await page.goto(self.url, timeout=240000, wait_until="domcontentloaded")
                    if response and response.status == 429:
                        print(f"Rate limit hit (429) for {self.url}, waiting before retry...")
                        retries -= 1
                        await asyncio.sleep(30)
                        continue
                    elif response and response.status >= 500:
                        print(f"Server error ({response.status}) for {self.url}, retrying...")
                        retries -= 1
                        await asyncio.sleep(10)
                        continue

                    await page.wait_for_load_state("networkidle", timeout=240000)
                    print("Page loaded successfully")

                    delivery_fees = await self.get_delivery_fees(page)
                    minimum_order = await self.get_minimum_order(page)
                    view_all_link = await self.get_general_link(page)

                    print(f"  Delivery fees: {delivery_fees}")
                    print(f"  Minimum order: {minimum_order}")

                    categories_data = {}
                    if view_all_link:
                        print(f"  Navigating to view all link: {view_all_link}")
                        context = await self.browser.new_context(
                            user_agent=random.choice(self.main_scraper.user_agents)
                        )
                        category_page = await context.new_page()
                        response = await category_page.goto(view_all_link, timeout=240000, wait_until="domcontentloaded")
                        if response and response.status == 429:
                            print(f"Rate limit hit (429) for {view_all_link}, waiting before retry...")
                            retries -= 1
                            await asyncio.sleep(30)
                            await category_page.close()
                            await context.close()
                            continue
                        elif response and response.status >= 500:
                            print(f"Server error ({response.status}) for {view_all_link}, retrying...")
                            retries -= 1
                            await asyncio.sleep(10)
                            await category_page.close()
                            await context.close()
                            continue

                        await category_page.wait_for_load_state("networkidle", timeout=240000)

                        category_names = await self.extract_category_names(category_page)
                        category_links = await self.extract_category_links(category_page)

                        print(f"  Found {len(category_names)} categories")

                        for name, link in zip(category_names, category_links):
                            print(f"  Rate limit hit (429) for {category_link}, waiting before retry...")
                            categories_data[name] = {
                                "category_link": link,
                                "sub_categories": []
                            }
                        await category_page.close()
                        await context.close()

                    return {
                        "delivery_fees": delivery_fees,
                        "minimum_order": minimum_order,
                        "categories": categories_data
                    }
                except PlaywrightTimeoutError as e:
                    print(f"Timeout error extracting categories: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    print(f"Unexpected error extracting categories: {e}")
                    retries -= 1
                    print(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return {"error": "Failed to extract categories after multiple attempts"}


class MainScraper:
    def __init__(self, area_name):
        self.area_name = area_name
        self.CURRENT_PROGRESS_FILE = f"current_progress_{area_name}.json"
        self.SCRAPED_PROGRESS_FILE = f"scraped_progress_{area_name}.json"
        self.output_dir = "output"
        self.github_token = os.environ.get('GITHUB_TOKEN')
        self.semaphore = asyncio.Semaphore(3)  # Reduced for less aggressive concurrency
        self.max_browsers = 2  # Reduced number of browser instances
        self.max_contexts_per_browser = 2  # Contexts per browser
        self.active_contexts = 0
        self.context_semaphore = asyncio.Semaphore(self.max_contexts_per_browser * self.max_browsers)
        self.browsers = []
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        ]
        # Webshare proxy configuration
        self.proxies = [
            {
                "server": "http://p.webshare.io:80",
                "username": "ocdjzatk-rotate",
                "password": "7m26xglp08ta"
            }
        ]
        self.rate_limit_delays = {}  # Track rate limit delays per domain
        credentials_json = os.environ.get('TALABAT_GCLOUD_KEY_JSON')
        os.makedirs(self.output_dir, exist_ok=True)
        self.current_progress = self.load_current_progress()
        self.scraped_progress = self.load_scraped_progress()
        self.drive_uploader = SavingOnDrive(credentials_json=credentials_json if credentials_json else None)
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.ensure_playwright_browsers()
        self.save_current_progress()
        self.save_scraped_progress()
        self.commit_progress(f"Initialized progress files for {area_name}")

    async def check_server_status(self, url):
        """Check server status with improved proxy rotation, extended retries, and detailed logging."""
        async with aiohttp.ClientSession() as session:
            max_retries = 6  # Increased from 5 to 6
            used_proxies = []
            proxies = self.proxies.copy() if self.proxies else []
            domain = url.split('/')[2]
    
            # Filter out placeholder or invalid proxies
            valid_proxies = [
                p for p in proxies
                if p["server"] and not p["server"].startswith("http://proxy") and p["username"] and p["password"]
            ]
    
            if not valid_proxies:
                logging.warning(f"No valid proxies configured for {url}, attempting request without proxy")
                for attempt in range(max_retries):
                    try:
                        async with session.get(url, timeout=20) as response:
                            logging.info(f"Non-proxy attempt {attempt + 1}/{max_retries} for {url}: status {response.status}")
                            if response.status == 429:
                                delay = 120 * (2 ** attempt)  # Increased base delay from 60 to 120
                                self.rate_limit_delays[domain] = min(delay, 1800)  # Cap at 30 minutes
                                logging.info(f"Rate limit hit (429) for {url}, attempt {attempt + 1}/{max_retries}, waiting {delay}s")
                                await asyncio.sleep(delay)
                                continue
                            elif response.status >= 500:
                                logging.warning(f"Server error ({response.status}) for {url}, attempt {attempt + 1}/{max_retries}")
                                await asyncio.sleep(15 * (2 ** attempt))
                                continue
                            elif response.status >= 400:
                                logging.error(f"Client error ({response.status}) for {url}, attempt {attempt + 1}/{max_retries}")
                                return False
                            logging.info(f"Server check successful for {url} without proxy: status {response.status}")
                            return response.status < 400
                    except Exception as e:
                        logging.warning(f"Non-proxy attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                        await asyncio.sleep(15 * (2 ** attempt))
                logging.error(f"Server check failed for {url} after {max_retries} non-proxy attempts")
                return False
    
            for attempt in range(max_retries):
                if not valid_proxies:
                    logging.warning(f"No more proxies available for {url}, switching to non-proxy attempt")
                    break
    
                proxy = random.choice(valid_proxies)
                used_proxies.append(proxy)
                valid_proxies.remove(proxy)
    
                try:
                    async with session.get(
                        url,
                        timeout=20,
                        proxy=proxy["server"],
                        proxy_auth=aiohttp.BasicAuth(proxy["username"], proxy["password"])
                    ) as response:
                        logging.info(f"Proxy attempt {attempt + 1}/{max_retries} with {proxy['server']} for {url}: status {response.status}")
                        if response.status == 429:
                            delay = 120 * (2 ** attempt)  # Increased base delay
                            self.rate_limit_delays[domain] = min(delay, 1800)
                            logging.info(f"Rate limit hit (429) for {url} with proxy {proxy['server']}, attempt {attempt + 1}/{max_retries}, waiting {delay}s")
                            await asyncio.sleep(delay)
                            continue
                        elif response.status >= 500:
                            logging.warning(f"Server error ({response.status}) for {url} with proxy {proxy['server']}, attempt {attempt + 1}/{max_retries}")
                            await asyncio.sleep(15 * (2 ** attempt))
                            continue
                        elif response.status >= 400:
                            logging.error(f"Client error ({response.status}) for {url} with proxy {proxy['server']}, attempt {attempt + 1}/{max_retries}")
                            return False
                        logging.info(f"Server check successful for {url} with proxy {proxy['server']}: status {response.status}")
                        return response.status < 400
                except Exception as e:
                    logging.warning(f"Proxy attempt {attempt + 1}/{max_retries} with {proxy['server']} failed for {url}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(15 * (2 ** attempt))
    
            # Fallback to non-proxy attempt if all proxies fail
            logging.warning(f"All proxies failed for {url}, attempting without proxy")
            for attempt in range(max_retries):
                try:
                    async with session.get(url, timeout=20) as response:
                        logging.info(f"Non-proxy fallback attempt {attempt + 1}/{max_retries} for {url}: status {response.status}")
                        if response.status == 429:
                            delay = 120 * (2 ** attempt)
                            self.rate_limit_delays[domain] = min(delay, 1800)
                            logging.info(f"Rate limit hit (429) for {url} without proxy, attempt {attempt + 1}/{max_retries}, waiting {delay}s")
                            await asyncio.sleep(delay)
                            continue
                        elif response.status >= 500:
                            logging.warning(f"Server error ({response.status}) for {url} without proxy, attempt {attempt + 1}/{max_retries}")
                            await asyncio.sleep(15 * (2 ** attempt))
                            continue
                        elif response.status >= 400:
                            logging.error(f"Client error ({response.status}) for {url} without proxy, attempt {attempt + 1}/{max_retries}")
                            return False
                        logging.info(f"Server check successful for {url} without proxy: status {response.status}")
                        return response.status < 400
                except Exception as e:
                    logging.warning(f"Non-proxy fallback attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                    await asyncio.sleep(15 * (2 ** attempt))
            logging.error(f"Server check failed for {url} after {max_retries} attempts with and without proxies")
            return False
        
    def reset_area_progress(self, area_name: str):
        """Reset progress for an area to start scraping from the beginning."""
        self.current_progress["current_progress"] = {
            "area_name": area_name,
            "current_grocery": 0,
            "current_grocery_title": None,
            "current_grocery_link": None,
            "total_groceries": 0,
            "processed_groceries": [],
            "current_category": None,
            "current_sub_category": None,
            "completed_groceries": {}
        }
        self.scraped_progress["current_progress"] = self.current_progress["current_progress"].copy()
        self.scraped_progress["all_results"][area_name] = {}
        if area_name in self.current_progress["completed_areas"]:
            self.current_progress["completed_areas"].remove(area_name)
        if area_name in self.scraped_progress["completed_areas"]:
            self.scraped_progress["completed_areas"].remove(area_name)
        self.current_progress["last_updated"] = datetime.now().isoformat()
        self.scraped_progress["last_updated"] = datetime.now().isoformat()
        self.save_current_progress()
        self.save_scraped_progress()
        self.commit_progress(f"Reset progress for {area_name} to re-scrape")
        logging.info(f"Reset progress for area: {area_name}")

    async def initialize_browsers(self, playwright):
        """Initialize a pool of browser instances."""
        for _ in range(self.max_browsers):
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--window-size=1920,1080",
                ]
            )
            self.browsers.append(browser)
        logging.info(f"Initialized {len(self.browsers)} browser instances")

    async def get_browser(self):
        """Get a random browser from the pool, rotating to balance load."""
        if not self.browsers:
            raise RuntimeError("No browsers initialized")
        return random.choice(self.browsers)

    async def close_browsers(self):
        """Close all browser instances."""
        for browser in self.browsers:
            await browser.close()
        self.browsers.clear()
        logging.info("Closed all browser instances")

    def ensure_playwright_browsers(self):
        """Ensure Playwright browsers are installed."""
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)

    def upload_to_drive(self, file_path: str) -> bool:
        """Upload a file to Google Drive."""
        try:
            if not self.drive_uploader or not hasattr(self.drive_uploader, 'upload_file'):
                logging.warning(f"Drive uploader not initialized properly, skipping upload of {file_path}")
                return False
            folder_id = os.environ.get('GOOGLE_DRIVE_FOLDER_ID', 'root')
            file_id = self.drive_uploader.upload_file(file_path, folder_id)
            if file_id:
                logging.info(f"Successfully uploaded {file_path} to Google Drive with file ID: {file_id}")
                return True
            else:
                logging.warning(f"Failed to upload {file_path} to Google Drive")
                return False
        except Exception as e:
            logging.error(f"Error uploading {file_path} to Google Drive: {e}")
            return False

    def save_current_progress(self, progress: Dict = None):
        """Save current progress to file."""
        progress = progress or self.current_progress
        try:
            progress["last_updated"] = datetime.now().isoformat()
            if "current_progress" in progress:
                progress["current_progress"]["processed_groceries"] = list(set(progress["current_progress"].get("processed_groceries", [])))
                progress["completed_areas"] = list(set(progress.get("completed_areas", [])))
            with tempfile.NamedTemporaryFile('w', delete=False, dir='.', encoding='utf-8') as temp_file:
                json.dump(progress, temp_file, indent=2, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_filename = temp_file.name
            os.replace(temp_filename, self.CURRENT_PROGRESS_FILE)
            logging.info(f"Saved {self.CURRENT_PROGRESS_FILE}")
        except Exception as e:
            logging.error(f"Error saving {self.CURRENT_PROGRESS_FILE}: {e}")

    def save_scraped_progress(self, progress: Dict = None):
        """Save scraped progress to file."""
        progress = progress or self.scraped_progress
        try:
            progress["last_updated"] = datetime.now().isoformat()
            if "current_progress" in progress:
                progress["current_progress"]["processed_groceries"] = list(set(progress["current_progress"].get("processed_groceries", [])))
                progress["completed_areas"] = list(set(progress.get("completed_areas", [])))
            with tempfile.NamedTemporaryFile('w', delete=False, dir='.', encoding='utf-8') as temp_file:
                json.dump(progress, temp_file, indent=2, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_filename = temp_file.name
            os.replace(temp_filename, self.SCRAPED_PROGRESS_FILE)
            logging.info(f"Saved {self.SCRAPED_PROGRESS_FILE}")
        except Exception as e:
            logging.error(f"Error saving {self.SCRAPED_PROGRESS_FILE}: {e}")

    # def load_current_progress(self) -> Dict:
    #     """Load current progress from file."""
    #     if os.path.exists(self.CURRENT_PROGRESS_FILE):
    #         try:
    #             with open(self.CURRENT_PROGRESS_FILE, 'r', encoding='utf-8') as f:
    #                 progress = json.load(f)
    #             if "current_progress" not in progress:
    #                 progress["current_progress"] = {}
    #             current = progress["current_progress"]
    #             current.setdefault("processed_groceries", [])
    #             current.setdefault("completed_groceries", {})
    #             current.setdefault("area_name", self.area_name)
    #             current.setdefault("current_grocery", 0)
    #             current.setdefault("current_grocery_title", None)
    #             current.setdefault("current_grocery_link", None)
    #             current.setdefault("current_category", None)
    #             current.setdefault("current_sub_category", None)
    #             current.setdefault("total_groceries", 0)
    #             current["processed_groceries"] = list(set(current["processed_groceries"]))
    #             progress["completed_areas"] = list(set(progress.get("completed_areas", [])))
    #             progress["last_updated"] = progress.get("last_updated", datetime.now().isoformat())
    #             logging.info(f"Loaded {self.CURRENT_PROGRESS_FILE}")
    #             return progress
    #         except Exception as e:
    #             logging.error(f"Error loading {self.CURRENT_PROGRESS_FILE}: {e}")

    #     default_progress = {
    #         "completed_areas": [],
    #         "current_area_index": 0,
    #         "last_updated": datetime.now().isoformat(),
    #         "current_progress": {
    #             "area_name": self.area_name,
    #             "current_grocery": 0,
    #             "current_grocery_title": None,
    #             "current_grocery_link": None,
    #             "total_groceries": 0,
    #             "processed_groceries": [],
    #             "current_category": None,
    #             "current_sub_category": None,
    #             "completed_groceries": {}
    #         }
    #     }
    #     logging.info(f"{self.CURRENT_PROGRESS_FILE} not found, creating default")
    #     self.save_current_progress(default_progress)
    #     return default_progress

    def load_current_progress(self):
        """Load current progress from file, initializing with default structure."""
        try:
            if os.path.exists(self.CURRENT_PROGRESS_FILE):
                with open(self.CURRENT_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                    # Ensure required keys are present
                    if "current_progress" not in progress:
                        progress["current_progress"] = {}
                    if "completed_areas" not in progress["current_progress"]:
                        progress["current_progress"]["completed_areas"] = []
                    if "completed_groceries" not in progress["current_progress"]:
                        progress["current_progress"]["completed_groceries"] = {}
                    return progress
            logging.info(f"No existing progress file found for {self.area_name}, creating new one")
            return {
                "current_progress": {
                    "area_name": self.area_name,
                    "completed_areas": [],
                    "completed_groceries": {},
                    "current_category": None,
                    "current_sub_category": None
                }
            }
        except Exception as e:
            logging.error(f"Error loading current progress: {e}")
            return {
                "current_progress": {
                    "area_name": self.area_name,
                    "completed_areas": [],
                    "completed_groceries": {},
                    "current_category": None,
                    "current_sub_category": None
                }
            }

    def load_scraped_progress(self) -> Dict:
        """Load scraped progress from file."""
        if os.path.exists(self.SCRAPED_PROGRESS_FILE):
            try:
                with open(self.SCRAPED_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                if "current_progress" not in progress:
                    progress["current_progress"] = {}
                if "all_results" not in progress:
                    progress["all_results"] = {}
                current = progress["current_progress"]
                current.setdefault("processed_groceries", [])
                current.setdefault("completed_groceries", {})
                current.setdefault("area_name", self.area_name)
                current.setdefault("current_grocery", 0)
                current.setdefault("current_grocery_title", None)
                current.setdefault("current_grocery_link", None)
                current.setdefault("current_category", None)
                current.setdefault("current_sub_category", None)
                current.setdefault("total_groceries", 0)
                current["processed_groceries"] = list(set(current["processed_groceries"]))
                progress["completed_areas"] = list(set(progress.get("completed_areas", [])))
                progress["last_updated"] = progress.get("last_updated", datetime.now().isoformat())
                logging.info(f"Loaded {self.SCRAPED_PROGRESS_FILE}")
                return progress
            except Exception as e:
                logging.error(f"Error loading {self.SCRAPED_PROGRESS_FILE}: {e}")

        default_progress = {
            "completed_areas": [],
            "current_area_index": 0,
            "last_updated": datetime.now().isoformat(),
            "current_progress": {
                "area_name": self.area_name,
                "current_grocery": 0,
                "current_grocery_title": None,
                "current_grocery_link": None,
                "total_groceries": 0,
                "processed_groceries": [],
                "current_category": None,
                "current_sub_category": None,
                "completed_groceries": {}
            },
            "all_results": {}
        }
        logging.info(f"{self.SCRAPED_PROGRESS_FILE} not found, creating default")
        self.save_scraped_progress(default_progress)
        return default_progress

    def commit_progress(self, message: str):
        """Commit progress files to Git repository."""
        if not self.github_token:
            logging.info("Skipping git commit due to missing GITHUB_TOKEN")
            return

        try:
            for file_path in [self.CURRENT_PROGRESS_FILE, self.SCRAPED_PROGRESS_FILE]:
                if not os.path.exists(file_path):
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump({}, f)

            subprocess.run(["git", "add", self.CURRENT_PROGRESS_FILE, self.SCRAPED_PROGRESS_FILE, self.output_dir], check=True)
            result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, check=True)
            if not result.stdout.strip():
                logging.info("No changes to commit")
                return

            subprocess.run(["git", "config", "--global", "user.name", "GitHub Action"], check=True)
            subprocess.run(["git", "config", "--global", "user.email", "action@github.com"], check=True)
            subprocess.run(["git", "commit", "-m", message], check=True)

            for attempt in range(3):
                try:
                    try:
                        subprocess.run(["git", "pull", "--rebase", "origin", "master"], check=True)
                    except subprocess.CalledProcessError as rebase_error:
                        logging.warning(f"Rebase failed: {rebase_error}")
                        conflict_check = subprocess.run(["git", "status"], capture_output=True, text=True, check=False)
                        if "both modified" in conflict_check.stdout.lower() or "unmerged" in conflict_check.stdout.lower():
                            logging.warning("Merge conflicts detected, resetting to origin/master")
                            subprocess.run(["git", "rebase", "--abort"], check=False)
                            subprocess.run(["git", "fetch", "origin"], check=True)
                            subprocess.run(["git", "reset", "--hard", "origin/master"], check=True)
                            subprocess.run(["git", "add", self.CURRENT_PROGRESS_FILE, self.SCRAPED_PROGRESS_FILE, self.output_dir], check=True)
                            result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, check=True)
                            if result.stdout.strip():
                                subprocess.run(["git", "commit", "-m", message], check=True)
                            else:
                                logging.info("No changes to commit after conflict resolution")
                                return

                    subprocess.run(["git", "push", "origin", "master"], check=True)
                    logging.info(f"Successfully committed and pushed: {message}")
                    break
                except subprocess.CalledProcessError as e:
                    logging.warning(f"Git operation failed (attempt {attempt + 1}/3): {e}")
                    if attempt == 2:
                        logging.error("Failed to push after 3 attempts, aborting")
                        raise
                    asyncio.get_event_loop().run_until_complete(asyncio.sleep(2))
        except subprocess.CalledProcessError as e:
            error_output = e.stderr or str(e)
            if "nothing to commit" in error_output.lower() or "no changes added" in error_output.lower():
                logging.info("No changes to commit")
            else:
                logging.error(f"Error committing progress: {e}")
                raise

    async def get_page_groceries(self, page):
        """Extract grocery information from a page."""
        groceries = []
        try:
            await page.wait_for_selector('[data-testid="grocery-card"]', timeout=30000)
            grocery_elements = await page.query_selector_all('[data-testid="grocery-card"]')
            for element in grocery_elements:
                title_element = await element.query_selector('[data-testid="grocery-title"]')
                link_element = await element.query_selector('a')
                delivery_time_element = await element.query_selector('[data-testid="delivery-time"]')
                
                title = await title_element.inner_text() if title_element else "Unknown"
                link = await link_element.get_attribute('href') if link_element else ""
                link = f"https://www.talabat.com{link}" if link else ""
                delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"
                
                groceries.append({
                    "grocery_title": title.strip(),
                    "grocery_link": link,
                    "delivery_time": delivery_time.strip()
                })
            logging.info(f"Found {len(groceries)} groceries on page")
            return groceries
        except Exception as e:
            logging.error(f"Error extracting groceries from page: {e}")
            return []

    async def process_grocery_categories(self, grocery_title, grocery_details, talabat_grocery, page, groceries_on_page, grocery_idx):
        """Process categories for a grocery."""
        completed_groceries = self.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {})
        completed_categories = completed_groceries.get("completed categories", [])
        current_category = self.current_progress["current_progress"].get("current_category")
        current_sub_category = self.current_progress["current_progress"].get("current_sub_category")
        categories = grocery_details.get("categories", {})

        if not categories:
            print(f"No categories found for {grocery_title}, marking as complete")
            self.current_progress["current_progress"]["processed_groceries"].append(grocery_title)
            self.scraped_progress["current_progress"]["processed_groceries"].append(grocery_title)
            self.update_to_next_grocery(groceries_on_page, grocery_idx)
            self.save_current_progress()
            self.save_scraped_progress()
            self.commit_progress(f"No categories for {grocery_title}, marked as complete")
            return

        start_processing = not current_category or not current_sub_category
        category_names = list(categories.keys())

        if current_sub_category and current_category:
            found = False
            for category_name in category_names:
                sub_categories = await talabat_grocery.extract_sub_categories(page, categories[category_name]["category_link"], grocery_title, category_name)
                sub_category_names = [sub["sub_category_name"] for sub in sub_categories]
                if current_sub_category in sub_category_names:
                    self.current_progress["current_progress"]["current_category"] = category_name
                    self.scraped_progress["current_progress"]["current_category"] = category_name
                    self.save_current_progress()
                    self.save_scraped_progress()
                    found = True
                    break
            if not found:
                print(f"Warning: Current sub-category {current_sub_category} not found in any category, resetting current_category")
                self.current_progress["current_progress"]["current_category"] = None
                self.scraped_progress["current_progress"]["current_category"] = None
                start_processing = True

        for idx, category_name in enumerate(category_names):
            if category_name in completed_categories:
                print(f"Category {category_name} already completed, skipping")
                continue

            if current_category and not start_processing:
                if category_name == current_category:
                    start_processing = True
                else:
                    print(f"Skipping category {category_name}, waiting for {current_category}")
                    continue

            print(f"Processing category {idx + 1}/{len(category_names)}: {category_name}")
            self.current_progress["current_progress"]["current_category"] = category_name
            self.scraped_progress["current_progress"]["current_category"] = category_name
            self.save_current_progress()
            self.save_scraped_progress()

            temp_page = await self.browser.new_page()
            await self.process_category(grocery_title, categories[category_name], category_name, talabat_grocery, temp_page)
            await temp_page.close()

            completed_groceries = self.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {})
            if category_name in completed_groceries.get("completed categories", []):
                self.move_to_next_category(category_names, idx, grocery_title, completed_categories)

        await self.verify_and_scrape_missing_sub_categories(grocery_title, grocery_details, talabat_grocery, page)

        completed_groceries = self.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {})
        if all(cat in completed_groceries.get("completed categories", []) for cat in category_names):
            self.current_progress["current_progress"]["processed_groceries"].append(grocery_title)
            self.scraped_progress["current_progress"]["processed_groceries"].append(grocery_title)
            self.update_to_next_grocery(groceries_on_page, grocery_idx)
            self.save_current_progress()
            self.save_scraped_progress()
            self.commit_progress(f"Completed all categories for {grocery_title}")

    async def process_category(self, grocery_title, category_data, category_name, talabat_grocery, page):
        """Process a single category."""
        sub_categories = await talabat_grocery.extract_sub_categories(page, category_data["category_link"], grocery_title, category_name)
        category_data["sub_categories"] = sub_categories
        self.save_scraped_progress()
        self.commit_progress(f"Processed category {category_name} for {grocery_title}")

    async def verify_and_scrape_missing_sub_categories(self, grocery_title, grocery_details, talabat_grocery, page):
        """Verify and scrape missing sub-categories."""
        print(f"Verifying sub-categories for grocery: {grocery_title}")
        area_name = self.current_progress["current_progress"]["area_name"]
        completed_groceries = self.current_progress["current_progress"]["completed_groceries"].setdefault(grocery_title, {})
        completed_sub_categories = completed_groceries.get("completed sub-categories", [])

        for category_name, category_data in grocery_details.get("categories", {}).items():
            category_link = category_data["category_link"]
            print(f"Checking category: {category_name}")

            missing_sub_categories = await talabat_grocery.verify_sub_categories(page, category_link, grocery_title, category_name)
            if missing_sub_categories:
                print(f"Found {len(missing_sub_categories)} missing sub-categories in {category_name}")
                for missing_sub in missing_sub_categories:
                    sub_category_name = missing_sub["sub_category_name"]
                    sub_category_link = missing_sub["sub_category_link"]
                    print(f"Scraping missing sub-category: {sub_category_name}")
                    self.current_progress["current_progress"]["current_category"] = category_name
                    self.current_progress["current_progress"]["current_sub_category"] = sub_category_name
                    self.scraped_progress["current_progress"]["current_category"] = category_name
                    self.scraped_progress["current_progress"]["current_sub_category"] = sub_category_name
                    self.save_current_progress()
                    self.save_scraped_progress()
                    items = await talabat_grocery.extract_all_items_from_sub_category(sub_category_link)
                    sub_category_data = {
                        "sub_category_name": sub_category_name,
                        "sub_category_link": sub_category_link,
                        "items": items
                    }

                    grocery_details["categories"][category_name]["sub_categories"].append(sub_category_data)

                    completed_sub_categories.append(sub_category_name)
                    completed_groceries["completed sub-categories"] = completed_sub_categories
                    self.current_progress["current_progress"]["completed_groceries"][grocery_title] = completed_groceries
                    self.scraped_progress["current_progress"]["completed_groceries"] = self.current_progress["current_progress"]["completed_groceries"]

                    grocery_data = self.scraped_progress["all_results"].setdefault(area_name, {}).setdefault(grocery_title, {
                        "grocery_link": talabat_grocery.url,
                        "delivery_time": "N/A",
                        "grocery_details": grocery_details
                    })
                    grocery_data["grocery_details"] = grocery_details
                    self.scraped_progress["all_results"][area_name][grocery_title] = grocery_data

                    self.current_progress["current_progress"]["current_sub_category"] = None
                    self.scraped_progress["current_progress"]["current_sub_category"] = None
                    self.current_progress["current_progress"]["current_category"] = None
                    self.scraped_progress["current_progress"]["current_category"] = None
                    self.save_current_progress()
                    self.save_scraped_progress()
                    self.commit_progress(f"Scraped missing sub-category {sub_category_name} for {grocery_title} in {category_name}")

        json_filename = os.path.join(self.output_dir, f"{area_name}.json")
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(self.scraped_progress["all_results"].get(area_name, {}), f, indent=2, ensure_ascii=False)
        if self.upload_to_drive(json_filename):
            logging.info(f"Uploaded updated {json_filename} to Google Drive")
        else:
            logging.warning(f"Failed to upload updated {json_filename} to Google Drive")

        print(f"Waiting 30 seconds before updating Excel for {area_name}...")
        await asyncio.sleep(30)
        await self.convert_json_to_excel(area_name, json_filename)

    def move_to_next_category(self, category_names, current_idx, grocery_title, completed_categories):
        """Move to the next unprocessed category."""
        next_idx = current_idx + 1
        self.current_progress["current_progress"]["current_category"] = None
        self.scraped_progress["current_progress"]["current_category"] = None
        self.current_progress["current_progress"]["current_sub_category"] = None
        self.scraped_progress["current_progress"]["current_sub_category"] = None
        while next_idx < len(category_names):
            next_category = category_names[next_idx]
            if next_category not in completed_categories:
                self.current_progress["current_progress"]["current_category"] = next_category
                self.scraped_progress["current_progress"]["current_category"] = next_category
                break
            next_idx += 1
        self.save_current_progress()
        self.save_scraped_progress()
        self.commit_progress(f"Moved to next category after {category_names[current_idx]} for {grocery_title}")

    def update_to_next_grocery(self, groceries_on_page, current_idx):
        """Update progress to the next unprocessed grocery."""
        processed_grocery_titles = set(self.current_progress["current_progress"]["processed_groceries"])
        next_idx = current_idx + 1
        self.current_progress["current_progress"].update({
            "current_grocery": 0,
            "current_grocery_title": None,
            "current_grocery_link": None,
            "current_category": None,
            "current_sub_category": None
        })
        self.scraped_progress["current_progress"].update({
            "current_grocery": 0,
            "current_grocery_title": None,
            "current_grocery_link": None,
            "current_category": None,
            "current_sub_category": None
        })
        while next_idx < len(groceries_on_page):
            next_grocery = groceries_on_page[next_idx]
            if next_grocery["grocery_title"] not in processed_grocery_titles:
                self.current_progress["current_progress"].update({
                    "current_grocery": next_idx + 1,
                    "current_grocery_title": next_grocery["grocery_title"],
                    "current_grocery_link": next_grocery["grocery_link"]
                })
                self.scraped_progress["current_progress"].update({
                    "current_grocery": next_idx + 1,
                    "current_grocery_title": next_grocery["grocery_title"],
                    "current_grocery_link": next_grocery["grocery_link"]
                })
                break
            next_idx += 1
        self.save_current_progress()
        self.save_scraped_progress()
        self.commit_progress(f"Updated to next grocery after index {current_idx}")

    async def convert_json_to_excel(self, area_name: str, json_filename: str):
        """Convert JSON data to Excel format."""
        try:
            with open(json_filename, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not data:
                logging.warning(f"No data to write to Excel for area: {area_name}")
                return

            excel_filename = os.path.join(self.output_dir, f"{area_name}_detailed.xlsx")
            with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                for grocery_title, grocery_data in data.items():
                    sheet_name = re.sub(r'[\\\/:*?"<>|]', '_', grocery_title)[:31]
                    general_info = {
                        "Grocery Title": grocery_title,
                        "Delivery Time": grocery_data.get("delivery_time", "N/A"),
                        "Delivery Fees": grocery_data.get("grocery_details", {}).get("delivery_fees", "N/A"),
                        "Minimum Order": grocery_data.get("grocery_details", {}).get("minimum_order", "N/A"),
                        "URL": grocery_data.get("grocery_link", "N/A")
                    }
                    simplified_data = []
                    for category_name, category_data in grocery_data.get("grocery_details", {}).get("categories", {}).items():
                        for sub_category in category_data.get("sub_categories", []):
                            items_list = [
                                {
                                    "Item Name": item.get("item_name", "N/A"),
                                    "Item Price": item.get("item_price", "N/A"),
                                    "Item Old Price": item.get("item_old_price", None),
                                    "Item Offer": item.get("item_offer", None),
                                    "Item Description": item.get("item_description", "N/A"),
                                    "Item Link": item.get("item_link", "N/A")
                                }
                                for item in sub_category.get("items", [])
                            ]
                            items_json = json.dumps(items_list, ensure_ascii=False)
                            simplified_data.append({
                                **general_info,
                                "Category": category_name,
                                "Category Link": category_data.get("category_link", "N/A"),
                                "Sub-Category": sub_category.get("sub_category_name", "N/A"),
                                "Sub-Category Link": sub_category.get("sub_category_link", "N/A"),
                                "Items": items_json
                            })

                    if simplified_data:
                        df = pd.DataFrame(simplified_data)
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        logging.info(f"Added sheet '{sheet_name}' to Excel: {excel_filename}")
                    else:
                        logging.warning(f"No data for grocery '{grocery_title}' in area: {area_name}")

            logging.info(f"Successfully created Excel: {excel_filename}")
            if self.upload_to_drive(excel_filename):
                logging.info(f"Uploaded {excel_filename} to Google Drive")
                os.remove(json_filename)
                logging.info(f"Deleted {json_filename} after Excel upload")
            else:
                logging.warning(f"Failed to upload {excel_filename} to Google Drive")
        except Exception as e:
            logging.error(f"Error converting JSON to Excel for {area_name}: {e}")

    async def scrape_and_save_area(self, area_name: str, area_url: str, browser) -> List[Dict]:
        """Scrape a single area, delete JSON after Excel upload."""
        print(f"\n{'='*50}\nSCRAPING AREA: {area_name}\nURL: {area_url}\n{'='*50}")
        process = psutil.Process()
        logging.info(f"Memory usage before scrape: {process.memory_info().rss / 1024 / 1024:.2f} MB")
        logging.info(f"Active browser contexts: {self.active_contexts}")

        if not await self.check_server_status(area_url):
            print(f"Server at {area_url} is not responding or rate limited, aborting scrape.")
            return []

        all_area_results = self.scraped_progress["all_results"].get(area_name, {})
        current_progress = self.current_progress["current_progress"]
        scraped_current_progress = self.scraped_progress["current_progress"]

        if current_progress["area_name"] != area_name:
            current_progress.update({
                "area_name": area_name,
                "current_grocery": 0,
                "current_grocery_title": None,
                "current_grocery_link": None,
                "total_groceries": 0,
                "processed_groceries": [],
                "current_category": None,
                "current_sub_category": None,
                "completed_groceries": {}
            })
            scraped_current_progress.update(current_progress)
            self.save_current_progress()
            self.save_scraped_progress()
            self.commit_progress(f"Started scraping {area_name}")

        context = None
        page = None
        try:
            browser = await self.get_browser()
            proxy = random.choice(self.proxies) if self.proxies else None
            context = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={"width": 1920, "height": 1080},
                java_script_enabled=True,
                bypass_csp=True,
                proxy=proxy,
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Connection": "keep-alive",
                }
            )
            page = await context.new_page()
            response = await page.goto(area_url, timeout=60000, wait_until="domcontentloaded")
            if response and response.status == 429:
                print(f"Rate limit hit (429) for {area_url}, aborting scrape.")
                domain = area_url.split('/')[2]
                self.rate_limit_delays[domain] = self.rate_limit_delays.get(domain, 60) * 1.5
                return []
            elif response and response.status >= 500:
                print(f"Server error ({response.status}) for {area_url}, aborting scrape.")
                return []

            groceries_on_page = await self.get_page_groceries(page)
            current_progress["total_groceries"] = len(groceries_on_page)
            scraped_current_progress["total_groceries"] = len(groceries_on_page)
            print(f"Found {len(groceries_on_page)} groceries")
            await page.close()

            processed_grocery_titles = set(current_progress["processed_groceries"])
            current_grocery_title = current_progress.get("current_grocery_title")

            for grocery_idx, grocery in enumerate(groceries_on_page):
                grocery_num = grocery_idx + 1
                grocery_title = grocery["grocery_title"]
                grocery_link = grocery["grocery_link"]

                if grocery_title in processed_grocery_titles:
                    print(f"Skipping already processed grocery: {grocery_title}")
                    continue

                if current_grocery_title and current_grocery_title != grocery_title:
                    print(f"Skipping grocery {grocery_title}, waiting for {current_grocery_title}")
                    continue

                current_progress["current_grocery"] = grocery_num
                current_progress["current_grocery_title"] = grocery_title
                current_progress["current_grocery_link"] = grocery_link
                scraped_current_progress["current_grocery"] = grocery_num
                scraped_current_progress["current_grocery_title"] = grocery_title
                scraped_current_progress["current_grocery_link"] = grocery_link
                self.save_current_progress()
                self.save_scraped_progress()
                print(f"Processing grocery {grocery_num}/{len(groceries_on_page)}: {grocery_title} (link: {grocery_link})")

                async with self.context_semaphore:
                    self.active_contexts += 1
                    grocery_context = await browser.new_context(
                        user_agent=random.choice(self.user_agents),
                        proxy=random.choice(self.proxies) if self.proxies else None
                    )
                    grocery_page = await grocery_context.new_page()
                    talabat_grocery = TalabatGroceries(grocery_link, browser, self)
                    grocery_details = await talabat_grocery.extract_categories(grocery_page)
                    all_area_results[grocery_title] = {
                        "grocery_link": grocery_link,
                        "delivery_time": grocery["delivery_time"],
                        "grocery_details": grocery_details
                    }
                    self.scraped_progress["all_results"][area_name] = all_area_results
                    self.save_scraped_progress()

                    await self.process_grocery_categories(grocery_title, grocery_details, talabat_grocery, grocery_page, groceries_on_page, grocery_idx)
                    await grocery_page.close()
                    await grocery_context.close()
                    self.active_contexts -= 1
                    await asyncio.sleep(random.uniform(5, 10))  # Random delay to avoid detection

                if current_grocery_title == grocery_title:
                    break

            print(f"Verifying groceries for area: {area_name}")
            verify_context = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                proxy=random.choice(self.proxies) if self.proxies else None
            )
            page = await verify_context.new_page()
            await page.goto(area_url, timeout=60000)
            current_groceries = await self.get_page_groceries(page)
            await page.close()
            await verify_context.close()

            missing_groceries = [g for g in current_groceries if g["grocery_title"] not in processed_grocery_titles]
            if missing_groceries:
                print(f"Found {len(missing_groceries)} missing groceries in {area_name}")
                for grocery_idx, grocery in enumerate(missing_groceries):
                    grocery_num = len(groceries_on_page) + grocery_idx + 1
                    grocery_title = grocery["grocery_title"]
                    grocery_link = grocery["grocery_link"]
                    print(f"Processing missing grocery {grocery_num}: {grocery_title} (link: {grocery_link})")

                    current_progress["current_grocery"] = grocery_num
                    current_progress["current_grocery_title"] = grocery_title
                    current_progress["current_grocery_link"] = grocery_link
                    scraped_current_progress["current_grocery"] = grocery_num
                    scraped_current_progress["current_grocery_title"] = grocery_title
                    scraped_current_progress["current_grocery_link"] = grocery_link
                    self.save_current_progress()
                    self.save_scraped_progress()

                    async with self.context_semaphore:
                        self.active_contexts += 1
                        grocery_context = await browser.new_context(
                            user_agent=random.choice(self.user_agents),
                            proxy=random.choice(self.proxies) if self.proxies else None
                        )
                        grocery_page = await grocery_context.new_page()
                        talabat_grocery = TalabatGroceries(grocery_link, browser, self)
                        grocery_details = await talabat_grocery.extract_categories(grocery_page)
                        all_area_results[grocery_title] = {
                            "grocery_link": grocery_link,
                            "delivery_time": grocery["delivery_time"],
                            "grocery_details": grocery_details
                        }
                        self.scraped_progress["all_results"][area_name] = all_area_results
                        self.save_scraped_progress()

                        await self.process_grocery_categories(grocery_title, grocery_details, talabat_grocery, grocery_page, current_groceries, grocery_idx)
                        await grocery_page.close()
                        await grocery_context.close()
                        self.active_contexts -= 1
                        await asyncio.sleep(random.uniform(5, 10))

            if all(g["grocery_title"] in processed_grocery_titles for g in groceries_on_page):
                print(f"All groceries processed for area: {area_name}")
                self.current_progress["completed_areas"].append(area_name)
                self.scraped_progress["completed_areas"].append(area_name)
                self.current_progress["current_progress"].update({
                    "current_grocery": 0,
                    "current_grocery_title": None,
                    "current_grocery_link": None,
                    "total_groceries": 0,
                    "processed_groceries": [],
                    "current_category": None,
                    "current_sub_category": None,
                    "completed_groceries": {}
                })
                self.scraped_progress["current_progress"].update({
                    "current_grocery": 0,
                    "current_grocery_title": None,
                    "current_grocery_link": None,
                    "total_groceries": 0,
                    "processed_groceries": [],
                    "current_category": None,
                    "current_sub_category": None,
                    "completed_groceries": {}
                })
                self.save_current_progress()
                self.save_scraped_progress()
                self.commit_progress(f"Completed scraping area: {area_name}")

            json_filename = os.path.join(self.output_dir, f"{area_name}.json")
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(all_area_results, f, indent=2, ensure_ascii=False)
            if self.upload_to_drive(json_filename):
                logging.info(f"Uploaded {json_filename} to Google Drive")
            else:
                logging.warning(f"Failed to upload {json_filename} to Google Drive")

            print(f"Waiting 30 seconds before updating Excel for {area_name}...")
            await asyncio.sleep(30)
            await self.convert_json_to_excel(area_name, json_filename)

            return list(all_area_results.values())
        except Exception as e:
            logging.error(f"Error scraping area {area_name}: {e}")
            return []
        finally:
            if page:
                await page.close()
            if context:
                await context.close()
                
    # async def run(self, area_urls: List[Dict]):
    #     """Run the scraper for all provided areas, re-scraping completed areas."""
    #     async with async_playwright() as playwright:
    #         await self.initialize_browsers(playwright)
    #         try:
    #             batch_size = 5
    #             for i in range(0, len(area_urls), batch_size):
    #                 batch = area_urls[i:i + batch_size]
    #                 tasks = []
    #                 for area in batch:
    #                     area_name = area["area_name"]
    #                     area_url = area["url"]
    #                     # Check if the area is completed and reset if necessary
    #                     if area_name in self.current_progress["completed_areas"]:
    #                         print(f"Area {area_name} is marked as completed, resetting progress for re-scrape")
    #                         self.reset_area_progress(area_name)
    #                     tasks.append(self.scrape_and_save_area(area_name, area_url, None))
    #                 await asyncio.gather(*tasks, return_exceptions=True)
    #                 logging.info(f"Processed batch {i//batch_size + 1} of {len(area_urls)//batch_size + 1}")
    #                 await asyncio.sleep(random.uniform(5, 10))
    #         finally:
    #             await self.close_browsers()
    #             self.executor.shutdown(wait=True)
    #             logging.info("Scraper run completed")

    async def run(self, areas):
        """Run the scraper for the specified areas with enhanced logging and broader selectors."""
        for area in areas:
            area_name = area["area_name"]
            area_url = area["url"]
            print(f"==================================================")
            print(f"SCRAPING AREA: {area_name}")
            print(f"URL: {area_url}")
            print(f"==================================================")
            logging.info(f"Starting scrape for area: {area_name}, URL: {area_url}")
    
            # Check server status
            if not await self.check_server_status(area_url):
                print(f"Server at {area_url} is not responding or rate limited, aborting scrape.")
                logging.error(f"Server check failed for {area_url}, aborting scrape for {area_name}")
                continue
    
            # Initialize browser context
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                self.browsers.append(browser)
                context = await browser.new_context(
                    user_agent=random.choice(self.user_agents),
                    viewport={"width": 1920, "height": 1080}
                )
                page = await context.new_page()
    
                try:
                    # Navigate to area URL
                    logging.info(f"Navigating to area URL: {area_url}")
                    response = await page.goto(area_url, timeout=240000, wait_until="domcontentloaded")
                    if response and response.status == 429:
                        logging.error(f"Rate limit hit (429) for {area_url}")
                        print(f"Rate limit hit (429) for {area_url}, aborting.")
                        continue
                    elif response and response.status >= 500:
                        logging.error(f"Server error ({response.status}) for {area_url}")
                        print(f"Server error ({response.status}) for {area_url}, aborting.")
                        continue
                    elif response and response.status >= 400:
                        logging.error(f"Client error ({response.status}) for {area_url}")
                        print(f"Client error ({response.status}) for {area_url}, aborting.")
                        continue
    
                    await page.wait_for_load_state("networkidle", timeout=240000)
                    logging.info(f"Area page loaded successfully: {area_url}, status: {response.status}")
    
                    # Log page title and headers for debugging
                    page_title = await page.title()
                    headers = response.headers
                    logging.info(f"Page title: {page_title}")
                    logging.info(f"Response headers: {headers}")
    
                    # Check for anti-bot page
                    anti_bot = await page.query_selector('//h1[contains(text(), "Access Denied")] | //div[contains(text(), "Please verify you are not a robot")]')
                    if anti_bot:
                        logging.error(f"Anti-bot page detected for {area_url}")
                        print(f"Anti-bot page detected for {area_url}, aborting.")
                        # Save debug artifacts
                        html_content = await page.content()
                        debug_file = f"debug_area_{area_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                        with open(debug_file, "w", encoding="utf-8") as f:
                            f.write(html_content)
                        screenshot_file = f"debug_screenshot_{area_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                        await page.screenshot(path=screenshot_file, full_page=True)
                        logging.info(f"Saved debug artifacts for {area_name}: {debug_file}, {screenshot_file}")
                        continue
    
                    # Extract grocery links with broader selectors
                    selectors = [
                        '[data-testid="vendor-card"] a',
                        '.vendor-card a',
                        '[data-testid="grocery-vendor"] a',
                        '[class*="vendor"] a',
                        'a[href*="/kuwait/groceries/"]'
                    ]
                    grocery_elements = []
                    for selector in selectors:
                        elements = await page.query_selector_all(selector)
                        if elements:
                            grocery_elements = elements
                            logging.info(f"Found grocery elements with selector: {selector}")
                            break
                    grocery_links = [await el.get_attribute('href') for el in grocery_elements]
                    grocery_links = [self.base_url + link for link in grocery_links if link and link.startswith('/')]
                    logging.info(f"Found {len(grocery_links)} grocery links: {grocery_links}")
                    print(f"Found {len(grocery_links)} groceries")
    
                    if not grocery_links:
                        logging.warning(f"No groceries found for {area_name} at {area_url}")
                        # Save page content for debugging
                        html_content = await page.content()
                        debug_file = f"debug_area_{area_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                        with open(debug_file, "w", encoding="utf-8") as f:
                            f.write(html_content)
                        screenshot_file = f"debug_screenshot_{area_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                        await page.screenshot(path=screenshot_file, full_page=True)
                        logging.info(f"Saved debug artifacts for {area_name}: {debug_file}, {screenshot_file}")
    
                    completed_groceries = self.current_progress["current_progress"]["completed_groceries"]
                    for grocery_link in grocery_links:
                        grocery_title = grocery_link.split('/')[-1]
                        if grocery_title in completed_groceries:
                            logging.info(f"Skipping completed grocery: {grocery_title}")
                            print(f"Skipping completed grocery: {grocery_title}")
                            continue
    
                        logging.info(f"Processing grocery: {grocery_title} at {grocery_link}")
                        talabat_groceries = TalabatGroceries(grocery_link, browser, self)
                        grocery_data = await talabat_groceries.extract_categories(page)
    
                        if "error" not in grocery_data:
                            self.scraped_progress["all_results"].setdefault(area_name, {})[grocery_title] = {
                                "grocery_link": grocery_link,
                                "delivery_time": "N/A",
                                "grocery_details": {
                                    "delivery_fees": grocery_data.get("delivery_fees", "N/A"),
                                    "minimum_order": grocery_data.get("minimum_order", "N/A"),
                                    "categories": grocery_data.get("categories", {})
                                }
                            }
                            self.current_progress["current_progress"]["completed_groceries"][grocery_title] = {
                                "completed categories": [],
                                "completed sub-categories": []
                            }
                            self.save_current_progress()
                            self.save_scraped_progress()
                            self.commit_progress(f"Processed grocery {grocery_title} for {area_name}")
    
                    # Process categories and sub-categories
                    for grocery_title, grocery_data in self.scraped_progress["all_results"].get(area_name, {}).items():
                        if grocery_title in completed_groceries and completed_groceries[grocery_title].get("completed categories"):
                            logging.info(f"All categories completed for {grocery_title}, skipping")
                            continue
    
                        talabat_groceries = TalabatGroceries(grocery_data["grocery_link"], browser, self)
                        for category_name, category_data in grocery_data["grocery_details"]["categories"].items():
                            completed_categories = completed_groceries.get(grocery_title, {}).get("completed categories", [])
                            if category_name in completed_categories:
                                logging.info(f"Skipping completed category: {category_name} for {grocery_title}")
                                print(f"Skipping completed category: {category_name}")
                                continue
    
                            logging.info(f"Processing category: {category_name} for {grocery_title}")
                            sub_categories = await talabat_groceries.extract_sub_categories(
                                page, category_data["category_link"], grocery_title, category_name
                            )
                            self.scraped_progress["all_results"][area_name][grocery_title]["grocery_details"]["categories"][category_name]["sub_categories"] = sub_categories
                            self.save_scraped_progress()
                            self.commit_progress(f"Updated sub-categories for {category_name} in {grocery_title}")
    
                    # Verify groceries
                    logging.info(f"Verifying groceries for area: {area_name}")
                    print(f"Verifying groceries for area: {area_name}")
                    for grocery_title, grocery_data in self.scraped_progress["all_results"].get(area_name, {}).items():
                        talabat_groceries = TalabatGroceries(grocery_data["grocery_link"], browser, self)
                        for category_name, category_data in grocery_data["grocery_details"]["categories"].items():
                            missing_sub_categories = await talabat_groceries.verify_sub_categories(
                                page, category_data["category_link"], grocery_title, category_name
                            )
                            if missing_sub_categories:
                                logging.info(f"Found {len(missing_sub_categories)} missing sub-categories for {category_name} in {grocery_title}")
                                for sub_category in missing_sub_categories:
                                    sub_category_data = await talabat_groceries.extract_sub_categories(
                                        page, sub_category["sub_category_link"], grocery_title, category_name
                                    )
                                    self.scraped_progress["all_results"][area_name][grocery_title]["grocery_details"]["categories"][category_name]["sub_categories"].extend(sub_category_data)
                                    self.save_scraped_progress()
                                    self.commit_progress(f"Processed missing sub-category {sub_category['sub_category_name']} for {category_name}")
    
                    logging.info(f"All groceries processed for area: {area_name}")
                    print(f"All groceries processed for area: {area_name}")
                    self.current_progress["current_progress"]["completed_areas"].append(area_name)
                    self.save_current_progress()
                    self.commit_progress(f"Completed scraping area: {area_name}")
    
                    # Update Excel
                    print(f"Waiting 30 seconds before updating Excel for {area_name}...")
                    logging.info(f"Waiting 30 seconds before updating Excel for {area_name}")
                    await asyncio.sleep(30)
                    self.update_excel(area_name)
                    logging.info(f"Excel updated for {area_name}")
    
                except Exception as e:
                    logging.error(f"Error processing area {area_name}: {e}")
                    print(f"Error processing area {area_name}: {e}")
                finally:
                    await page.close()
                    await context.close()
                    await browser.close()
                    self.browsers.remove(browser)
                
async def main():
    parser = argparse.ArgumentParser(description="Talabat Groceries Scraper for a specific area")
    parser.add_argument("--area-name", required=True, help="Name of the area to scrape")
    parser.add_argument("--url", required=True, help="URL of the area to scrape")
    args = parser.parse_args()

    scraper = MainScraper(args.area_name)
    # Reset progress for the area to start scraping from the beginning
    scraper.reset_area_progress(args.area_name)
    areas = [{"area_name": args.area_name, "url": args.url}]
    await scraper.run(areas)

# async def main():
#     parser = argparse.ArgumentParser(description="Talabat Groceries Scraper for a specific area")
#     parser.add_argument("--area-name", required=True, help="Name of the area to scrape")
#     parser.add_argument("--url", required=True, help="URL of the area to scrape")
#     args = parser.parse_args()

#     scraper = MainScraper(args.area_name)
#     areas = [{"area_name": args.area_name, "url": args.url}]
#     await scraper.run(areas)

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

