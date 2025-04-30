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
from datetime import datetime
import argparse
import logging
import random
import time
import psutil
from SavingOnDrive import SavingOnDrive

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
        logging.info(f"Initialized TalabatGroceries with URL: {self.url}")

    async def get_general_link(self, page):
        logging.info("Attempting to get general link")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    link_element = await page.wait_for_selector('//a[@data-testid="view-all-link"]', timeout=90000)
                    if link_element:
                        full_link = self.base_url + await link_element.get_attribute('href')
                        logging.info(f"General link found: {full_link}")
                        return full_link
                    logging.warning("General link not found")
                    return None
                except PlaywrightTimeoutError as e:
                    logging.error(f"Timeout error getting general link: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    logging.error(f"Unexpected error getting general link: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return None

    async def get_delivery_fees(self, page):
        logging.info("Attempting to get delivery fees")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    delivery_fees_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]')
                    delivery_fees = await delivery_fees_element.inner_text() if delivery_fees_element else "N/A"
                    logging.info(f"Delivery fees: {delivery_fees}")
                    return delivery_fees
                except PlaywrightTimeoutError as e:
                    logging.error(f"Timeout error getting delivery fees: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    logging.error(f"Unexpected error getting delivery fees: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return "N/A"

    async def get_minimum_order(self, page):
        logging.info("Attempting to get minimum order")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    minimum_order_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]')
                    minimum_order = await minimum_order_element.inner_text() if minimum_order_element else "N/A"
                    logging.info(f"Minimum order: {minimum_order}")
                    return minimum_order
                except PlaywrightTimeoutError as e:
                    logging.error(f"Timeout error getting minimum order: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    logging.error(f"Unexpected error getting minimum order: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return "N/A"

    async def extract_category_names(self, page):
        logging.info("Attempting to extract category names")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
                    category_names = [await element.inner_text() for element in category_name_elements]
                    logging.info(f"Category names extracted: {category_names}")
                    return category_names
                except PlaywrightTimeoutError as e:
                    logging.error(f"Timeout error extracting category names: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    logging.error(f"Unexpected error extracting category names: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return []

    async def extract_category_links(self, page):
        logging.info("Attempting to extract category links")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
                    category_links = [self.base_url + await element.get_attribute('href') for element in category_link_elements]
                    logging.info(f"Category links extracted: {category_links}")
                    return category_links
                except PlaywrightTimeoutError as e:
                    logging.error(f"Timeout error extracting category links: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    logging.error(f"Unexpected error extracting category links: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return []

    async def extract_sub_categories(self, page, category_link, grocery_title, category_name):
        logging.info(f"Extracting sub-categories for: {category_link}")
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
                        logging.warning(f"Rate limit hit (429) for {category_link}, waiting before retry...")
                        retries -= 1
                        await asyncio.sleep(random.uniform(30, 60))
                        continue
                    elif response and response.status >= 500:
                        logging.warning(f"Server error ({response.status}) for {category_link}, retrying...")
                        retries -= 1
                        await asyncio.sleep(random.uniform(10, 20))
                        continue

                    await page.wait_for_load_state("networkidle", timeout=240000)
                    sub_category_elements = await page.query_selector_all('//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
                    sub_category_names = [await el.inner_text() for el in sub_category_elements]
                    sub_category_links = [self.base_url + await el.get_attribute('href') for el in sub_category_elements]

                    for idx, (sub_category_name, sub_category_link) in enumerate(zip(sub_category_names, sub_category_links)):
                        if sub_category_name in completed_sub_categories:
                            logging.info(f"Skipping completed sub-category: {sub_category_name}")
                            continue

                        if current_sub_category and not start_processing:
                            if sub_category_name == current_sub_category:
                                logging.info(f"Found current sub-category: {sub_category_name}, starting processing")
                                start_processing = True
                            else:
                                logging.info(f"Skipping sub-category {sub_category_name}, waiting for {current_sub_category}")
                                continue

                        logging.info(f"Processing sub-category: {sub_category_name}")
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
                    logging.error(f"Timeout error extracting sub-categories: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    logging.error(f"Unexpected error extracting sub-categories: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return sub_categories

    async def verify_sub_categories(self, page, category_link, grocery_title, category_name):
        logging.info(f"Verifying sub-categories for category: {category_name} at {category_link}")
        async with self.main_scraper.semaphore:
            retries = 3
            missing_sub_categories = []
            completed_sub_categories = self.main_scraper.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {}).get("completed sub-categories", [])

            while retries > 0:
                try:
                    response = await page.goto(category_link, timeout=240000, wait_until="domcontentloaded")
                    if response and response.status == 429:
                        logging.warning(f"Rate limit hit (429) for {category_link}, waiting before retry...")
                        retries -= 1
                        await asyncio.sleep(random.uniform(30, 60))
                        continue
                    elif response and response.status >= 500:
                        logging.warning(f"Server error ({response.status}) for {category_link}, retrying...")
                        retries -= 1
                        await asyncio.sleep(random.uniform(10, 20))
                        continue

                    await page.wait_for_load_state("networkidle", timeout=240000)
                    sub_category_elements = await page.query_selector_all('//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
                    sub_category_names = [await el.inner_text() for el in sub_category_elements]
                    sub_category_links = [self.base_url + await el.get_attribute('href') for el in sub_category_elements]

                    for name, link in zip(sub_category_names, sub_category_links):
                        if name not in completed_sub_categories:
                            logging.info(f"Found missing sub-category: {name}")
                            missing_sub_categories.append({"sub_category_name": name, "sub_category_link": link})
                    return missing_sub_categories
                except PlaywrightTimeoutError as e:
                    logging.error(f"Timeout error verifying sub-categories for {category_link}: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    logging.error(f"Unexpected error verifying sub-categories for {category_link}: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return missing_sub_categories

    async def extract_item_details(self, item_link):
        logging.info(f"Extracting item details for link: {item_link}")
        async with self.main_scraper.semaphore:
            retries = 3
            context = None
            page = None

            for attempt in range(retries):
                try:
                    async with self.main_scraper.context_semaphore:
                        self.main_scraper.active_contexts += 1
                        context = await self.browser.new_context(
                            user_agent=random.choice(self.main_scraper.user_agents),
                            viewport={"width": 1920, "height": 1080},
                            java_script_enabled=True,
                            bypass_csp=True,
                            extra_http_headers={
                                "Accept-Language": "en-US,en;q=0.9",
                                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                                "Connection": "keep-alive",
                                "Cache-Control": "no-cache",
                            },
                            storage_state=self.main_scraper.load_cookies()
                        )
                        page = await context.new_page()

                        response = await page.goto(item_link, timeout=20000, wait_until="domcontentloaded")
                        if response and response.status == 429:
                            logging.warning(f"Rate limit hit (429) for {item_link}, attempt {attempt + 1}/{retries}")
                            delay = random.uniform(60, 120)
                            await asyncio.sleep(delay)
                            continue
                        elif response and response.status >= 500:
                            logging.warning(f"Server error ({response.status}) for {item_link}, attempt {attempt + 1}/{retries}")
                            await asyncio.sleep(random.uniform(10, 20))
                            continue

                        await page.wait_for_load_state("networkidle", timeout=20000)

                        anti_bot = await page.query_selector('//h1[contains(text(), "Access Denied")] | //div[contains(text(), "Please verify you are not a robot")]')
                        if anti_bot:
                            logging.warning(f"Anti-bot page detected for {item_link}")
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
                                    logging.info(f"Found critical element with selector: {selector}")
                                    break
                            except PlaywrightTimeoutError:
                                logging.warning(f"Selector {selector} timed out")

                        if not critical_element:
                            logging.warning(f"No critical elements found on {item_link}")
                            html_content = await page.content()
                            debug_file = f"debug_item_page_{item_link.split('/')[-1]}.html"
                            with open(debug_file, "w", encoding="utf-8") as f:
                                f.write(html_content)
                            screenshot_file = f"debug_screenshot_{item_link.split('/')[-1]}.png"
                            await page.screenshot(path=screenshot_file, full_page=True)
                            logging.info(f"Saved debug artifacts for {item_link}")
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
                        logging.info(f"Item price: {item_price}")

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
                        logging.info(f"Item old price: {item_old_price}")

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
                        logging.info(f"Item offer: {item_offer}")

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
                        logging.info(f"Item description: {item_description}")

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
                        logging.info(f"Delivery time range: {delivery_time}")

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
                        logging.info(f"Item images: {item_images}")

                        if item_price == "N/A" and item_description == "N/A" and not item_images:
                            logging.warning(f"Critical data missing for {item_link}, likely rate limited")
                            return {
                                "item_price": "N/A",
                                "item_old_price": None,
                                "item_offer": None,
                                "item_description": "Failed due to rate limiting",
                                "item_delivery_time_range": "N/A",
                                "item_images": []
                            }

                        # Save cookies for session persistence
                        cookies = await context.storage_state()
                        self.main_scraper.save_cookies(cookies)

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
                    logging.error(f"Timeout error extracting item details for {item_link}: {e}")
                    if page:
                        try:
                            html_content = await page.content()
                            debug_file = f"debug_item_page_{item_link.split('/')[-1]}.html"
                            with open(debug_file, "w", encoding="utf-8") as f:
                                f.write(html_content)
                            screenshot_file = f"debug_screenshot_{item_link.split('/')[-1]}.png"
                            await page.screenshot(path=screenshot_file, full_page=True)
                            logging.info(f"Saved debug artifacts for {item_link}")
                        except Exception as debug_e:
                            logging.error(f"Failed to save debug artifacts: {debug_e}")
                except Exception as e:
                    logging.error(f"Unexpected error extracting item details for {item_link}: {e}")
                finally:
                    if page:
                        await page.close()
                    if context:
                        await context.close()
                    self.main_scraper.active_contexts -= 1
                    await asyncio.sleep(random.uniform(5, 10))
                    if attempt < retries - 1:
                        logging.info(f"Attempt {attempt + 1}/{retries}, retrying...")
            logging.error(f"Failed to extract details for {item_link} after all retries")
            return {
                "item_price": "N/A",
                "item_old_price": None,
                "item_offer": None,
                "item_description": "Failed due to rate limiting or timeout",
                "item_delivery_time_range": "N/A",
                "item_images": []
            }

    async def extract_all_items_from_sub_category(self, sub_category_link):
        logging.info(f"Extracting all items from sub-category: {sub_category_link}")
        async with self.main_scraper.semaphore:
            retries = 3
            context = None
            sub_page = None

            for attempt in range(retries):
                try:
                    async with self.main_scraper.context_semaphore:
                        self.main_scraper.active_contexts += 1
                        context = await self.browser.new_context(
                            user_agent=random.choice(self.main_scraper.user_agents),
                            viewport={"width": 1920, "height": 1080},
                            java_script_enabled=True,
                            bypass_csp=True,
                            storage_state=self.main_scraper.load_cookies()
                        )
                        sub_page = await context.new_page()

                        response = await sub_page.goto(sub_category_link, timeout=60000, wait_until="domcontentloaded")
                        if response and response.status == 429:
                            logging.warning(f"Rate limit hit (429) for {sub_category_link}, attempt {attempt + 1}/{retries}")
                            await asyncio.sleep(random.uniform(60, 120))
                            continue
                        elif response and response.status >= 500:
                            logging.warning(f"Server error ({response.status}) for {sub_category_link}, attempt {attempt + 1}/{retries}")
                            await asyncio.sleep(random.uniform(10, 20))
                            continue

                        await sub_page.wait_for_load_state("networkidle", timeout=60000)

                        empty_message = await sub_page.query_selector('//div[contains(text(), "No items") or contains(text(), "empty") or contains(@class, "no-results")]')
                        if empty_message:
                            logging.info(f"Sub-category {sub_category_link} is empty")
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
                                    logging.info(f"Found item containers using selector: {selector}")
                                    break
                            except PlaywrightTimeoutError:
                                logging.warning(f"Selector {selector} timed out")

                        if not item_elements:
                            logging.warning(f"No item containers found on {sub_category_link}")
                            html_content = await sub_page.content()
                            debug_file = f"debug_sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.html"
                            with open(debug_file, "w", encoding="utf-8") as f:
                                f.write(html_content)
                            screenshot_file = f"debug_screenshot_sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.png"
                            await sub_page.screenshot(path=screenshot_file, full_page=True)
                            logging.info(f"Saved debug artifacts for {sub_category_link}")
                            await sub_page.close()
                            await context.close()
                            return []

                        html_content = await sub_page.content()
                        html_filename = f"sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.html"
                        with open(html_filename, "w", encoding="utf-8") as f:
                            f.write(html_content)
                        logging.info(f"Saved sub-category HTML to {html_filename} for debugging")

                        pagination_element = await sub_page.query_selector('[class*="paginate-wrap"]')
                        total_pages = 1
                        if pagination_element:
                            page_numbers = await pagination_element.query_selector_all('li.paginate-li a')
                            total_pages = len(page_numbers) if page_numbers else 1
                        logging.info(f"Found {total_pages} pages in this sub-category")

                        items = []
                        for page_number in range(1, total_pages + 1):
                            logging.info(f"Processing page {page_number} of {total_pages}")
                            page_url = f"{sub_category_link}&page={page_number}" if page_number > 1 else sub_category_link
                            response = await sub_page.goto(page_url, timeout=60000, wait_until="domcontentloaded")
                            if response and response.status == 429:
                                logging.warning(f"Rate limit hit (429) for page {page_number}, attempt {attempt + 1}/{retries}")
                                await asyncio.sleep(random.uniform(60, 120))
                                continue
                            await sub_page.wait_for_load_state("networkidle", timeout=60000)

                            item_link_elements = await sub_page.query_selector_all('[data-testid="grocery-item-link-nofollow"]')
                            logging.info(f"Found {len(item_link_elements)} items on page {page_number}")

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
                                                    logging.info(f"Item name: {item_name}")
                                                    break
                                                else:
                                                    item_name = None

                                    if not item_name or not item_name.strip():
                                        item_name = f"Unknown Item {i+1}"
                                        logging.warning(f"No valid item name found, using default: {item_name}")

                                    item_link = self.base_url + await element.get_attribute('href')
                                    logging.info(f"Item link: {item_link}")

                                    item_details = await self.extract_item_details(item_link)
                                    items.append({
                                        "item_name": item_name.strip(),
                                        "item_link": item_link,
                                        **item_details
                                    })

                                    await asyncio.sleep(random.uniform(3, 6))
                                except Exception as e:
                                    logging.error(f"Error processing item {i+1}: {e}")

                        # Save cookies for session persistence
                        cookies = await context.storage_state()
                        self.main_scraper.save_cookies(cookies)

                        await sub_page.close()
                        await context.close()
                        return items
                except PlaywrightTimeoutError as e:
                    logging.error(f"Timeout error extracting items from sub-category {sub_category_link}: {e}")
                    if sub_page:
                        try:
                            html_content = await sub_page.content()
                            debug_file = f"debug_sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.html"
                            with open(debug_file, "w", encoding="utf-8") as f:
                                f.write(html_content)
                            screenshot_file = f"debug_screenshot_sub_category_{sub_category_link.split('/')[-1].replace('?aid=', '')}.png"
                            await sub_page.screenshot(path=screenshot_file, full_page=True)
                            logging.info(f"Saved debug artifacts for {sub_category_link}")
                        except Exception as debug_e:
                            logging.error(f"Failed to save debug artifacts: {debug_e}")
                except Exception as e:
                    logging.error(f"Unexpected error extracting items from sub-category {sub_category_link}: {e}")
                finally:
                    if sub_page:
                        await sub_page.close()
                    if context:
                        await context.close()
                    self.main_scraper.active_contexts -= 1
                    await asyncio.sleep(random.uniform(5, 10))
                    if attempt < retries - 1:
                        logging.info(f"Attempt {attempt + 1}/{retries}, retrying...")
            logging.error(f"Failed to extract items from sub-category {sub_category_link} after all retries")
            return []

    async def extract_categories(self, page):
        logging.info(f"Processing grocery: {self.url}")
        async with self.main_scraper.semaphore:
            retries = 3
            while retries > 0:
                try:
                    response = await page.goto(self.url, timeout=240000, wait_until="domcontentloaded")
                    if response and response.status == 429:
                        logging.warning(f"Rate limit hit (429) for {self.url}, waiting before retry...")
                        retries -= 1
                        await asyncio.sleep(random.uniform(30, 60))
                        continue
                    elif response and response.status >= 500:
                        logging.warning(f"Server error ({response.status}) for {self.url}, retrying...")
                        retries -= 1
                        await asyncio.sleep(random.uniform(10, 20))
                        continue

                    await page.wait_for_load_state("networkidle", timeout=240000)
                    logging.info("Page loaded successfully")

                    delivery_fees = await self.get_delivery_fees(page)
                    minimum_order = await self.get_minimum_order(page)
                    view_all_link = await self.get_general_link(page)

                    logging.info(f"Delivery fees: {delivery_fees}")
                    logging.info(f"Minimum order: {minimum_order}")

                    categories_data = {}
                    if view_all_link:
                        logging.info(f"Navigating to view all link: {view_all_link}")
                        context = await self.browser.new_context(
                            user_agent=random.choice(self.main_scraper.user_agents),
                            storage_state=self.main_scraper.load_cookies()
                        )
                        category_page = await context.new_page()
                        response = await category_page.goto(view_all_link, timeout=240000, wait_until="domcontentloaded")
                        if response and response.status == 429:
                            logging.warning(f"Rate limit hit (429) for {view_all_link}, waiting before retry...")
                            retries -= 1
                            await asyncio.sleep(random.uniform(30, 60))
                            await category_page.close()
                            await context.close()
                            continue
                        elif response and response.status >= 500:
                            logging.warning(f"Server error ({response.status}) for {view_all_link}, retrying...")
                            retries -= 1
                            await asyncio.sleep(random.uniform(10, 20))
                            await category_page.close()
                            await context.close()
                            continue

                        await category_page.wait_for_load_state("networkidle", timeout=240000)

                        category_names = await self.extract_category_names(category_page)
                        category_links = await self.extract_category_links(category_page)

                        logging.info(f"Found {len(category_names)} categories")

                        for name, link in zip(category_names, category_links):
                            logging.info(f"Category: {name}, Link: {link}")
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
                    logging.error(f"Timeout error extracting categories: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
                except Exception as e:
                    logging.error(f"Unexpected error extracting categories: {e}")
                    retries -= 1
                    logging.info(f"Retries left: {retries}")
                    await asyncio.sleep(10 * (2 ** (3 - retries)))
            return {"error": "Failed to extract categories after multiple attempts"}

class MainScraper:
    def __init__(self, area_name):
        self.area_name = area_name
        self.CURRENT_PROGRESS_FILE = f"current_progress_{area_name}.json"
        self.SCRAPED_PROGRESS_FILE = f"scraped_progress_{area_name}.json"
        self.COOKIES_FILE = f"cookies_{area_name}.json"
        self.output_dir = "output"
        self.OUTPUT_JSON_FILE = f"output/{area_name}.json"
        self.OUTPUT_EXCEL_FILE = f"output/{area_name}_detailed.xlsx"
        self.output_suffix = ""
        self.github_token = os.environ.get('GITHUB_TOKEN')
        self.gcloud_key_json = os.environ.get('TALABAT_GCLOUD_KEY_JSON')
        self.semaphore = asyncio.Semaphore(2)
        self.max_browsers = 1
        self.max_contexts_per_browser = 2
        self.active_contexts = 0
        self.context_semaphore = asyncio.Semaphore(self.max_contexts_per_browser * self.max_browsers)
        self.browsers = []
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        ]
        os.makedirs(self.output_dir, exist_ok=True)
        self.current_progress = self.load_current_progress()
        self.scraped_progress = self.load_scraped_progress()
        self.ensure_playwright_browsers()
        self.save_current_progress()
        self.save_scraped_progress()
        # Run async commit_progress in an event loop
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.commit_progress(f"Initialized progress files for {area_name}"))

    def save_cookies(self, storage_state):
        try:
            with tempfile.NamedTemporaryFile('w', delete=False, dir='.', encoding='utf-8') as temp_file:
                json.dump(storage_state, temp_file, indent=2, ensure_ascii=False)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_filename = temp_file.name
            os.replace(temp_filename, self.COOKIES_FILE)
            logging.info(f"Saved cookies to {self.COOKIES_FILE}")
        except Exception as e:
            logging.error(f"Error saving cookies: {e}")

    def load_cookies(self):
        if os.path.exists(self.COOKIES_FILE):
            try:
                with open(self.COOKIES_FILE, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                logging.info(f"Loaded cookies from {self.COOKIES_FILE}")
                return cookies
            except Exception as e:
                logging.error(f"Error loading cookies: {e}")
        return None

    def reset_area_progress(self, area_name: str, new_suffix=None):
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
        if new_suffix:
            self.output_suffix = new_suffix
            self.OUTPUT_JSON_FILE = f"output/{area_name}{new_suffix}.json"
            self.OUTPUT_EXCEL_FILE = f"output/{area_name}{new_suffix}_detailed.xlsx"
        else:
            self.output_suffix = ""
            self.OUTPUT_JSON_FILE = f"output/{area_name}.json"
            self.OUTPUT_EXCEL_FILE = f"output/{area_name}_detailed.xlsx"
        self.save_current_progress()
        self.save_scraped_progress()
        self.commit_progress(f"Reset progress for {area_name} to re-scrape with suffix {new_suffix or 'default'}")
        logging.info(f"Reset progress for area: {area_name} with suffix {new_suffix or 'default'}")
    
    async def initialize_browsers(self, playwright):
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
        if not self.browsers:
            raise RuntimeError("No browsers initialized")
        return random.choice(self.browsers)

    async def close_browsers(self):
        for browser in self.browsers:
            await browser.close()
        self.browsers.clear()
        logging.info("Closed all browser instances")

    def ensure_playwright_browsers(self):
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)

    def save_current_progress(self, progress: Dict = None):
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

    def load_current_progress(self) -> Dict:
        if os.path.exists(self.CURRENT_PROGRESS_FILE):
            try:
                with open(self.CURRENT_PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                if "current_progress" not in progress:
                    progress["current_progress"] = {}
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
                logging.info(f"Loaded {self.CURRENT_PROGRESS_FILE}")
                return progress
            except Exception as e:
                logging.error(f"Error loading {self.CURRENT_PROGRESS_FILE}: {e}")

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
            }
        }
        logging.info(f"{self.CURRENT_PROGRESS_FILE} not found, creating default")
        self.save_current_progress(default_progress)
        return default_progress

    def load_scraped_progress(self) -> Dict:
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

    def save_scraped_progress(self, progress: Dict = None):
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
            logging.info(f"Saved scraped progress to {self.SCRAPED_PROGRESS_FILE}")
    
            # Save to JSON file
            output_data = progress.get("all_results", {})
            with open(self.OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            logging.info(f"Saved output to {self.OUTPUT_JSON_FILE}")
    
            # Save to Excel
            self.save_to_excel(output_data)
        except Exception as e:
            logging.error(f"Error saving scraped progress: {e}")
    
    def save_to_excel(self, output_data):
        try:
            excel_data = []
            area_data = output_data.get(self.area_name, {})
            for grocery_title, grocery_data in area_data.items():
                categories = grocery_data.get("grocery_details", {}).get("categories", {})
                for category_name, category_data in categories.items():
                    for sub_category in category_data.get("sub_categories", []):
                        for item in sub_category.get("items", []):
                            excel_data.append({
                                "Area": self.area_name,
                                "Grocery": grocery_title,
                                "Category": category_name,
                                "Sub-Category": sub_category.get("sub_category_name"),
                                "Item Name": item.get("item_name"),
                                "Item Link": item.get("item_link"),
                                "Item Price": item.get("item_price"),
                                "Item Old Price": item.get("item_old_price"),
                                "Item Offer": item.get("item_offer"),
                                "Item Description": item.get("item_description"),
                                "Delivery Time Range": item.get("item_delivery_time_range"),
                                "Item Images": ", ".join(item.get("item_images", []))
                            })
            if excel_data:
                df = pd.DataFrame(excel_data)
                df.to_excel(self.OUTPUT_EXCEL_FILE, index=False)
                logging.info(f"Saved detailed data to {self.OUTPUT_EXCEL_FILE}")
            else:
                logging.warning("No data to save to Excel")
        except Exception as e:
            logging.error(f"Error saving to Excel: {e}")
    
    async def commit_progress(self, message):
        logging.info(f"Committing progress: {message}")
        try:
            subprocess.run(["git", "config", "--global", "user.name", "GitHub Action"], check=True)
            subprocess.run(["git", "config", "--global", "user.email", "action@github.com"], check=True)
            subprocess.run(["git", "add", f"current_progress_{self.area_name}.json", f"scraped_progress_{self.area_name}.json", f"cookies_{self.area_name}.json", "output/*.json", "output/*.xlsx"], check=True)
            subprocess.run(["git", "commit", "-m", message], check=True, capture_output=True)
            
            for attempt in range(1, 4):
                try:
                    subprocess.run(["git", "pull", "--rebase"], check=True)
                    subprocess.run(["git", "push", "origin", "master"], check=True)
                    logging.info(f"Successfully pushed changes on attempt {attempt}")
                    return
                except subprocess.CalledProcessError as e:
                    logging.warning(f"Git push failed (attempt {attempt}/3): {e}")
                    if attempt < 3:
                        await asyncio.sleep(random.uniform(5, 10) * attempt)  # Exponential backoff with jitter
            logging.error("Failed to push changes after 3 attempts")
        except subprocess.CalledProcessError as e:
            logging.error(f"Git commit failed: {e}")
        except Exception as e:
            logging.error(f"Unexpected error during commit: {e}")

    async def process_grocery_categories(self, grocery_title, grocery_details, talabat_grocery, page, groceries_on_page, grocery_idx):
        completed_groceries = self.current_progress["current_progress"]["completed_groceries"].get(grocery_title, {})
        completed_categories = completed_groceries.get("completed categories", [])
        current_category = self.current_progress["current_progress"].get("current_category")
        current_sub_category = self.current_progress["current_progress"].get("current_sub_category")
        categories = grocery_details.get("categories", {})

        if not categories:
            logging.info(f"No categories found for {grocery_title}, marking as complete")
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
                logging.warning(f"Current sub-category {current_sub_category} not found in any category, resetting current_category")
                self.current_progress["current_progress"]["current_category"] = None
                self.scraped_progress["current_progress"]["current_category"] = None
                start_processing = True

        for idx, category_name in enumerate(category_names):
            if category_name in completed_categories:
                logging.info(f"Category {category_name} already completed, skipping")
                continue

            if current_category and not start_processing:
                if category_name == current_category:
                    start_processing = True
                else:
                    logging.info(f"Skipping category {category_name}, waiting for {current_category}")
                    continue

            logging.info(f"Processing category {idx + 1}/{len(category_names)}: {category_name}")
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

    async def verify_and_scrape_missing_sub_categories(self, grocery_title, grocery_details, talabat_grocery, page):
        logging.info(f"Verifying sub-categories for grocery: {grocery_title}")
        area_name = self.current_progress["current_progress"]["area_name"]
        completed_groceries = self.current_progress["current_progress"]["completed_groceries"].setdefault(grocery_title, {})
        completed_sub_categories = completed_groceries.get("completed sub-categories", [])

        for category_name, category_data in grocery_details.get("categories", {}).items():
            category_link = category_data["category_link"]
            logging.info(f"Checking category: {category_name}")

            missing_sub_categories = await talabat_grocery.verify_sub_categories(page, category_link, grocery_title, category_name)
            if missing_sub_categories:
                logging.info(f"Found {len(missing_sub_categories)} missing sub-categories in {category_name}")
                for missing_sub in missing_sub_categories:
                    sub_category_name = missing_sub["sub_category_name"]
                    sub_category_link = missing_sub["sub_category_link"]
                    logging.info(f"Scraping missing sub-category: {sub_category_name}")
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

        logging.info(f"Waiting 30 seconds before updating Excel for {area_name}...")
        await asyncio.sleep(30)
        await self.convert_json_to_excel(area_name, json_filename)

    def move_to_next_category(self, category_names, current_idx, grocery_title, completed_categories):
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
        except Exception as e:
            logging.error(f"Error converting JSON to Excel for {area_name}: {e}")

    async def handle_completed_area(self):
        logging.info(f"Handling completed area: {self.area_name}")
        success = self.upload_to_drive()
        if success:
            # Delete files on successful upload
            try:
                os.remove(self.OUTPUT_JSON_FILE)
                os.remove(self.OUTPUT_EXCEL_FILE)
                logging.info(f"Deleted {self.OUTPUT_JSON_FILE} and {self.OUTPUT_EXCEL_FILE}")
            except Exception as e:
                logging.error(f"Error deleting files: {e}")
            # Reset progress with default file names
            self.reset_area_progress(self.area_name)
        else:
            # Create new file names with timestamp
            timestamp = datetime.now().strftime("_%Y%m%d_%H%M%S")
            self.reset_area_progress(self.area_name, new_suffix=timestamp)
        await self.commit_progress(f"Handled completed area {self.area_name}, upload {'successful' if success else 'failed'}")

    def upload_to_drive(self):
        logging.info(f"Uploading files for {self.area_name} to Google Drive...")
        try:
            # Initialize SavingOnDrive with credentials from environment variable
            drive_uploader = SavingOnDrive(self.gcloud_key_json)
            if not drive_uploader.authenticate():
                logging.error("Failed to authenticate with Google Drive. Check TALABAT_GCLOUD_KEY_JSON validity.")
                return False
    
            # Define files to upload
            files_to_upload = [
                self.OUTPUT_JSON_FILE,
                self.OUTPUT_EXCEL_FILE
            ]
    
            success = True
            for file_path in files_to_upload:
                if not os.path.exists(file_path):
                    logging.error(f"File {file_path} does not exist")
                    success = False
                    continue
    
                # Upload to multiple folders (date-based subfolders in target folders)
                file_ids = drive_uploader.upload_to_multiple_folders(file_path)
                if len(file_ids) == len(drive_uploader.target_folders):
                    logging.info(f"Successfully uploaded {file_path} to Google Drive")
                else:
                    logging.error(f"Failed to upload {file_path}: Incomplete upload to folders")
                    success = False
    
            return success
    
        except Exception as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                logging.error(f"Rate limit exceeded while uploading to Google Drive: {str(e)}")
            else:
                logging.error(f"Error uploading to Google Drive: {str(e)}")
            return False
    
    async def scrape_and_save_area(self, area_name: str, area_url: str, browser, max_runtime=5.5*3600) -> List[Dict]:
        start_time = time.time()
        logging.info(f"\n{'='*50}\nSCRAPING AREA: {area_name}\nURL: {area_url}\n{'='*50}")
        process = psutil.Process()
        logging.info(f"Memory usage before scrape: {process.memory_info().rss / 1024 / 1024:.2f} MB")
        logging.info(f"Active browser contexts: {self.active_contexts}")
    
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
            context = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                viewport={"width": 1920, "height": 1080},
                java_script_enabled=True,
                bypass_csp=True,
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Connection": "keep-alive",
                },
                storage_state=self.load_cookies()
            )
            page = await context.new_page()
            response = await page.goto(area_url, timeout=60000, wait_until="domcontentloaded")
            if response and response.status == 429:
                logging.warning(f"Rate limit hit (429) for {area_url}, aborting scrape.")
                return []
            elif response and response.status >= 500:
                logging.warning(f"Server error ({response.status}) for {area_url}, aborting scrape.")
                return []
    
            cookies = await context.storage_state()
            self.save_cookies(cookies)
    
            groceries_on_page = await self.get_page_groceries(page)
            current_progress["total_groceries"] = len(groceries_on_page)
            scraped_current_progress["total_groceries"] = len(groceries_on_page)
            logging.info(f"Found {len(groceries_on_page)} groceries")
            await page.close()
    
            processed_grocery_titles = set(current_progress["processed_groceries"])
            current_grocery_title = current_progress.get("current_grocery_title")
    
            for grocery_idx, grocery in enumerate(groceries_on_page):
                if time.time() - start_time >= max_runtime:
                    logging.info(f"Max runtime ({max_runtime}s) reached for {area_name}, saving progress")
                    break
    
                grocery_num = grocery_idx + 1
                grocery_title = grocery["grocery_title"]
                grocery_link = grocery["grocery_link"]
    
                if grocery_title in processed_grocery_titles:
                    logging.info(f"Skipping already processed grocery: {grocery_title}")
                    continue
    
                if current_grocery_title and current_grocery_title != grocery_title:
                    logging.info(f"Skipping grocery {grocery_title}, waiting for {current_grocery_title}")
                    continue
    
                current_progress["current_grocery"] = grocery_num
                current_progress["current_grocery_title"] = grocery_title
                current_progress["current_grocery_link"] = grocery_link
                scraped_current_progress["current_grocery"] = grocery_num
                scraped_current_progress["current_grocery_title"] = grocery_title
                scraped_current_progress["current_grocery_link"] = grocery_link
                self.save_current_progress()
                self.save_scraped_progress()
                logging.info(f"Processing grocery {grocery_num}/{len(groceries_on_page)}: {grocery_title} (link: {grocery_link})")
    
                async with self.context_semaphore:
                    self.active_contexts += 1
                    grocery_context = await browser.new_context(
                        user_agent=random.choice(self.user_agents),
                        storage_state=self.load_cookies()
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
                    await asyncio.sleep(random.uniform(5, 10))
    
                if current_grocery_title == grocery_title:
                    break
    
            logging.info(f"Verifying groceries for area: {area_name}")
            verify_context = await browser.new_context(
                user_agent=random.choice(self.user_agents),
                storage_state=self.load_cookies()
            )
            page = await verify_context.new_page()
            await page.goto(area_url, timeout=60000)
            current_groceries = await self.get_page_groceries(page)
            await page.close()
            await verify_context.close()
    
            missing_groceries = [g for g in current_groceries if g["grocery_title"] not in processed_grocery_titles]
            if missing_groceries and time.time() - start_time < max_runtime:
                logging.info(f"Found {len(missing_groceries)} missing groceries in {area_name}")
                for grocery_idx, grocery in enumerate(missing_groceries):
                    if time.time() - start_time >= max_runtime:
                        logging.info(f"Max runtime ({max_runtime}s) reached for {area_name}, saving progress")
                        break
    
                    grocery_num = len(groceries_on_page) + grocery_idx + 1
                    grocery_title = grocery["grocery_title"]
                    grocery_link = grocery["grocery_link"]
                    logging.info(f"Processing missing grocery {grocery_num}: {grocery_title} (link: {grocery_link})")
    
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
                            storage_state=self.load_cookies()
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
    
                        await self.process_grocery_categories(grocery_title, grocery_details, talabat_grocery, grocery_page, groceries_on_page + missing_groceries, grocery_idx)
                        await grocery_page.close()
                        await grocery_context.close()
                        self.active_contexts -= 1
                        await asyncio.sleep(random.uniform(5, 10))
    
            json_filename = os.path.join(self.output_dir, f"{area_name}.json")
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(all_area_results, f, indent=2, ensure_ascii=False)
    
            processed_grocery_titles = set(current_progress["processed_groceries"])
            if all(g["grocery_title"] in processed_grocery_titles for g in current_groceries):
                current_progress.update({
                    "area_name": None,
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
                self.current_progress["completed_areas"].append(area_name)
                self.scraped_progress["completed_areas"].append(area_name)
                self.save_current_progress()
                self.save_scraped_progress()
                await self.commit_progress(f"Marked area {area_name} as complete")
    
                # Upload files to Google Drive and handle completion
                await self.handle_completed_area()
    
            self.save_current_progress()
            self.save_scraped_progress()
            await self.commit_progress(f"Completed {area_name}")
    
            return list(all_area_results.values())
        except Exception as e:
            logging.error(f"Error scraping area {area_name}: {e}")
            return []
        finally:
            if page:
                await page.close()
            if context:
                await context.close()
            logging.info(f"Memory usage after scrape: {process.memory_info().rss / 1024 / 1024:.2f} MB")
            logging.info(f"Active browser contexts: {self.active_contexts}")
        
    async def process_category(self, grocery_title, category_data, category_name, talabat_grocery, page):
        async with self.semaphore:
            sub_categories = await talabat_grocery.extract_sub_categories(page, category_data["category_link"], grocery_title, category_name)
            category_data["sub_categories"] = sub_categories
            self.save_current_progress()
            self.save_scraped_progress()

    async def get_page_groceries(self, page) -> List[Dict]:
        async with self.semaphore:
            logging.info("Extracting grocery information")
            try:
                await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=90000)
                vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
                groceries_info = []
                for container in vendor_containers:
                    title_element = await container.query_selector('a div h2')
                    title = await title_element.inner_text() if title_element else "Unknown Grocery"
                    link_element = await container.query_selector('a')
                    link = "https://www.talabat.com" + await link_element.get_attribute('href') if link_element else None
                    delivery_info = await container.query_selector('div.deliveryInfo')
                    delivery_time_text = await delivery_info.inner_text() if delivery_info else ""
                    delivery_time = re.findall(r'\d+', delivery_time_text)[0] + " mins" if re.findall(r'\d+', delivery_time_text) else "N/A"
                    if link:
                        groceries_info.append({"grocery_title": title, "grocery_link": link, "delivery_time": delivery_time})
                logging.info(f"Extracted {len(groceries_info)} groceries: {[g['grocery_title'] for g in groceries_info]}")
                return groceries_info
            except Exception as e:
                logging.error(f"Error extracting groceries: {e}")
                return []

    async def run(self, areas: List[Dict[str, str]], playwright):
        await self.initialize_browsers(playwright)
        try:
            for area in areas:
                area_name = area["name"]
                area_url = area["url"]
                if area_name in self.current_progress["completed_areas"]:
                    logging.info(f"Skipping completed area: {area_name}")
                    continue
                logging.info(f"Starting scrape for area: {area_name}")
                await self.scrape_and_save_area(area_name, area_url, None)
                await asyncio.sleep(random.uniform(10, 20))
            self.current_progress["current_area_index"] = len(areas)
            self.save_current_progress()
            self.save_scraped_progress()
            self.commit_progress("Completed all areas")
        finally:
            await self.close_browsers()
        logging.info("All areas processed.")

async def main():
    parser = argparse.ArgumentParser(description="Talabat Groceries Scraper for a specific area")
    parser.add_argument("--area-name", required=True, help="Name of the area to scrape (e.g., المنقف)")
    parser.add_argument("--url", required=True, help="URL of the area to scrape (e.g., https://www.talabat.com/kuwait/restaurants/32/mangaf)")
    parser.add_argument("--upload-to-drive", action="store_true", help="Upload completed area files to Google Drive")
    args = parser.parse_args()

    scraper = MainScraper(args.area_name)
    async with async_playwright() as playwright:
        if args.upload_to_drive:
            # Handle upload for completed area
            logging.info(f"Processing upload-to-drive for area: {args.area_name}")
            await scraper.handle_completed_area()
        else:
            # Run scraping for the specified area
            areas = [{"name": args.area_name, "url": args.url}]
            await scraper.run(areas, playwright)

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

