import asyncio
import json
import os
import re
import time
from playwright.async_api import async_playwright, Page, TimeoutError

class TalabatGroceries:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.talabat.com"
        self.timeout = 45000  # 45 seconds
        self.short_timeout = 10000  # 10 seconds for faster checks
        self.debug = True  # Enable debug output
        
    def debug_print(self, message):
        """Print debug messages if debug is enabled"""
        if self.debug:
            print(f"  DEBUG: {message}")

    async def save_screenshot(self, page, name):
        """Save screenshot for debugging"""
        if self.debug:
            screenshot_dir = "screenshots"
            if not os.path.exists(screenshot_dir):
                os.makedirs(screenshot_dir)
            timestamp = int(time.time())
            filename = f"{screenshot_dir}/{name}_{timestamp}.png"
            await page.screenshot(path=filename)
            self.debug_print(f"Screenshot saved to {filename}")

    async def check_selectors(self, page, selectors, description):
        """Try multiple selectors and return the one that works"""
        self.debug_print(f"Checking selectors for {description}...")
        for selector in selectors:
            try:
                exists = await page.is_visible(selector, timeout=self.short_timeout)
                if exists:
                    self.debug_print(f"Found working selector for {description}: {selector}")
                    return selector
            except Exception:
                continue
        self.debug_print(f"No working selector found for {description}")
        return None

    async def navigate_to_all_categories(self, page):
        """Try different methods to navigate to the all categories view"""
        try:
            # First, check if we're already on the all categories page
            all_categories_indicators = [
                '//span[@data-testid="category-name"]',
                '//div[contains(@class, "category-name")]',
                '//h2[contains(@class, "category-title")]',
                '//div[contains(@class, "categories-list")]'
            ]
            
            for indicator in all_categories_indicators:
                if await page.is_visible(indicator, timeout=5000):
                    self.debug_print("Already on all categories page")
                    return True
            
            # Try to find and click the "View All" link
            view_all_selectors = [
                '//a[@data-testid="view-all-link"]',
                '//a[contains(text(), "View All")]',
                '//a[contains(text(), "view all")]',
                '//a[contains(@class, "view-all")]'
            ]
            
            working_selector = await self.check_selectors(page, view_all_selectors, "View All link")
            if working_selector:
                self.debug_print(f"Clicking View All link: {working_selector}")
                await page.click(working_selector)
                await page.wait_for_load_state("networkidle", timeout=self.timeout)
                return True
            
            # Try to find and click on Menu tab
            menu_tab_selectors = [
                '//button[contains(text(), "Menu")]',
                '//div[contains(@class, "tab-menu")]',
                '//div[contains(@class, "vendor-tabs")]//button[contains(text(), "Menu")]'
            ]
            
            working_selector = await self.check_selectors(page, menu_tab_selectors, "Menu tab")
            if working_selector:
                self.debug_print(f"Clicking Menu tab: {working_selector}")
                await page.click(working_selector)
                await page.wait_for_load_state("networkidle", timeout=self.timeout)
                return True
                
            self.debug_print("Could not navigate to all categories view")
            return False
        except Exception as e:
            self.debug_print(f"Error navigating to all categories: {e}")
            return False

    async def extract_info_from_meta(self, page):
        """Try to extract grocery info from meta tags or JSON-LD data"""
        try:
            # Check for structured data in the page
            script_data = await page.evaluate('''() => {
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                const data = [];
                scripts.forEach(script => {
                    try {
                        data.push(JSON.parse(script.textContent));
                    } catch (e) {
                        // Ignore parse errors
                    }
                });
                return data;
            }''')
            
            if script_data and len(script_data) > 0:
                self.debug_print(f"Found {len(script_data)} JSON-LD scripts")
                
                for data in script_data:
                    if isinstance(data, dict):
                        if '@type' in data and data['@type'] == 'Restaurant':
                            self.debug_print("Found Restaurant structured data")
                            return {
                                "name": data.get('name', 'Unknown'),
                                "delivery_fees": data.get('deliveryCharge', 'N/A'),
                                "minimum_order": data.get('minimumOrderValue', 'N/A'),
                                "categories": []
                            }
            
            # Try to extract from meta tags
            meta_title = await page.evaluate('''() => {
                const metaTitle = document.querySelector('meta[property="og:title"]');
                return metaTitle ? metaTitle.getAttribute('content') : null;
            }''')
            
            if meta_title:
                self.debug_print(f"Found meta title: {meta_title}")
                return {
                    "name": meta_title,
                    "delivery_fees": "N/A",
                    "minimum_order": "N/A",
                    "categories": []
                }
                
            return None
        except Exception as e:
            self.debug_print(f"Error extracting info from meta: {e}")
            return None

    async def get_delivery_info(self, page):
        """Extract delivery fees and minimum order amount"""
        delivery_fees = "N/A"
        minimum_order = "N/A"
        
        try:
            # Try multiple selectors for delivery fees
            fee_selectors = [
                '//div[contains(@class, "delivery-fee")]/span',
                '//span[contains(text(), "Delivery")]/following-sibling::span',
                'xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]',
                '//div[contains(@class, "vendor-info")]//span[contains(text(), "KD")]',
                '//div[contains(@class, "fee-label")]'
            ]
            
            working_selector = await self.check_selectors(page, fee_selectors, "delivery fees")
            if working_selector:
                element = await page.query_selector(working_selector)
                if element:
                    text = await element.inner_text()
                    if "KD" in text:
                        delivery_fees = text.strip()
                    else:
                        # Look for KD pattern in the text
                        match = re.search(r'KD\s+[\d\.]+', text)
                        if match:
                            delivery_fees = match.group(0)
                            
            # Try multiple selectors for minimum order
            order_selectors = [
                '//div[contains(@class, "min-order")]/span',
                '//span[contains(text(), "Min. order")]/following-sibling::span',
                'xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]',
                '//div[contains(@class, "min-order")]//span[contains(text(), "KD")]',
                '//div[contains(text(), "Min. Order")]'
            ]
            
            working_selector = await self.check_selectors(page, order_selectors, "minimum order")
            if working_selector:
                element = await page.query_selector(working_selector)
                if element:
                    text = await element.inner_text()
                    if "KD" in text:
                        minimum_order = text.strip()
                    else:
                        # Look for KD pattern in the text
                        match = re.search(r'KD\s+[\d\.]+', text)
                        if match:
                            minimum_order = match.group(0)
                            
        except Exception as e:
            self.debug_print(f"Error getting delivery info: {e}")
            
        return delivery_fees, minimum_order

    async def extract_categories_from_page(self, page):
        """Extract categories from different page structures"""
        categories_data = []
        
        try:
            # Check if we can find category elements
            category_selectors = [
                '//div[contains(@class, "category-item-component")]',
                '//div[contains(@class, "category-name")]',
                '//span[@data-testid="category-name"]',
                '//h2[contains(@class, "category-title")]',
                '//div[contains(@class, "categories-list")]//div[contains(@class, "category-section")]'
            ]
            
            # Check all category selectors
            working_selector = await self.check_selectors(page, category_selectors, "categories")
            
            if working_selector:
                self.debug_print(f"Found category selector: {working_selector}")
                
                # Take screenshot before extracting categories
                await self.save_screenshot(page, "before_categories")
                
                # Extract categories based on the working selector
                category_elements = await page.query_selector_all(working_selector)
                self.debug_print(f"Found {len(category_elements)} category elements")
                
                for i, element in enumerate(category_elements):
                    try:
                        # Extract category name - try different approaches
                        name_selectors = [
                            './/span[@data-testid="category-name"]',
                            './/div[contains(@class, "category-name")]',
                            './/h2',
                            './/*[contains(@class, "title")]'
                        ]
                        
                        category_name = f"Category {i+1}"
                        for selector in name_selectors:
                            name_element = await element.query_selector(selector)
                            if name_element:
                                name_text = await name_element.inner_text()
                                if name_text and name_text.strip():
                                    category_name = name_text.strip()
                                    break
                        
                        self.debug_print(f"Processing category: {category_name}")
                        
                        # Create placeholder link for category
                        category_link = f"{self.url}#{category_name.replace(' ', '-')}"
                        
                        # Try to find subcategories
                        subcategory_selectors = [
                            './/div[contains(@class, "subcategory")]//a',
                            './/div[contains(@class, "sub-category")]//a',
                            './/div[@data-test="sub-category-container"]//a'
                        ]
                        
                        sub_categories = []
                        subcategory_elements = []
                        
                        for selector in subcategory_selectors:
                            subcategory_elements = await element.query_selector_all(selector)
                            if subcategory_elements and len(subcategory_elements) > 0:
                                break
                        
                        if subcategory_elements and len(subcategory_elements) > 0:
                            self.debug_print(f"Found {len(subcategory_elements)} subcategories")
                            
                            for j, sub_element in enumerate(subcategory_elements):
                                try:
                                    sub_name = await sub_element.inner_text()
                                    sub_link = self.base_url + await sub_element.get_attribute('href')
                                    
                                    self.debug_print(f"  Subcategory {j+1}: {sub_name}")
                                    
                                    sub_categories.append({
                                        "sub_category_name": sub_name,
                                        "sub_category_link": sub_link,
                                        "Items": []  # We'll leave items empty for now
                                    })
                                except Exception as e:
                                    self.debug_print(f"  Error processing subcategory {j+1}: {e}")
                        
                        # If no subcategories found, create a default one
                        if not sub_categories:
                            self.debug_print("  No subcategories found, creating default subcategory")
                            sub_categories.append({
                                "sub_category_name": f"All {category_name}",
                                "sub_category_link": category_link,
                                "Items": []
                            })
                        
                        categories_data.append({
                            "name": category_name,
                            "link": category_link,
                            "sub_categories": sub_categories
                        })
                        
                    except Exception as e:
                        self.debug_print(f"Error processing category {i+1}: {e}")
            else:
                # If no categories found, try to look for items directly
                self.debug_print("No categories found, looking for items directly")
                
                item_selectors = [
                    '//div[contains(@class, "item-card")]',
                    '//div[contains(@class, "product-card")]',
                    '//div[contains(@class, "restaurant-item")]'
                ]
                
                working_item_selector = await self.check_selectors(page, item_selectors, "items")
                
                if working_item_selector:
                    self.debug_print(f"Found items directly with selector: {working_item_selector}")
                    
                    # Create a single category for all items
                    categories_data.append({
                        "name": "All Items",
                        "link": self.url,
                        "sub_categories": [{
                            "sub_category_name": "All Items",
                            "sub_category_link": self.url,
                            "Items": []  # We'll leave items empty for now
                        }]
                    })
                else:
                    self.debug_print("No items found directly")
        except Exception as e:
            self.debug_print(f"Error extracting categories: {e}")
            
        return categories_data

    async def extract_categories(self, page):
        """Main method to extract all grocery data"""
        try:
            print(f"Processing grocery: {self.url}")
            
            # Navigate to the URL and wait for the page to load
            self.debug_print(f"Navigating to {self.url}")
            await page.goto(self.url, timeout=self.timeout)
            await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            # Save a screenshot for debugging
            await self.save_screenshot(page, "initial_page")
            
            # Try to navigate to all categories view
            self.debug_print("Attempting to navigate to all categories view")
            await self.navigate_to_all_categories(page)
            
            # Save another screenshot after navigation
            await self.save_screenshot(page, "after_navigation")
            
            # Extract delivery fees and minimum order
            self.debug_print("Extracting delivery info")
            delivery_fees, minimum_order = await self.get_delivery_info(page)
            
            print(f"  Delivery fees: {delivery_fees}")
            print(f"  Minimum order: {minimum_order}")
            
            # Try to extract categories
            self.debug_print("Extracting categories")
            categories = await self.extract_categories_from_page(page)
            
            if categories:
                print(f"  Found {len(categories)} categories")
                
                # Save the original categories page content for debugging
                page_content = await page.content()
                with open(f"page_content_{int(time.time())}.html", "w", encoding="utf-8") as f:
                    f.write(page_content)
                self.debug_print("Saved page content to file")
                
                # Try loading a few categories to gather more info
                if len(categories) > 0:
                    for i, category in enumerate(categories[:2]):  # Try first 2 categories
                        for j, subcategory in enumerate(category['sub_categories'][:1]):  # Try first subcategory
                            # Check if we have a valid subcategory link
                            if subcategory['sub_category_link'] and not subcategory['sub_category_link'].startswith(f"{self.url}#"):
                                try:
                                    self.debug_print(f"Navigating to subcategory: {subcategory['sub_category_name']}")
                                    await page.goto(subcategory['sub_category_link'], timeout=self.timeout)
                                    await page.wait_for_load_state("networkidle", timeout=self.timeout)
                                    
                                    # Save screenshot of subcategory page
                                    await self.save_screenshot(page, f"subcategory_{i}_{j}")
                                    
                                    # Try to find items
                                    item_selectors = [
                                        '//div[contains(@class, "item-card")]',
                                        '//div[contains(@class, "product-card")]',
                                        '//a[contains(@data-testid, "item-link")]',
                                        '//div[contains(@class, "item-container")]'
                                    ]
                                    
                                    working_selector = await self.check_selectors(page, item_selectors, "items in subcategory")
                                    
                                    if working_selector:
                                        item_elements = await page.query_selector_all(working_selector)
                                        self.debug_print(f"Found {len(item_elements)} items in subcategory")
                                        
                                        # Add sample items
                                        items = []
                                        for k, item_element in enumerate(item_elements[:5]):  # Limit to 5 items for testing
                                            try:
                                                # Extract item name
                                                name_selectors = [
                                                    './/h3',
                                                    './/div[contains(@class, "item-name")]',
                                                    './/div[contains(@class, "name")]'
                                                ]
                                                
                                                item_name = f"Item {k+1}"
                                                for selector in name_selectors:
                                                    name_element = await item_element.query_selector(selector)
                                                    if name_element:
                                                        name_text = await name_element.inner_text()
                                                        if name_text and name_text.strip():
                                                            item_name = name_text.strip()
                                                            break
                                                
                                                # Extract item price
                                                price_selectors = [
                                                    './/span[contains(@class, "price")]',
                                                    './/div[contains(@class, "price")]',
                                                    './/div[contains(@class, "amount")]'
                                                ]
                                                
                                                item_price = "N/A"
                                                for selector in price_selectors:
                                                    price_element = await item_element.query_selector(selector)
                                                    if price_element:
                                                        price_text = await price_element.inner_text()
                                                        if price_text and price_text.strip():
                                                            item_price = price_text.strip()
                                                            break
                                                
                                                items.append({
                                                    "item_name": item_name,
                                                    "item_link": f"{subcategory['sub_category_link']}#{item_name.replace(' ', '-')}",
                                                    "item_price": item_price,
                                                    "item_description": "N/A",
                                                    "item_delivery_time_range": "N/A",
                                                    "item_images": []
                                                })
                                            except Exception as e:
                                                self.debug_print(f"Error processing item {k+1}: {e}")
                                        
                                        # Update subcategory with items
                                        subcategory['Items'] = items
                                        
                                except Exception as e:
                                    self.debug_print(f"Error navigating to subcategory: {e}")
            else:
                print("  No categories found")
                
                # Try to extract basic info from meta tags
                meta_info = await self.extract_info_from_meta(page)
                if meta_info:
                    self.debug_print("Using meta info as fallback")
                    return meta_info
            
            return {
                "delivery_fees": delivery_fees,
                "minimum_order": minimum_order,
                "categories": categories
            }
            
        except Exception as e:
            print(f"Error in extract_categories: {e}")
            return {
                "delivery_fees": "N/A",
                "minimum_order": "N/A",
                "categories": []
            }



# import asyncio
# import json
# import os
# import re
# import nest_asyncio  # Import nest_asyncio to allow nested event loops
# from bs4 import BeautifulSoup
# import pandas as pd
# from playwright.async_api import async_playwright

# class TalabatGroceries:
#     def __init__(self, url):
#         self.url = url
#         self.base_url = "https://www.talabat.com"

#     async def get_general_link(self, page):
#         try:
#             link_element = await page.get_attribute('//a[@data-testid="view-all-link"]', 'href')
#             if link_element:
#                 full_link = self.base_url + link_element
#                 return full_link
#             else:
#                 return None
#         except Exception as e:
#             print(f"Error getting general link: {e}")
#             return None

#     async def get_delivery_fees(self, page):
#         try:
#             delivery_fees_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]')
#             delivery_fees = await delivery_fees_element.inner_text() if delivery_fees_element else "N/A"
#             return delivery_fees
#         except Exception as e:
#             print(f"Error getting delivery fees: {e}")
#             return "N/A"

#     async def get_minimum_order(self, page):
#         try:
#             minimum_order_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]')
#             minimum_order = await minimum_order_element.inner_text() if minimum_order_element else "N/A"
#             return minimum_order
#         except Exception as e:
#             print(f"Error getting minimum order: {e}")
#             return "N/A"

#     async def extract_category_names(self, page):
#         try:
#             category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
#             category_names = [await element.inner_text() for element in category_name_elements]
#             return category_names
#         except Exception as e:
#             print(f"Error extracting category names: {e}")
#             return []

#     async def extract_category_links(self, page):
#         try:
#             category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
#             category_links = [self.base_url + await element.get_attribute('href') for element in category_link_elements]
#             return category_links
#         except Exception as e:
#             print(f"Error extracting category links: {e}")
#             return []

#     async def extract_sub_categories(self, page, category_xpath):
#         try:
#             sub_category_elements = await page.query_selector_all(f'{category_xpath}//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
#             sub_categories = []
#             for element in sub_category_elements:
#                 sub_category_name = await element.inner_text()
#                 sub_category_link = self.base_url + await element.get_attribute('href')
#                 print(f"    Processing sub-category: {sub_category_name}")
#                 items = await self.extract_all_items_from_sub_category(sub_category_link)
#                 sub_categories.append({
#                     "sub_category_name": sub_category_name,
#                     "sub_category_link": sub_category_link,
#                     "Items": items
#                 })
#             return sub_categories
#         except Exception as e:
#             print(f"Error extracting sub-categories: {e}")
#             return []

#     async def extract_item_details(self, item_link):
#         try:
#             async with async_playwright() as p:
#                 browser = await p.chromium.launch(headless=True)
#                 page = await browser.new_page()
#                 await page.goto(item_link, timeout=60000)

#                 # Wait for the page to load
#                 await page.wait_for_load_state("networkidle", timeout=60000)

#                 item_price_element = await page.query_selector('//div[@class="price"]//span[@class="currency "]')
#                 item_price = await item_price_element.inner_text() if item_price_element else "N/A"

#                 item_description_element = await page.query_selector('//div[@class="description"]//p[@data-testid="item-description"]')
#                 item_description = await item_description_element.inner_text() if item_description_element else "N/A"

#                 delivery_time_element = await page.query_selector('//div[@data-testid="delivery-tag"]//span')
#                 delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"

#                 item_image_elements = await page.query_selector_all('//div[@data-testid="item-image"]//img')
#                 item_images = [await img.get_attribute('src') for img in item_image_elements]

#                 await browser.close()

#                 return {
#                     "item_price": item_price,
#                     "item_description": item_description,
#                     "item_delivery_time_range": delivery_time,
#                     "item_images": item_images
#                 }
#         except Exception as e:
#             print(f"Error extracting item details for {item_link}: {e}")
#             return {
#                 "item_price": "N/A",
#                 "item_description": "N/A",
#                 "item_delivery_time_range": "N/A",
#                 "item_images": []
#             }

#     async def extract_all_items_from_sub_category(self, sub_category_link):
#         try:
#             async with async_playwright() as p:
#                 browser = await p.chromium.launch(headless=True)
#                 page = await browser.new_page()
#                 await page.goto(sub_category_link, timeout=60000)

#                 # Wait for the page to load
#                 await page.wait_for_load_state("networkidle", timeout=60000)

#                 # Check for pagination
#                 pagination_element = await page.query_selector('//div[@class="sc-104fa483-0 fCcIDQ"]//ul[@class="paginate-wrap"]')
#                 total_pages = 1

#                 if pagination_element:
#                     page_numbers = await pagination_element.query_selector_all('//li[contains(@class, "paginate-li f-16 f-500")]//a')
#                     total_pages = len(page_numbers) if page_numbers else 1

#                 print(f"      Found {total_pages} pages in this sub-category")

#                 items = []
#                 for page_number in range(1, total_pages + 1):
#                     print(f"      Processing page {page_number} of {total_pages}")
#                     page_url = f"{sub_category_link}&page={page_number}"
#                     await page.goto(page_url, timeout=60000)
#                     await page.wait_for_load_state("networkidle", timeout=60000)

#                     item_elements = await page.query_selector_all('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]//a[@data-testid="grocery-item-link-nofollow"]')
#                     print(f"        Found {len(item_elements)} items on page {page_number}")

#                     for i, element in enumerate(item_elements):
#                         try:
#                             item_name_element = await element.query_selector('div[data-test="item-name"]')
#                             item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {i+1}"

#                             item_link = self.base_url + await element.get_attribute('href')
#                             print(f"        Processing item {i+1}/{len(item_elements)}: {item_name}")

#                             item_details = await self.extract_item_details(item_link)

#                             items.append({
#                                 "item_name": item_name,
#                                 "item_link": item_link,
#                                 **item_details
#                             })
#                         except Exception as e:
#                             print(f"        Error processing item {i+1}: {e}")

#                 await browser.close()
#                 return items
#         except Exception as e:
#             print(f"Error extracting items from sub-category {sub_category_link}: {e}")
#             return []

#     async def extract_categories(self, page):
#         try:
#             print(f"Processing grocery: {self.url}")
#             await page.goto(self.url, timeout=60000)
#             await page.wait_for_load_state("networkidle", timeout=60000)

#             # Get general information
#             delivery_fees = await self.get_delivery_fees(page)
#             minimum_order = await self.get_minimum_order(page)
#             view_all_link = await self.get_general_link(page)

#             print(f"  Delivery fees: {delivery_fees}")
#             print(f"  Minimum order: {minimum_order}")

#             # Extract categories
#             if view_all_link:
#                 print(f"  Navigating to view all link: {view_all_link}")
#                 await page.goto(view_all_link, timeout=60000)
#                 await page.wait_for_load_state("networkidle", timeout=60000)

#             category_names = await self.extract_category_names(page)
#             category_links = await self.extract_category_links(page)

#             print(f"  Found {len(category_names)} categories")

#             categories_data = []
#             for index, (name, link) in enumerate(zip(category_names, category_links)):
#                 print(f"  Processing category {index+1}/{len(category_names)}: {name}")
#                 category_xpath = f'//div[@data-testid="category-item-component"][{index + 1}]'
#                 sub_categories = await self.extract_sub_categories(page, category_xpath)
#                 category_data = {
#                     "name": name,
#                     "link": link,
#                     "sub_categories": sub_categories
#                 }
#                 categories_data.append(category_data)

#             grocery_data = {
#                 "delivery_fees": delivery_fees,
#                 "minimum_order": minimum_order,
#                 "categories": categories_data
#             }

#             return grocery_data
#         except Exception as e:
#             print(f"Error extracting categories: {e}")
#             return {"error": str(e)}

