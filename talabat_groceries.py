import asyncio
import json
import os
import re
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page

class TalabatGroceries:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.talabat.com"
        self.timeout = 45000  # Increased timeout to 45 seconds
        
    async def get_general_link(self, page):
        try:
            # Wait for selector with increased timeout and log progress
            print("  Looking for view-all-link element...")
            await page.wait_for_selector('//a[@data-testid="view-all-link"]', timeout=self.timeout)
            link_element = await page.get_attribute('//a[@data-testid="view-all-link"]', 'href')
            if link_element:
                full_link = self.base_url + link_element
                return full_link
            else:
                print("  No view-all-link found, continuing with current page")
                return None
        except Exception as e:
            print(f"  Warning: Could not get general link: {e}")
            print("  Continuing with current page")
            return None

    async def get_delivery_fees(self, page):
        try:
            # Try multiple selectors for better reliability
            selectors = [
                '//div[contains(@class, "delivery-fee")]/span',
                '//span[contains(text(), "Delivery")]/following-sibling::span',
                'xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]'
            ]
            
            for selector in selectors:
                try:
                    await page.wait_for_selector(selector, timeout=10000)
                    element = await page.query_selector(selector)
                    if element:
                        return await element.inner_text()
                except:
                    continue
                    
            return "N/A"
        except Exception as e:
            print(f"  Warning: Could not get delivery fees: {e}")
            return "N/A"

    async def get_minimum_order(self, page):
        try:
            # Try multiple selectors for better reliability
            selectors = [
                '//div[contains(@class, "min-order")]/span',
                '//span[contains(text(), "Min. order")]/following-sibling::span',
                'xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]'
            ]
            
            for selector in selectors:
                try:
                    await page.wait_for_selector(selector, timeout=10000)
                    element = await page.query_selector(selector)
                    if element:
                        return await element.inner_text()
                except:
                    continue
                    
            return "N/A"
        except Exception as e:
            print(f"  Warning: Could not get minimum order: {e}")
            return "N/A"

    async def extract_category_names(self, page):
        try:
            # Wait for DOM to fully load
            await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            # Try alternative selectors if the primary one fails
            try:
                await page.wait_for_selector('//span[@data-testid="category-name"]', timeout=15000)
                category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
            except:
                print("  Using alternative category name selector")
                await page.wait_for_selector('//h2[contains(@class, "category-title")]', timeout=15000)
                category_name_elements = await page.query_selector_all('//h2[contains(@class, "category-title")]')
            
            # Extract category names
            category_names = []
            for element in category_name_elements:
                name = await element.inner_text()
                if name and name.strip():
                    category_names.append(name.strip())
            
            return category_names
        except Exception as e:
            print(f"  Warning: Could not extract category names: {e}")
            return []

    async def extract_category_links(self, page):
        try:
            # Try different selectors
            try:
                await page.wait_for_selector('//a[@data-testid="category-item-container"]', timeout=15000)
                category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
            except:
                print("  Using alternative category link selector")
                await page.wait_for_selector('//div[contains(@class, "category-container")]//a', timeout=15000)
                category_link_elements = await page.query_selector_all('//div[contains(@class, "category-container")]//a')
            
            # Extract links
            category_links = []
            for element in category_link_elements:
                href = await element.get_attribute('href')
                if href:
                    full_link = self.base_url + href
                    category_links.append(full_link)
            
            return category_links
        except Exception as e:
            print(f"  Warning: Could not extract category links: {e}")
            return []

    async def extract_sub_categories(self, page, category_xpath):
        try:
            # Try different selectors for sub-categories
            sub_category_elements = []
            
            selectors = [
                f'{category_xpath}//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]',
                f'{category_xpath}//div[contains(@class, "sub-category")]//a',
                f'//div[contains(@class, "subcategory-container")]//a'
            ]
            
            for selector in selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    if elements and len(elements) > 0:
                        sub_category_elements = elements
                        break
                except:
                    continue
            
            if not sub_category_elements:
                print(f"    No sub-categories found using available selectors")
                return []
            
            sub_categories = []
            for element in sub_category_elements:
                try:
                    sub_category_name = await element.inner_text()
                    sub_category_link = self.base_url + await element.get_attribute('href')
                    
                    if not sub_category_name or not sub_category_link:
                        continue
                        
                    print(f"    Processing sub-category: {sub_category_name}")
                    
                    # Create a new page for each sub-category to avoid state contamination
                    browser = page.context.browser
                    context = await browser.new_context()
                    sub_page = await context.new_page()
                    
                    items = await self.extract_all_items_from_sub_category(sub_page, sub_category_link)
                    await sub_page.close()
                    await context.close()
                    
                    sub_categories.append({
                        "sub_category_name": sub_category_name,
                        "sub_category_link": sub_category_link,
                        "Items": items
                    })
                except Exception as e:
                    print(f"    Error processing sub-category: {e}")
                    continue
                    
            return sub_categories
        except Exception as e:
            print(f"  Error extracting sub-categories: {e}")
            return []

    async def extract_item_details(self, page, item_link):
        try:
            await page.goto(item_link, timeout=self.timeout)
            await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            # Extract price
            item_price = "N/A"
            try:
                price_selectors = [
                    '//div[@class="price"]//span[@class="currency "]',
                    '//span[contains(@class, "price")]',
                    '//div[contains(@class, "price-container")]//span'
                ]
                
                for selector in price_selectors:
                    element = await page.query_selector(selector)
                    if element:
                        item_price = await element.inner_text()
                        break
            except:
                pass
                
            # Extract description
            item_description = "N/A"
            try:
                desc_selectors = [
                    '//div[@class="description"]//p[@data-testid="item-description"]',
                    '//p[contains(@class, "description")]',
                    '//div[contains(@class, "description")]'
                ]
                
                for selector in desc_selectors:
                    element = await page.query_selector(selector)
                    if element:
                        item_description = await element.inner_text()
                        break
            except:
                pass
                
            # Extract delivery time
            delivery_time = "N/A"
            try:
                time_selectors = [
                    '//div[@data-testid="delivery-tag"]//span',
                    '//span[contains(@class, "delivery-time")]',
                    '//div[contains(@class, "delivery")]//span'
                ]
                
                for selector in time_selectors:
                    element = await page.query_selector(selector)
                    if element:
                        delivery_time = await element.inner_text()
                        break
            except:
                pass
                
            # Extract images
            item_images = []
            try:
                img_selectors = [
                    '//div[@data-testid="item-image"]//img',
                    '//img[contains(@class, "item-image")]',
                    '//div[contains(@class, "item-image-container")]//img'
                ]
                
                for selector in img_selectors:
                    elements = await page.query_selector_all(selector)
                    if elements and len(elements) > 0:
                        for img in elements:
                            src = await img.get_attribute('src')
                            if src:
                                item_images.append(src)
                        break
            except:
                pass

            return {
                "item_price": item_price,
                "item_description": item_description,
                "item_delivery_time_range": delivery_time,
                "item_images": item_images
            }
            
        except Exception as e:
            print(f"      Error extracting item details for {item_link}: {e}")
            return {
                "item_price": "N/A",
                "item_description": "N/A",
                "item_delivery_time_range": "N/A",
                "item_images": []
            }

    async def extract_all_items_from_sub_category(self, page, sub_category_link):
        try:
            await page.goto(sub_category_link, timeout=self.timeout)
            await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            # Check for pagination
            total_pages = 1
            try:
                pagination_selectors = [
                    '//div[@class="sc-104fa483-0 fCcIDQ"]//ul[@class="paginate-wrap"]',
                    '//ul[contains(@class, "paginate")]',
                    '//div[contains(@class, "pagination")]'
                ]
                
                for selector in pagination_selectors:
                    pagination_element = await page.query_selector(selector)
                    if pagination_element:
                        page_numbers = await pagination_element.query_selector_all('//li[contains(@class, "paginate-li")]//a')
                        if page_numbers and len(page_numbers) > 0:
                            total_pages = len(page_numbers)
                            break
            except:
                total_pages = 1

            # If pagination detection failed, try to locate next page button
            if total_pages == 1:
                try:
                    next_button = await page.query_selector('//a[contains(@class, "next")]')
                    if next_button:
                        # If next button exists, set pages to 3 for initial crawl
                        total_pages = 3
                except:
                    pass
                
            print(f"      Found {total_pages} pages in this sub-category")
            
            items = []
            for page_number in range(1, total_pages + 1):
                print(f"      Processing page {page_number} of {total_pages}")
                
                if page_number > 1:
                    page_url = f"{sub_category_link}&page={page_number}"
                    await page.goto(page_url, timeout=self.timeout)
                    await page.wait_for_load_state("networkidle", timeout=self.timeout)
                
                # Try multiple selectors for items
                item_elements = []
                item_selectors = [
                    '//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]//a[@data-testid="grocery-item-link-nofollow"]',
                    '//a[contains(@data-testid, "grocery-item")]',
                    '//div[contains(@class, "item-container")]//a',
                    '//div[contains(@class, "product-card")]//a'
                ]
                
                for selector in item_selectors:
                    try:
                        elements = await page.query_selector_all(selector)
                        if elements and len(elements) > 0:
                            item_elements = elements
                            break
                    except:
                        continue
                
                print(f"        Found {len(item_elements)} items on page {page_number}")
                
                # Skip if no items found on this page
                if len(item_elements) == 0:
                    # Check if we should stop pagination early
                    if page_number > 1:
                        print("        No items found on this page, stopping pagination")
                        break
                    else:
                        # For the first page, try harder with a page refresh
                        print("        Trying page refresh...")
                        await page.reload()
                        await page.wait_for_load_state("networkidle", timeout=self.timeout)
                        
                        # Try again with all selectors
                        for selector in item_selectors:
                            try:
                                elements = await page.query_selector_all(selector)
                                if elements and len(elements) > 0:
                                    item_elements = elements
                                    print(f"        After refresh: Found {len(item_elements)} items")
                                    break
                            except:
                                continue
                
                # Process items
                for i, element in enumerate(item_elements):
                    try:
                        # Try to get item name
                        item_name_element = None
                        name_selectors = [
                            'div[data-test="item-name"]',
                            'div[contains(@class, "item-name")]',
                            'span[contains(@class, "name")]'
                        ]
                        
                        for selector in name_selectors:
                            item_name_element = await element.query_selector(selector)
                            if item_name_element:
                                break
                                
                        item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {i+1}"
                        
                        # Get item link
                        item_link = self.base_url + await element.get_attribute('href')
                        print(f"        Processing item {i+1}/{len(item_elements)}: {item_name}")
                        
                        # Create a new context for item details
                        browser = page.context.browser
                        context = await browser.new_context()
                        detail_page = await context.new_page()
                        
                        # Get item details
                        item_details = await self.extract_item_details(detail_page, item_link)
                        await detail_page.close()
                        await context.close()
                        
                        items.append({
                            "item_name": item_name,
                            "item_link": item_link,
                            **item_details
                        })
                    except Exception as e:
                        print(f"        Error processing item {i+1}: {e}")
                
                # Check if we've reached the item limit for testing purposes
                if len(items) >= 100:
                    print("        Reached item limit (100), stopping pagination")
                    break
            
            return items
        except Exception as e:
            print(f"      Error extracting items from sub-category {sub_category_link}: {e}")
            return []

    async def extract_categories(self, page):
        try:
            print(f"Processing grocery: {self.url}")
            await page.goto(self.url, timeout=self.timeout)
            await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            # Get general information
            delivery_fees = await self.get_delivery_fees(page)
            minimum_order = await self.get_minimum_order(page)
            view_all_link = await self.get_general_link(page)
            
            print(f"  Delivery fees: {delivery_fees}")
            print(f"  Minimum order: {minimum_order}")
            
            # Extract categories - navigate to view all link if available
            if view_all_link:
                print(f"  Navigating to view all link: {view_all_link}")
                await page.goto(view_all_link, timeout=self.timeout)
                await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            category_names = await self.extract_category_names(page)
            category_links = await self.extract_category_links(page)
            
            # Make sure category names and links match in length
            min_length = min(len(category_names), len(category_links))
            category_names = category_names[:min_length]
            category_links = category_links[:min_length]
            
            print(f"  Found {len(category_names)} categories")
            
            categories_data = []
            for index, (name, link) in enumerate(zip(category_names, category_links)):
                print(f"  Processing category {index+1}/{len(category_names)}: {name}")
                
                # Use a different strategy for finding the category xpath
                category_xpath = f'//div[@data-testid="category-item-component"][{index + 1}]'
                
                # Also try alternative expressions if needed
                alternative_xpaths = [
                    f'(//div[contains(@class, "category-item")])[{index + 1}]',
                    f'(//h2[contains(text(), "{name}")])[1]/ancestor::div[contains(@class, "category")]'
                ]
                
                # Try the main xpath first, then alternatives
                sub_categories = await self.extract_sub_categories(page, category_xpath)
                
                if not sub_categories:
                    for alt_xpath in alternative_xpaths:
                        print(f"    Trying alternative xpath for category")
                        sub_categories = await self.extract_sub_categories(page, alt_xpath)
                        if sub_categories:
                            break
                
                category_data = {
                    "name": name,
                    "link": link,
                    "sub_categories": sub_categories
                }
                categories_data.append(category_data)
                
                # Save progress after processing each category
                # You can implement a callback here for saving progress if needed
            
            grocery_data = {
                "delivery_fees": delivery_fees,
                "minimum_order": minimum_order,
                "categories": categories_data
            }
            
            return grocery_data
        except Exception as e:
            print(f"Error extracting categories: {e}")
            return {"error": str(e)}


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

