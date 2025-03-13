import asyncio
import json
import re
import time
from urllib.parse import urlparse, parse_qs
from playwright.async_api import async_playwright, Page, TimeoutError

class TalabatGroceries:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.talabat.com"
        self.timeout = 45000  # 45 seconds
        self.api_timeout = 60000  # 60 seconds for API calls
        self.api_base_url = "https://www.talabat.com/api/v3/vendors"
        self.api_headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        }
    
    def extract_vendor_id(self, url):
        """Extract vendor ID from URL"""
        try:
            # Parse the URL
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip('/').split('/')
            
            # Look for numeric ID in the path
            for part in path_parts:
                if part.isdigit():
                    return part
            
            # Try to extract from query parameters
            query_params = parse_qs(parsed_url.query)
            if 'id' in query_params:
                return query_params['id'][0]
                
            # Try to find vendor ID in the path segment before query
            if len(path_parts) >= 2:
                # Sometimes the ID is part of the restaurant name slug
                match = re.search(r'(\d+)', path_parts[-1])
                if match:
                    return match.group(1)
            
            print(f"  Warning: Could not extract vendor ID from URL: {url}")
            return None
        except Exception as e:
            print(f"  Error extracting vendor ID: {e}")
            return None
    
    async def get_api_data(self, page, endpoint):
        """Get data from Talabat API directly"""
        try:
            # Use page.evaluate to make fetch request
            script = f"""
            async () => {{
                try {{
                    const response = await fetch('{endpoint}', {{
                        method: 'GET',
                        headers: {{
                            'Accept': 'application/json',
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                        }}
                    }});
                    
                    if (!response.ok) {{
                        throw new Error(`HTTP error! Status: ${{response.status}}`);
                    }}
                    
                    return await response.json();
                }} catch (error) {{
                    console.error('Error fetching API data:', error);
                    return null;
                }}
            }}
            """
            
            result = await page.evaluate(script)
            return result
        except Exception as e:
            print(f"  Error getting API data from {endpoint}: {e}")
            return None
    
    async def extract_data_using_api(self, page):
        """Extract data using Talabat's API rather than scraping the DOM"""
        try:
            # First, extract vendor ID from the URL
            vendor_id = self.extract_vendor_id(self.url)
            if not vendor_id:
                print("  Could not extract vendor ID, falling back to DOM scraping")
                return None
                
            print(f"  Extracted vendor ID: {vendor_id}")
            
            # Construct API endpoint for vendor details
            vendor_api_url = f"{self.api_base_url}/{vendor_id}"
            print(f"  Fetching vendor details from API: {vendor_api_url}")
            
            # Get vendor details
            vendor_details = await self.get_api_data(page, vendor_api_url)
            if not vendor_details:
                print("  Failed to get vendor details from API, falling back to DOM scraping")
                return None
                
            # Extract basic info
            delivery_fees = "N/A"
            minimum_order = "N/A"
            
            try:
                if "deliveryFee" in vendor_details:
                    delivery_fees = f"KD {vendor_details['deliveryFee']:.3f}"
                
                if "minimumOrderValue" in vendor_details:
                    minimum_order = f"KD {vendor_details['minimumOrderValue']:.3f}"
            except:
                pass
                
            print(f"  API Delivery fees: {delivery_fees}")
            print(f"  API Minimum order: {minimum_order}")
            
            # Get menu categories from API
            menu_api_url = f"{self.api_base_url}/{vendor_id}/menus"
            print(f"  Fetching menu from API: {menu_api_url}")
            
            menu_data = await self.get_api_data(page, menu_api_url)
            if not menu_data:
                print("  Failed to get menu data from API, falling back to DOM scraping")
                return None
                
            # Process categories
            categories_data = []
            
            # Check if menu data has a categories array
            if not menu_data or "categories" not in menu_data:
                print("  No categories found in API response")
                return {
                    "delivery_fees": delivery_fees,
                    "minimum_order": minimum_order,
                    "categories": []
                }
                
            print(f"  Found {len(menu_data['categories'])} categories via API")
            
            # Process categories and items
            for category in menu_data['categories']:
                category_name = category.get('name', 'Unknown')
                category_id = category.get('id', '')
                
                print(f"  Processing category: {category_name}")
                
                # Create category link
                category_link = f"{self.url}/menu/{category_id}"
                
                # Process sub-categories or items
                sub_categories = []
                
                # Some categories have sub-categories, others have items directly
                if 'subcategories' in category and category['subcategories']:
                    for subcategory in category['subcategories']:
                        subcategory_name = subcategory.get('name', 'Unknown')
                        subcategory_id = subcategory.get('id', '')
                        
                        print(f"    Processing sub-category: {subcategory_name}")
                        
                        # Create subcategory link
                        subcategory_link = f"{self.url}/menu/{category_id}/{subcategory_id}"
                        
                        # Get items
                        items = []
                        
                        if 'items' in subcategory and subcategory['items']:
                            for item in subcategory['items']:
                                item_name = item.get('name', 'Unknown Item')
                                item_id = item.get('id', '')
                                item_price = f"KD {item.get('price', 0):.3f}"
                                item_description = item.get('description', 'N/A')
                                item_link = f"{self.url}/item/{item_id}"
                                
                                # Get images
                                item_images = []
                                if 'imageUrl' in item and item['imageUrl']:
                                    item_images.append(item['imageUrl'])
                                
                                items.append({
                                    "item_name": item_name,
                                    "item_link": item_link,
                                    "item_price": item_price,
                                    "item_description": item_description,
                                    "item_delivery_time_range": vendor_details.get('deliveryTime', 'N/A'),
                                    "item_images": item_images
                                })
                                
                            print(f"      Found {len(items)} items")
                            
                        sub_categories.append({
                            "sub_category_name": subcategory_name,
                            "sub_category_link": subcategory_link,
                            "Items": items
                        })
                elif 'items' in category and category['items']:
                    # If no subcategories, create a default subcategory with the same name
                    items = []
                    
                    for item in category['items']:
                        item_name = item.get('name', 'Unknown Item')
                        item_id = item.get('id', '')
                        item_price = f"KD {item.get('price', 0):.3f}"
                        item_description = item.get('description', 'N/A')
                        item_link = f"{self.url}/item/{item_id}"
                        
                        # Get images
                        item_images = []
                        if 'imageUrl' in item and item['imageUrl']:
                            item_images.append(item['imageUrl'])
                        
                        items.append({
                            "item_name": item_name,
                            "item_link": item_link,
                            "item_price": item_price,
                            "item_description": item_description,
                            "item_delivery_time_range": vendor_details.get('deliveryTime', 'N/A'),
                            "item_images": item_images
                        })
                    
                    print(f"    Found {len(items)} items directly in category")
                    
                    sub_categories.append({
                        "sub_category_name": category_name,
                        "sub_category_link": category_link,
                        "Items": items
                    })
                
                categories_data.append({
                    "name": category_name,
                    "link": category_link,
                    "sub_categories": sub_categories
                })
            
            return {
                "delivery_fees": delivery_fees,
                "minimum_order": minimum_order,
                "categories": categories_data
            }
            
        except Exception as e:
            print(f"  Error extracting data using API: {e}")
            return None

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
                'xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]',
                '//div[contains(@class, "vendor-info")]//span[contains(text(), "KD")]'
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
                'xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]',
                '//div[contains(@class, "min-order")]//span[contains(text(), "KD")]'
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

    async def extract_categories_from_dom(self, page):
        """Extract categories using DOM scraping as fallback method"""
        try:
            # Wait for DOM to fully load
            await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            # Get general information
            delivery_fees = await self.get_delivery_fees(page)
            minimum_order = await self.get_minimum_order(page)
            
            print(f"  DOM Delivery fees: {delivery_fees}")
            print(f"  DOM Minimum order: {minimum_order}")
            
            # Extract categories - try different methods
            categories_data = []
            
            # First attempt - check for category sections
            try:
                await page.wait_for_selector('//div[contains(@class, "categories-list")]', timeout=15000)
                category_sections = await page.query_selector_all('//div[contains(@class, "categories-list")]//div[contains(@class, "category-section")]')
                
                if category_sections and len(category_sections) > 0:
                    print(f"  Found {len(category_sections)} category sections")
                    
                    for i, section in enumerate(category_sections):
                        try:
                            # Get category name
                            name_element = await section.query_selector('.//h2')
                            category_name = await name_element.inner_text() if name_element else f"Category {i+1}"
                            
                            # Get items in this category
                            item_elements = await section.query_selector_all('.//div[contains(@class, "item-card")]')
                            
                            items = []
                            for item_element in item_elements:
                                try:
                                    item_name_element = await item_element.query_selector('.//h3')
                                    item_name = await item_name_element.inner_text() if item_name_element else "Unknown Item"
                                    
                                    item_price_element = await item_element.query_selector('.//span[contains(@class, "price")]')
                                    item_price = await item_price_element.inner_text() if item_price_element else "N/A"
                                    
                                    item_desc_element = await item_element.query_selector('.//p[contains(@class, "description")]')
                                    item_description = await item_desc_element.inner_text() if item_desc_element else "N/A"
                                    
                                    # Use a placeholder link since we can't easily get the actual one
                                    item_link = f"{self.url}#{category_name.replace(' ', '-')}-{item_name.replace(' ', '-')}"
                                    
                                    items.append({
                                        "item_name": item_name,
                                        "item_link": item_link,
                                        "item_price": item_price,
                                        "item_description": item_description,
                                        "item_delivery_time_range": "N/A",
                                        "item_images": []
                                    })
                                except Exception as e:
                                    print(f"      Error processing item: {e}")
                            
                            # Create a single subcategory with items
                            sub_categories = [{
                                "sub_category_name": category_name,
                                "sub_category_link": f"{self.url}#{category_name.replace(' ', '-')}",
                                "Items": items
                            }]
                            
                            categories_data.append({
                                "name": category_name,
                                "link": f"{self.url}#{category_name.replace(' ', '-')}",
                                "sub_categories": sub_categories
                            })
                        except Exception as e:
                            print(f"    Error processing category section {i+1}: {e}")
                    
                    # If we found categories, return the data
                    if categories_data:
                        return {
                            "delivery_fees": delivery_fees,
                            "minimum_order": minimum_order,
                            "categories": categories_data
                        }
            except Exception as e:
                print(f"  First attempt to find categories failed: {e}")
            
            # Second attempt - try traditional category selectors
            try:
                # Try to find the category list
                await page.wait_for_selector('//span[@data-testid="category-name"]', timeout=15000)
                category_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
                
                if category_elements and len(category_elements) > 0:
                    print(f"  Found {len(category_elements)} categories with traditional selectors")
                    
                    for i, element in enumerate(category_elements):
                        category_name = await element.inner_text()
                        # Create placeholder links since we can't easily get the actual ones
                        category_link = f"{self.url}#{category_name.replace(' ', '-')}"
                        
                        categories_data.append({
                            "name": category_name,
                            "link": category_link,
                            "sub_categories": [{
                                "sub_category_name": f"All {category_name}",
                                "sub_category_link": category_link,
                                "Items": []  # Empty items - we can't easily get them
                            }]
                        })
                    
                    # If we found categories, return the data
                    if categories_data:
                        return {
                            "delivery_fees": delivery_fees,
                            "minimum_order": minimum_order,
                            "categories": categories_data
                        }
            except Exception as e:
                print(f"  Second attempt to find categories failed: {e}")
            
            # If we get here, we couldn't find any categories
            print("  Warning: Could not find any categories in the DOM")
            return {
                "delivery_fees": delivery_fees,
                "minimum_order": minimum_order,
                "categories": []
            }
            
        except Exception as e:
            print(f"  Error extracting categories from DOM: {e}")
            return {
                "delivery_fees": "N/A",
                "minimum_order": "N/A",
                "categories": []
            }

    async def extract_categories(self, page):
        try:
            print(f"Processing grocery: {self.url}")
            
            # First load the page
            await page.goto(self.url, timeout=self.timeout)
            await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            # Try API method first
            print("Attempting to extract data using API...")
            api_data = await self.extract_data_using_api(page)
            
            if api_data:
                print("Successfully extracted data using API")
                return api_data
                
            # If API method fails, fall back to DOM scraping
            print("API extraction failed, falling back to DOM scraping...")
            return await self.extract_categories_from_dom(page)
            
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

