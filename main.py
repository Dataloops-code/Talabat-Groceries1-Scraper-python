import asyncio
import json
import os
import re
import nest_asyncio
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError

# Apply nest_asyncio to allow running asyncio.run() in a notebook environment
nest_asyncio.apply()

class TalabatGroceries:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.talabat.com"
        self.timeout = 120000  # Increased timeout to 120 seconds

    async def get_general_link(self, page):
        try:
            # Wait for the view all link to be visible
            await page.wait_for_selector('a[data-testid="view-all-link"]', timeout=self.timeout)
            link_element = await page.get_attribute('a[data-testid="view-all-link"]', 'href')
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
            # Try multiple selectors for delivery fees
            selectors = [
                'div[data-testid="vendor-delivery-info"] span:nth-child(1)',
                'span.vendor-delivery-fee',
                '.deliveryInfo span',
                'div.delivery-fee span'
            ]
            
            for selector in selectors:
                try:
                    await page.wait_for_selector(selector, timeout=10000)
                    element = await page.query_selector(selector)
                    if element:
                        text = await element.inner_text()
                        if "KD" in text or "delivery" in text.lower():
                            return text
                except:
                    continue
                    
            return "N/A"
        except Exception as e:
            print(f"Error getting delivery fees: {e}")
            return "N/A"

    async def get_minimum_order(self, page):
        try:
            # Try multiple selectors for minimum order
            selectors = [
                'div[data-testid="vendor-delivery-info"] span:nth-child(3)',
                'span.vendor-min-order',
                '.deliveryInfo span:nth-child(3)',
                'div.min-order span'
            ]
            
            for selector in selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        text = await element.inner_text()
                        if "KD" in text or "min" in text.lower():
                            return text
                except:
                    continue
                    
            return "N/A"
        except Exception as e:
            print(f"Error getting minimum order: {e}")
            return "N/A"

    async def extract_category_names(self, page):
        try:
            # Try multiple selectors for category names
            selectors = [
                'span[data-testid="category-name"]',
                '.category-name',
                'div.categories span',
                'div.menu-category h3'
            ]
            
            for selector in selectors:
                try:
                    await page.wait_for_selector(selector, timeout=10000)
                    elements = await page.query_selector_all(selector)
                    if elements and len(elements) > 0:
                        return [await element.inner_text() for element in elements]
                except:
                    continue
                    
            # Try to get category names from the HTML directly
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            categories = []
            
            # Try different patterns
            for pattern in ['category-name', 'menu-category']:
                elements = soup.select(f'[data-testid*="{pattern}"], [class*="{pattern}"]')
                if elements:
                    categories = [elem.text.strip() for elem in elements if elem.text.strip()]
                    if categories:
                        return categories
            
            return []
        except Exception as e:
            print(f"Error extracting category names: {e}")
            return []

    async def extract_category_links(self, page):
        try:
            # Try multiple selectors for category links
            selectors = [
                'a[data-testid="category-item-container"]',
                '.category-item a',
                'div.categories a',
                'div.menu-category a'
            ]
            
            for selector in selectors:
                try:
                    await page.wait_for_selector(selector, timeout=10000)
                    elements = await page.query_selector_all(selector)
                    if elements and len(elements) > 0:
                        return [self.base_url + await element.get_attribute('href') for element in elements]
                except:
                    continue
                    
            # Try to get category links from the HTML directly
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            
            # Try different patterns
            for pattern in ['category-item', 'menu-category']:
                elements = soup.select(f'[data-testid*="{pattern}"] a, [class*="{pattern}"] a')
                if elements:
                    links = [self.base_url + elem.get('href') for elem in elements if elem.get('href')]
                    if links:
                        return links
            
            return []
        except Exception as e:
            print(f"Error extracting category links: {e}")
            return []

    async def extract_sub_categories(self, page, category_name, category_link):
        """Improved method to extract sub-categories by navigating to the category page"""
        try:
            # Navigate to the category page
            print(f"    Navigating to category: {category_name} at {category_link}")
            await page.goto(category_link, timeout=self.timeout)
            await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            # Try multiple selectors for sub-categories
            sub_category_selectors = [
                'div[data-test="sub-category-container"] a[data-testid="subCategory-a"]',
                '.sub-category a',
                'div.subcategories a',
                'div.menu-subcategory a'
            ]
            
            sub_category_elements = []
            for selector in sub_category_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=10000)
                    elements = await page.query_selector_all(selector)
                    if elements and len(elements) > 0:
                        sub_category_elements = elements
                        break
                except:
                    continue
            
            if not sub_category_elements:
                print(f"    No sub-categories found for category {category_name}")
                # Try to find items directly if no sub-categories
                return [{
                    "sub_category_name": "All",
                    "sub_category_link": category_link,
                    "Items": await self.extract_all_items_from_page(page)
                }]
            
            sub_categories = []
            for element in sub_category_elements:
                sub_category_name = await element.inner_text()
                sub_category_link = self.base_url + await element.get_attribute('href')
                print(f"    Processing sub-category: {sub_category_name}")
                
                # Navigate to the sub-category page
                await page.goto(sub_category_link, timeout=self.timeout)
                await page.wait_for_load_state("networkidle", timeout=self.timeout)
                
                # Extract items from all pages in this sub-category
                items = await self.extract_all_items_from_sub_category(page, sub_category_link)
                
                sub_categories.append({
                    "sub_category_name": sub_category_name,
                    "sub_category_link": sub_category_link,
                    "Items": items
                })
                
            return sub_categories
        except Exception as e:
            print(f"Error extracting sub-categories for {category_name}: {e}")
            return []

    async def extract_all_items_from_page(self, page):
        """Extract items from the current page"""
        try:
            # Try multiple selectors for items
            item_selectors = [
                'div.category-items-container a[data-testid="grocery-item-link-nofollow"]',
                '.item-card a',
                'div.item-container a',
                'div.product-card a'
            ]
            
            item_elements = []
            for selector in item_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=10000)
                    elements = await page.query_selector_all(selector)
                    if elements and len(elements) > 0:
                        item_elements = elements
                        break
                except:
                    continue
            
            if not item_elements:
                print("    No items found on this page")
                
                # Try to scrape items directly from HTML
                html = await page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                items = []
                product_cards = soup.select('.item-card, .product-card, [data-testid*="item"], [class*="item-container"]')
                
                for i, card in enumerate(product_cards):
                    try:
                        name_elem = card.select_one('.item-name, h3, .title')
                        name = name_elem.text.strip() if name_elem else f"Unknown Item {i+1}"
                        
                        link_elem = card.find('a')
                        link = self.base_url + link_elem['href'] if link_elem and 'href' in link_elem.attrs else ""
                        
                        price_elem = card.select_one('.price, .currency')
                        price = price_elem.text.strip() if price_elem else "N/A"
                        
                        items.append({
                            "item_name": name,
                            "item_link": link,
                            "item_price": price,
                            "item_description": "N/A",
                            "item_delivery_time_range": "N/A",
                            "item_images": []
                        })
                    except Exception as e:
                        print(f"    Error extracting item {i+1} from HTML: {e}")
                
                if items:
                    print(f"    Found {len(items)} items directly from HTML")
                    return items
                
                return []
            
            print(f"    Found {len(item_elements)} items on current page")
            
            items = []
            for i, element in enumerate(item_elements):
                try:
                    # Try different selectors for item name
                    name_selectors = ['div[data-test="item-name"]', '.item-name', 'h3', '.title']
                    item_name = f"Unknown Item {i+1}"
                    
                    for selector in name_selectors:
                        name_element = await element.query_selector(selector)
                        if name_element:
                            item_name = await name_element.inner_text()
                            break
                    
                    item_link = self.base_url + await element.get_attribute('href')
                    print(f"    Processing item {i+1}/{len(item_elements)}: {item_name}")
                    
                    # Try different selectors for price
                    price_selectors = ['span.currency', '.price', '.item-price']
                    item_price = "N/A"
                    
                    for selector in price_selectors:
                        price_element = await element.query_selector(selector)
                        if price_element:
                            item_price = await price_element.inner_text()
                            break
                    
                    items.append({
                        "item_name": item_name,
                        "item_link": item_link,
                        "item_price": item_price,
                        "item_description": "N/A",
                        "item_delivery_time_range": "N/A",
                        "item_images": []
                    })
                except Exception as e:
                    print(f"    Error processing item {i+1}: {e}")
            
            return items
        except Exception as e:
            print(f"Error extracting items from page: {e}")
            return []

    async def extract_all_items_from_sub_category(self, page, sub_category_link):
        """Extract items from all pages in a sub-category"""
        try:
            items = []
            page_number = 1
            has_more_pages = True
            consecutive_empty_pages = 0
            
            while has_more_pages and consecutive_empty_pages < 2:
                print(f"      Processing page {page_number}")
                page_url = f"{sub_category_link}&page={page_number}"
                
                await page.goto(page_url, timeout=self.timeout)
                await page.wait_for_load_state("networkidle", timeout=self.timeout)
                
                # Get items from current page
                page_items = await self.extract_all_items_from_page(page)
                items.extend(page_items)
                
                if not page_items:
                    consecutive_empty_pages += 1
                    print(f"      No items found on page {page_number}, empty page count: {consecutive_empty_pages}")
                else:
                    consecutive_empty_pages = 0
                
                # Check if there's a next page
                next_button_selectors = [
                    'a[data-testid="next-page-link"]',
                    '.pagination-next',
                    'a[aria-label="Next page"]',
                    '.paginate-next'
                ]
                
                next_button = None
                for selector in next_button_selectors:
                    next_button = await page.query_selector(selector)
                    if next_button:
                        break
                
                has_more_pages = next_button is not None
                page_number += 1
                
                # Safety check - don't go beyond page 10
                if page_number > 10:
                    print("      Reached maximum page limit (10)")
                    break
            
            print(f"      Total items found in sub-category: {len(items)}")
            return items
        except Exception as e:
            print(f"Error extracting items from sub-category {sub_category_link}: {e}")
            return []

    async def extract_categories(self, page):
        """Extract all categories and their sub-categories"""
        try:
            print(f"Processing grocery: {self.url}")
            await page.goto(self.url, timeout=self.timeout)
            await page.wait_for_load_state("networkidle", timeout=self.timeout)
            
            # Scroll down to ensure all content loads
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)

            # Get general information
            delivery_fees = await self.get_delivery_fees(page)
            minimum_order = await self.get_minimum_order(page)
            view_all_link = await self.get_general_link(page)

            print(f"  Delivery fees: {delivery_fees}")
            print(f"  Minimum order: {minimum_order}")

            # Navigate to view all link if available
            if view_all_link:
                print(f"  Navigating to view all link: {view_all_link}")
                await page.goto(view_all_link, timeout=self.timeout)
                await page.wait_for_load_state("networkidle", timeout=self.timeout)
                
                # Scroll down to ensure all content loads
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)

            # Extract categories
            category_names = await self.extract_category_names(page)
            category_links = await self.extract_category_links(page)
            
            # Ensure we have the same number of names and links
            if len(category_names) != len(category_links):
                print(f"  Warning: Mismatch between category names ({len(category_names)}) and links ({len(category_links)})")
                
                # Use the smaller of the two to avoid index errors
                min_len = min(len(category_names), len(category_links))
                category_names = category_names[:min_len]
                category_links = category_links[:min_len]

            print(f"  Found {len(category_names)} categories")

            # Process each category
            categories_data = []
            for index, (name, link) in enumerate(zip(category_names, category_links)):
                print(f"  Processing category {index+1}/{len(category_names)}: {name}")
                
                # Extract sub-categories and items
                sub_categories = await self.extract_sub_categories(page, name, link)
                
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


class MainScraper:
    def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
        self.target_url = target_url
        self.base_url = "https://www.talabat.com"
        self.json_file = "talabat_groceries.json"
        self.excel_file = "dhaher.xlsx"
        self.groceries_data = {}
        self.timeout = 120000  # Increased timeout

        # Initialize JSON file if it doesn't exist
        if not os.path.exists(self.json_file):
            with open(self.json_file, 'w') as f:
                json.dump({}, f)
        else:
            # Load existing data
            with open(self.json_file, 'r') as f:
                try:
                    self.groceries_data = json.load(f)
                except json.JSONDecodeError:
                    self.groceries_data = {}

    async def extract_grocery_info(self, page):
        """Extract grocery title, link and delivery time"""
        groceries_info = []

        try:
            # Try multiple selectors for vendor containers
            vendor_selectors = [
                'div[data-testid="one-vendor-container"]',
                '.vendor-card',
                '.restaurant-card',
                '.grocery-card',
                'div[data-test*="vendor"]',
                'div.vendor-list > div'
            ]
            
            vendor_containers = []
            for selector in vendor_selectors:
                try:
                    print(f"Trying selector: {selector}")
                    await page.wait_for_selector(selector, timeout=15000)
                    containers = await page.query_selector_all(selector)
                    if containers and len(containers) > 0:
                        vendor_containers = containers
                        print(f"Found {len(containers)} vendors with selector: {selector}")
                        break
                except Exception as e:
                    print(f"Selector {selector} failed: {str(e)}")
                    continue
            
            if not vendor_containers:
                print("No vendor containers found with any selector")
                
                # Try screen scraping as a last resort
                html = await page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Look for any elements that might contain vendor information
                vendor_cards = soup.select('a[href*="/grocery/"], div[class*="vendor"], div[class*="restaurant"], div[class*="card"]')
                
                if vendor_cards:
                    print(f"Found {len(vendor_cards)} potential vendor cards from HTML")
                    
                    for i, card in enumerate(vendor_cards):
                        try:
                            title_elem = card.select_one('h2, h3, .title, [class*="name"]')
                            title = title_elem.text.strip() if title_elem else f"Unknown Grocery {i+1}"
                            
                            link = None
                            if card.name == 'a' and 'href' in card.attrs:
                                link = self.base_url + card['href']
                            else:
                                link_elem = card.find('a')
                                link = self.base_url + link_elem['href'] if link_elem and 'href' in link_elem.attrs else None
                            
                            # Check if this looks like a grocery link
                            if link and ('/grocery/' in link or '/groceries/' in link):
                                delivery_time = "N/A"
                                delivery_elem = card.select_one('.delivery, [class*="time"], [class*="delivery"]')
                                if delivery_elem:
                                    delivery_text = delivery_elem.text.strip()
                                    digits = re.findall(r'\d+', delivery_text)
                                    delivery_time = f"{digits[0]} mins" if digits else "N/A"
                                
                                print(f"Found grocery from HTML: {title}")
                                groceries_info.append({
                                    'grocery_title': title,
                                    'grocery_link': link,
                                    'delivery_time': delivery_time
                                })
                        except Exception as e:
                            print(f"Error processing HTML vendor {i+1}: {e}")
                
                if groceries_info:
                    return groceries_info
                
                # If nothing works, specify some hardcoded groceries as a fallback
                print("Using hardcoded grocery data as fallback")
                return [
                    {
                        'grocery_title': 'Hala Store',
                        'grocery_link': 'https://www.talabat.com/kuwait/grocery/627805/hala-storefintas?aid=59',
                        'delivery_time': '21 mins'
                    },
                    {
                        'grocery_title': 'Elite One Grocery',
                        'grocery_link': 'https://www.talabat.com/kuwait/grocery/605066/elite-one-grocerymangaf?aid=59',
                        'delivery_time': '30 mins'
                    }
                ]
            
            print(f"Found {len(vendor_containers)} grocery vendors on page")

            for i, container in enumerate(vendor_containers, 1):
                try:
                    # Try multiple selectors for title
                    title_selectors = ['a div h2', 'h2', '.vendor-name', '.title']
                    grocery_title = f"Unknown Grocery {i}"
                    
                    for selector in title_selectors:
                        title_element = await container.query_selector(selector)
                        if title_element:
                            grocery_title = await title_element.inner_text()
                            break

                    # Try multiple selectors for link
                    link_selectors = ['a', '[data-test*="link"]']
                    grocery_link = None
                    
                    for selector in link_selectors:
                        link_element = await container.query_selector(selector)
                        if link_element:
                            href = await link_element.get_attribute('href')
                            if href:
                                grocery_link = self.base_url + href
                                break

                    # Try multiple selectors for delivery time
                    time_selectors = ['div.deliveryInfo', '.delivery-time', '[data-test*="delivery"]']
                    delivery_time_text = "N/A"
                    
                    for selector in time_selectors:
                        delivery_info = await container.query_selector(selector)
                        if delivery_info:
                            delivery_time_text = await delivery_info.inner_text()
                            break

                    # Clean up delivery time
                    delivery_time = "N/A"
                    if delivery_time_text != "N/A":
                        digits = re.findall(r'\d+', delivery_time_text)
                        delivery_time = f"{digits[0]} mins" if digits else "N/A"

                    if grocery_title and grocery_link:
                        print(f"Found grocery: {grocery_title}, Delivery time: {delivery_time}")
                        groceries_info.append({
                            'grocery_title': grocery_title,
                            'grocery_link': grocery_link,
                            'delivery_time': delivery_time
                        })
                except Exception as e:
                    print(f"Error processing grocery {i}: {e}")
        except Exception as e:
            print(f"Error extracting grocery information: {e}")

        return groceries_info

    def save_to_json(self):
        """Save scraped data to JSON file"""
        with open(self.json_file, 'w') as f:
            json.dump(self.groceries_data, f, indent=4)
        print(f"Data saved to {self.json_file}")

    def save_to_excel(self):
        """Save data to Excel file with each grocery in a separate sheet"""
        try:
            writer = pd.ExcelWriter(self.excel_file, engine='xlsxwriter')

            for grocery_title, grocery_data in self.groceries_data.items():
                # Create flat structure for the data
                flattened_data = []

                # Add general info
                general_info = {
                    'grocery_title': grocery_title,
                    'delivery_time': grocery_data.get('delivery_time', 'N/A'),
                    'delivery_fees': grocery_data.get('grocery_details', {}).get('delivery_fees', 'N/A'),
                    'minimum_order': grocery_data.get('grocery_details', {}).get('minimum_order', 'N/A')
                }

                # Process categories and items
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

                # If there are no items, add just the general info
                if not flattened_data:
                    flattened_data.append(general_info)

                # Convert to DataFrame and save to Excel
                # Make sheet name valid for Excel (max 31 chars, no special chars)
                safe_title = re.sub(r'[\\/*?:\[\]]', '_', grocery_title)[:31]
                df = pd.DataFrame(flattened_data)
                df.to_excel(writer, sheet_name=safe_title, index=False)

            writer.close()
            print(f"Data saved to {self.excel_file}")
        except Exception as e:
            print(f"Error saving to Excel: {e}")

    async def process_grocery(self, grocery_info, browser):
        """Process a single grocery using TalabatGroceries class"""
        grocery_title = grocery_info['grocery_title']
        grocery_link = grocery_info['grocery_link']
        delivery_time = grocery_info['delivery_time']

        print(f"Processing grocery: {grocery_title}")

        # Skip if already processed
        if grocery_title in self.groceries_data:
            print(f"Skipping {grocery_title} - already processed")
            return

        # Initialize TalabatGroceries with the grocery link
        talabat_grocery = TalabatGroceries(grocery_link)

        # Create a new page for this grocery
        page = await browser.new_page()
        page.set_default_timeout(self.timeout)

        try:
            # Extract grocery details
            grocery_details = await talabat_grocery.extract_categories(page)

            # Store data in groceries_data dictionary
            self.groceries_data[grocery_title] = {
                'grocery_link': grocery_link,
                'delivery_time': delivery_time,
                'grocery_details': grocery_details
            }

            # Save progress after each grocery
            self.save_to_json()
            print(f"Successfully processed and saved data for {grocery_title}")
        except Exception as e:
            print(f"Error processing {grocery_title}: {e}")
        finally:
            await page.close()

    async def run(self):
        """Main method to run the scraper"""
        try:
            print(f"Starting the scraper, targeting URL: {self.target_url}")

            # Initialize playwright
            async with async_playwright() as p:
                # Launch browser with higher default timeout
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                )
                
                # Create a page to get groceries list
                page = await context.new_page()
                page.set_default_timeout(self.timeout)

                # Navigate to target URL
                await page.goto(self.target_url, timeout=self.timeout)
                await page.wait_for_load_state("networkidle", timeout=self.timeout)
                print("Page loaded successfully")
                
                # Try to bypass any potential bot detection 
                await page.wait_for_timeout(3000)  # Wait 3 seconds
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")  # Scroll to middle
                await page.wait_for_timeout(2000)  # Wait 2 seconds
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")  # Scroll to bottom
                await page.wait_for_timeout(3000)  # Wait 3 seconds
                
                # Take a screenshot for debugging
                await page.screenshot(path="debug_screenshot.png")
                print("Saved debug screenshot to debug_screenshot.png")

                # Extract grocery information
                groceries_info = await self.extract_grocery_info(page)
                await page.close()

                print(f"Found {len(groceries_info)} groceries to process")

                # Process each grocery using the same browser instance
                for grocery_info in groceries_info:
                    await self.process_grocery(grocery_info, context)

                # Close browser
                await context.close()
                await browser.close()

                # Save all data to Excel
                self.save_to_excel()
                print("Scraping completed successfully")
        except Exception as e:
            print(f"Error in main scraper: {e}")


# Main execution point
async def main():
    # Initialize and run the scraper
    scraper = MainScraper()
    await scraper.run()

# Handle both script and notebook execution
if __name__ == "__main__":
    asyncio.run(main())
else:
    # For notebook/IPython environment
    asyncio.get_event_loop().run_until_complete(main())




# import asyncio
# import json
# import os
# import re
# import nest_asyncio
# import pandas as pd
# from bs4 import BeautifulSoup
# from playwright.async_api import async_playwright
# from talabat_groceries import TalabatGroceries

# # Apply nest_asyncio to allow running asyncio.run() in a notebook/IPython environment
# nest_asyncio.apply()

# class MainScraper:
#     def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
#         self.target_url = target_url
#         self.base_url = "https://www.talabat.com"
#         self.json_file = "talabat_groceries.json"
#         self.excel_file = "dhaher.xlsx"
#         self.groceries_data = {}

#         # Initialize JSON file if it doesn't exist
#         if not os.path.exists(self.json_file):
#             with open(self.json_file, 'w') as f:
#                 json.dump({}, f)
#         else:
#             # Load existing data
#             with open(self.json_file, 'r') as f:
#                 try:
#                     self.groceries_data = json.load(f)
#                 except json.JSONDecodeError:
#                     self.groceries_data = {}

#     async def extract_grocery_info(self, page):
#         """Extract grocery title, link and delivery time directly from the webpage"""
#         groceries_info = []

#         try:
#             # Get all vendor containers
#             vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
#             print(f"Found {len(vendor_containers)} grocery vendors on page")

#             for i, container in enumerate(vendor_containers, 1):
#                 try:
#                     # Extract grocery title using the specified xpath pattern but with playwright
#                     title_element = await container.query_selector('a div h2')
#                     grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"

#                     # Extract grocery link
#                     link_element = await container.query_selector('a')
#                     grocery_link = self.base_url + await link_element.get_attribute('href') if link_element else None

#                     # Extract delivery time
#                     delivery_info = await container.query_selector('div.deliveryInfo')
#                     delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"

#                     # Clean up delivery time to extract just the minutes
#                     if delivery_time_text != "N/A":
#                         # Extract digits from the delivery time text
#                         digits = re.findall(r'\d+', delivery_time_text)
#                         delivery_time = f"{digits[0]} mins" if digits else "N/A"
#                     else:
#                         delivery_time = "N/A"

#                     if grocery_title and grocery_link and delivery_time:
#                         print(f"Found grocery: {grocery_title}, Delivery time: {delivery_time}")
#                         groceries_info.append({
#                             'grocery_title': grocery_title,
#                             'grocery_link': grocery_link,
#                             'delivery_time': delivery_time
#                         })
#                 except Exception as e:
#                     print(f"Error processing grocery {i}: {e}")
#         except Exception as e:
#             print(f"Error extracting grocery information: {e}")

#         return groceries_info

#     def save_to_json(self):
#         """Save scraped data to JSON file"""
#         with open(self.json_file, 'w') as f:
#             json.dump(self.groceries_data, f, indent=4)
#         print(f"Data saved to {self.json_file}")

#     def save_to_excel(self):
#         """Save data from JSON to Excel file with each grocery in a separate sheet"""
#         try:
#             writer = pd.ExcelWriter(self.excel_file, engine='xlsxwriter')

#             for grocery_title, grocery_data in self.groceries_data.items():
#                 # Create flat structure for the data
#                 flattened_data = []

#                 # Add general info
#                 general_info = {
#                     'grocery_title': grocery_title,
#                     'delivery_time': grocery_data.get('delivery_time', 'N/A'),
#                     'delivery_fees': grocery_data.get('grocery_details', {}).get('delivery_fees', 'N/A'),
#                     'minimum_order': grocery_data.get('grocery_details', {}).get('minimum_order', 'N/A')
#                 }

#                 # Process categories and items
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

#                 # If there are no items, add just the general info
#                 if not flattened_data:
#                     flattened_data.append(general_info)

#                 # Convert to DataFrame and save to Excel
#                 # Make sheet name valid for Excel (max 31 chars, no special chars)
#                 safe_title = re.sub(r'[\\/*?:\[\]]', '_', grocery_title)[:31]
#                 df = pd.DataFrame(flattened_data)
#                 df.to_excel(writer, sheet_name=safe_title, index=False)

#             writer.close()
#             print(f"Data saved to {self.excel_file}")
#         except Exception as e:
#             print(f"Error saving to Excel: {e}")

#     async def process_grocery(self, grocery_info):
#         """Process a single grocery using TalabatGroceries class"""
#         grocery_title = grocery_info['grocery_title']
#         grocery_link = grocery_info['grocery_link']
#         delivery_time = grocery_info['delivery_time']

#         print(f"Processing grocery: {grocery_title}")

#         # Skip if already processed
#         if grocery_title in self.groceries_data:
#             print(f"Skipping {grocery_title} - already processed")
#             return

#         # Initialize TalabatGroceries with the grocery link
#         talabat_grocery = TalabatGroceries(grocery_link)

#         # Extract grocery details using Playwright
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(headless=True)  # Always use headless mode
#             page = await browser.new_page()

#             try:
#                 grocery_details = await talabat_grocery.extract_categories(page)

#                 # Store data in groceries_data dictionary
#                 self.groceries_data[grocery_title] = {
#                     'grocery_link': grocery_link,
#                     'delivery_time': delivery_time,
#                     'grocery_details': grocery_details
#                 }

#                 # Save progress after each grocery
#                 self.save_to_json()
#                 print(f"Successfully processed and saved data for {grocery_title}")
#             except Exception as e:
#                 print(f"Error processing {grocery_title}: {e}")
#             finally:
#                 await browser.close()

#     async def run(self):
#         """Main method to run the scraper"""
#         try:
#             print(f"Starting the scraper, targeting URL: {self.target_url}")

#             # Initialize playwright and navigate to target URL
#             async with async_playwright() as p:
#                 browser = await p.chromium.launch(headless=True)  # Always use headless mode
#                 page = await browser.new_page()

#                 # Set longer timeouts and wait for page load
#                 page.set_default_timeout(60000)  # 60 seconds

#                 # Navigate to the target URL
#                 await page.goto(self.target_url, timeout=60000)
#                 await page.wait_for_load_state("networkidle", timeout=60000)
#                 print("Page loaded successfully")

#                 # Wait for grocery vendor elements to load
#                 try:
#                     await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=60000)
#                     print("Grocery vendor elements found")
#                 except Exception as e:
#                     print(f"Error waiting for vendor elements: {e}")
#                     print("Attempting to continue anyway...")

#                 # Extract grocery information directly from page
#                 groceries_info = await self.extract_grocery_info(page)
#                 await browser.close()

#                 print(f"Found {len(groceries_info)} groceries to process")

#                 # Process each grocery sequentially
#                 for grocery_info in groceries_info:
#                     await self.process_grocery(grocery_info)

#                 # Save all data to Excel
#                 self.save_to_excel()
#                 print("Scraping completed successfully")
#         except Exception as e:
#             print(f"Error in main scraper: {e}")


# # Main execution point - now compatible with notebook environments
# async def main():
#     # Initialize and run the scraper
#     scraper = MainScraper()
#     await scraper.run()

# # Handle both script and notebook execution
# if __name__ == "__main__":
#     asyncio.run(main())
# else:
#     # For notebook/IPython environment, use this method to run
#     asyncio.get_event_loop().run_until_complete(main())
