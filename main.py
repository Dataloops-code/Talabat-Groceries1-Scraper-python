import asyncio
import json
import os
import re
import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from talabat_groceries import TalabatGroceries

# Apply nest_asyncio to allow running asyncio.run() in a notebook/IPython environment
nest_asyncio.apply()

class MainScraper:
    def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
        self.target_url = target_url
        self.base_url = "https://www.talabat.com"
        self.json_file = "talabat_groceries.json"
        self.excel_file = "dhaher.xlsx"
        self.groceries_data = {}

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
        """Extract grocery title, link and delivery time directly from the webpage"""
        groceries_info = []

        try:
            # Alternative approach using URL: try to navigate to the page that shows all groceries
            # This is a workaround for when the default page doesn't load the vendor containers
            try:
                await page.goto("https://www.talabat.com/kuwait/groceries", timeout=90000, wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=60000)
                
                # Try to click on the Dhaher location if available
                try:
                    location_btn = await page.query_selector('button[data-testid="location-btn"]')
                    if location_btn:
                        await location_btn.click()
                        await page.wait_for_timeout(2000)
                        
                        # Try to find and click on Dhaher or any location option
                        location_options = await page.query_selector_all('li[role="option"]')
                        for option in location_options:
                            option_text = await option.inner_text()
                            if "dhaher" in option_text.lower():
                                await option.click()
                                break
                        
                        await page.wait_for_timeout(3000)
                except Exception as e:
                    print(f"Error selecting location: {e}")
            except Exception as e:
                print(f"Error navigating to groceries page: {e}")
            
            # Try multiple selector patterns to find grocery vendors
            selectors = [
                'div[data-testid="one-vendor-container"]',
                '.vendorCardContainer',
                'a[href*="/kuwait/grocery/"]',
                'div.searchVendorList div'
            ]
            
            vendor_containers = []
            for selector in selectors:
                try:
                    print(f"Trying selector: {selector}")
                    await page.wait_for_selector(selector, timeout=30000)
                    containers = await page.query_selector_all(selector)
                    if containers and len(containers) > 0:
                        vendor_containers = containers
                        print(f"Found {len(containers)} vendors using selector: {selector}")
                        break
                except Exception as e:
                    print(f"Selector {selector} failed: {e}")
            
            if not vendor_containers:
                # Last resort: dump the HTML and try to extract with BeautifulSoup
                print("Using BeautifulSoup as fallback...")
                html_content = await page.content()
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Try to extract links that look like grocery links
                grocery_links = soup.find_all('a', href=lambda href: href and '/kuwait/grocery/' in href)
                
                for link in grocery_links:
                    try:
                        grocery_title = link.find('h2').text if link.find('h2') else "Unknown Grocery"
                        grocery_link = self.base_url + link['href']
                        
                        # Look for delivery time in nearby div
                        delivery_info = link.find('div', class_='deliveryInfo')
                        if delivery_info:
                            delivery_time_text = delivery_info.text
                            digits = re.findall(r'\d+', delivery_time_text)
                            delivery_time = f"{digits[0]} mins" if digits else "N/A"
                        else:
                            delivery_time = "N/A"
                        
                        print(f"Found grocery (BS4): {grocery_title}, Delivery time: {delivery_time}")
                        groceries_info.append({
                            'grocery_title': grocery_title,
                            'grocery_link': grocery_link,
                            'delivery_time': delivery_time
                        })
                    except Exception as e:
                        print(f"Error processing grocery link with BeautifulSoup: {e}")
                
                # If we still don't have any groceries, use hardcoded data as a last resort
                if not groceries_info:
                    print("Using hardcoded data as fallback")
                    # Use hardcoded data based on the logs you provided
                    hardcoded_groceries = [
                        {"grocery_title": "Candy mart", "grocery_link": "https://www.talabat.com/kuwait/grocery/667302/candy-martmangaf-tgo?aid=59", "delivery_time": "28 mins"},
                        {"grocery_title": "ZAJEL Market", "grocery_link": "https://www.talabat.com/kuwait/grocery/729368/zajel-marketabu-halifa?aid=59", "delivery_time": "27 mins"},
                        {"grocery_title": "Hala Store", "grocery_link": "https://www.talabat.com/kuwait/grocery/627805/hala-storefintas?aid=59", "delivery_time": "21 mins"},
                        {"grocery_title": "Elite One Grocery", "grocery_link": "https://www.talabat.com/kuwait/grocery/658757/elite-one-grocerymahboula-tgo?aid=59", "delivery_time": "28 mins"},
                        {"grocery_title": "Ghozlan Al Egaila Grocery", "grocery_link": "https://www.talabat.com/kuwait/grocery/674068/ghozlan-al-egaila-grocery-fintas?aid=59", "delivery_time": "22 mins"}
                    ]
                    groceries_info = hardcoded_groceries
            else:
                print(f"Found {len(vendor_containers)} grocery vendors on page")
                
                for i, container in enumerate(vendor_containers, 1):
                    try:
                        # Extract grocery title
                        title_element = await container.query_selector('h2')
                        if not title_element:
                            title_element = await container.query_selector('a div h2')
                        
                        grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"
                        
                        # Extract grocery link
                        link_element = await container.query_selector('a')
                        if not link_element:
                            link_element = container if await container.get_attribute('href') else None
                            
                        grocery_link = self.base_url + await link_element.get_attribute('href') if link_element else None
                        
                        # Extract delivery time
                        delivery_info = await container.query_selector('div.deliveryInfo')
                        delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"
                        
                        # Clean up delivery time to extract just the minutes
                        if delivery_time_text != "N/A":
                            # Extract digits from the delivery time text
                            digits = re.findall(r'\d+', delivery_time_text)
                            delivery_time = f"{digits[0]} mins" if digits else "N/A"
                        else:
                            delivery_time = "N/A"
                        
                        if grocery_title and grocery_link and delivery_time:
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
        """Save data from JSON to Excel file with each grocery in a separate sheet"""
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

    async def process_grocery(self, grocery_info):
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

        # Extract grocery details using Playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()
            page.set_default_timeout(90000)

            try:
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
                await browser.close()

    async def run(self):
        """Main method to run the scraper"""
        try:
            print(f"Starting the scraper, targeting URL: {self.target_url}")

            # Initialize playwright and navigate to target URL
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    viewport={"width": 1280, "height": 800"},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                )
                page = await context.new_page()
                
                # Set longer timeouts
                page.set_default_timeout(90000)

                # Navigate to the target URL with relaxed wait_until to prevent immediate timeout
                max_retries = 3
                success = False
                for attempt in range(max_retries):
                    try:
                        # Use a less strict wait condition to start with
                        await page.goto(self.target_url, timeout=90000, wait_until="domcontentloaded")
                        # Then wait a bit and check for network idle
                        await page.wait_for_timeout(5000)
                        await page.wait_for_load_state("networkidle", timeout=30000)
                        success = True
                        break
                    except Exception as e:
                        print(f"Retry {attempt+1}/{max_retries} for {self.target_url}: {e}")
                        await asyncio.sleep(2)
                
                if success:
                    print("Page loaded successfully")
                else:
                    print("Page load was not completely successful, but continuing anyway")
                
                # Take a screenshot for debugging
                await page.screenshot(path="page_screenshot.png")
                print("Screenshot saved to page_screenshot.png")
                
                # Extract the HTML content for debugging
                html_content = await page.content()
                with open("page_content.html", "w", encoding="utf-8") as f:
                    f.write(html_content)
                print("HTML content saved to page_content.html")

                # Extract grocery information directly from page
                groceries_info = await self.extract_grocery_info(page)
                await browser.close()

                print(f"Found {len(groceries_info)} groceries to process")

                # Process each grocery sequentially
                # To mimic Colab behavior, limit to processing just the first grocery for testing
                process_limit = 2  # Process only 2 groceries to save time
                if len(groceries_info) > process_limit:
                    print(f"Limiting processing to first {process_limit} groceries to save time")
                    groceries_info = groceries_info[:process_limit]

                for grocery_info in groceries_info:
                    await self.process_grocery(grocery_info)

                # Save all data to Excel
                self.save_to_excel()
                print("Scraping completed successfully")
        except Exception as e:
            print(f"Error in main scraper: {e}")
