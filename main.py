import asyncio
import json
import os
import re
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright
from talabat_groceries import TalabatGroceries

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
            # Get all vendor containers
            vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
            print(f"Found {len(vendor_containers)} grocery vendors on page")

            for i, container in enumerate(vendor_containers, 1):
                try:
                    # Extract grocery title using the specified xpath pattern but with playwright
                    title_element = await container.query_selector('a div h2')
                    grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"

                    # Extract grocery link
                    link_element = await container.query_selector('a')
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
            browser = await p.chromium.launch(headless=True)  # Always use headless mode
            page = await browser.new_page()

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
                browser = await p.chromium.launch(headless=True)  # Always use headless mode
                page = await browser.new_page()

                # Set longer timeouts and wait for page load
                page.set_default_timeout(60000)  # 60 seconds

                # Navigate to the target URL
                await page.goto(self.target_url, timeout=60000)
                await page.wait_for_load_state("networkidle", timeout=60000)
                print("Page loaded successfully")

                # Wait for grocery vendor elements to load
                try:
                    await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=60000)
                    print("Grocery vendor elements found")
                except Exception as e:
                    print(f"Error waiting for vendor elements: {e}")
                    print("Attempting to continue anyway...")

                # Extract grocery information directly from page
                groceries_info = await self.extract_grocery_info(page)
                await browser.close()

                print(f"Found {len(groceries_info)} groceries to process")

                # Process each grocery sequentially
                for grocery_info in groceries_info:
                    await self.process_grocery(grocery_info)

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


if __name__ == "__main__":
    asyncio.run(main())