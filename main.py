import asyncio
import json
import os
import re
import nest_asyncio
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright
from SavingOnDrive import SavingOnDrive

class MainScraper:
    def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
        self.target_url = target_url
        self.base_url = "https://www.talabat.com"
        self.json_file = "talabat_groceries.json"
        self.excel_file = "dhaher.xlsx"
        self.groceries_data = {}
        self.drive_uploader = SavingOnDrive('credentials.json')  # Initialize SavingOnDrive
        print(f"Initialized MainScraper with target URL: {self.target_url}")

        if not os.path.exists(self.json_file):
            print(f"Creating new JSON file: {self.json_file}")
            with open(self.json_file, 'w') as f:
                json.dump({}, f)
        else:
            print(f"Loading data from JSON file: {self.json_file}")
            with open(self.json_file, 'r') as f:
                try:
                    self.groceries_data = json.load(f)
                    print(f"Loaded data: {self.groceries_data}")
                except json.JSONDecodeError:
                    print("Error decoding JSON file. Starting with an empty dictionary.")
                    self.groceries_data = {}

    async def extract_grocery_info(self, page):
        print("Attempting to extract grocery information")
        retries = 3
        while retries > 0:
            try:
                vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
                print(f"Found {len(vendor_containers)} grocery vendors on page")
                groceries_info = []
                for i, container in enumerate(vendor_containers, 1):
                    try:
                        title_element = await container.query_selector('a div h2')
                        grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"
                        print(f"  Grocery title: {grocery_title}")
                        link_element = await container.query_selector('a')
                        grocery_link = self.base_url + await link_element.get_attribute('href') if link_element else None
                        print(f"  Grocery link: {grocery_link}")
                        delivery_info = await container.query_selector('div.deliveryInfo')
                        delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"
                        print(f"  Delivery time text: {delivery_time_text}")
                        if delivery_time_text != "N/A":
                            digits = re.findall(r'\d+', delivery_time_text)
                            delivery_time = f"{digits[0]} mins" if digits else "N/A"
                        else:
                            delivery_time = "N/A"
                        print(f"  Delivery time: {delivery_time}")
                        if grocery_title and grocery_link and delivery_time:
                            print(f"Found grocery: {grocery_title}, Delivery time: {delivery_time}")
                            groceries_info.append({
                                'grocery_title': grocery_title,
                                'grocery_link': grocery_link,
                                'delivery_time': delivery_time
                            })
                    except Exception as e:
                        print(f"Error processing grocery {i}: {e}")
                return groceries_info
            except Exception as e:
                print(f"Error extracting grocery information: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    def save_to_json(self):
        print("Saving data to JSON file")
        try:
            with open(self.json_file, 'w') as f:
                json.dump(self.groceries_data, f, indent=4)
            print(f"Data saved to {self.json_file}")
        except Exception as e:
            print(f"Error saving to JSON file: {e}")
               
    def save_to_excel(self):
        print("Saving data to Excel file")
        try:
            writer = pd.ExcelWriter(self.excel_file, engine='xlsxwriter')
            for grocery_title, grocery_data in self.groceries_data.items():
                flattened_data = []
                general_info = {
                    'grocery_title': grocery_title,
                    'delivery_time': grocery_data.get('delivery_time', 'N/A'),
                    'delivery_fees': grocery_data.get('grocery_details', {}).get('delivery_fees', 'N/A'),
                    'minimum_order': grocery_data.get('grocery_details', {}).get('minimum_order', 'N/A')
                }
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
                if not flattened_data:
                    flattened_data.append(general_info)
                safe_title = re.sub(r'[\\/*?:\[\]]', '_', grocery_title)[:31]
                df = pd.DataFrame(flattened_data)
                df.to_excel(writer, sheet_name=safe_title, index=False)
            writer.close()
            print(f"Data saved to {self.excel_file}")
            # Upload to Google Drive after saving
            self.upload_to_drive()
        except Exception as e:
            print(f"Error saving to Excel: {e}")

    def upload_to_drive(self):
        """Upload the Excel file to Google Drive"""
        print(f"\nUploading {self.excel_file} to Google Drive...")
        try:
            if not self.drive_uploader.authenticate():
                print("Failed to authenticate with Google Drive")
                return False
            file_ids = self.drive_uploader.upload_to_multiple_folders(self.excel_file)
            if len(file_ids) == len(self.drive_uploader.target_folders):
                print(f"Uploaded {self.excel_file} to Google Drive successfully")
                return True
            else:
                print(f"Failed to upload {self.excel_file} to all target folders")
                return False
        except Exception as e:
            print(f"Error uploading to Google Drive: {str(e)}")
            return False

    async def process_grocery(self, grocery_info):
        grocery_title = grocery_info['grocery_title']
        grocery_link = grocery_info['grocery_link']
        delivery_time = grocery_info['delivery_time']
        print(f"Processing grocery: {grocery_title}")
        if grocery_title in self.groceries_data:
            print(f"Skipping {grocery_title} - already processed")
            return
        talabat_grocery = TalabatGroceries(grocery_link)
        for browser_type in ["chromium", "firefox"]:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    page = await browser.new_page()
                    grocery_details = await talabat_grocery.extract_categories(page)
                    self.groceries_data[grocery_title] = {
                        'grocery_link': grocery_link,
                        'delivery_time': delivery_time,
                        'grocery_details': grocery_details
                    }
                    self.save_to_json()
                    print(f"Successfully processed and saved data for {grocery_title}")
                    break
            except Exception as e:
                print(f"Error processing {grocery_title} with {browser_type}: {e}")
                continue
            finally:
                await browser.close()

    async def run(self):
        print("Starting the scraper")
        retries = 3
        while retries > 0:
            try:
                print(f"Target URL: {self.target_url}")
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()
                    page.set_default_timeout(240000)
                    await page.goto(self.target_url, timeout=240000)
                    await page.wait_for_load_state("networkidle", timeout=240000)
                    print("Page loaded successfully")
                    try:
                        await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=240000)
                        print("Grocery vendor elements found")
                    except Exception as e:
                        print(f"Error waiting for vendor elements: {e}")
                        CHIprint("Attempting to continue anyway...")
                    groceries_info = await self.extract_grocery_info(page)
                    await browser.close()
                    print(f"Found {len(groceries_info)} groceries to process")
                    for grocery_info in groceries_info:
                        await self.process_grocery(grocery_info)
                    self.save_to_excel()
                    print("Scraping completed successfully")
                break
            except Exception as e:
                print(f"Error in main scraper: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
                if retries == 0:
                    print("Failed to complete the scraping process after multiple attempts.")

async def main():
    if not os.path.exists('credentials.json'):
        credentials_json = os.environ.get('TALABAT_GCLOUD_KEY_JSON')
        if credentials_json:
            with open('credentials.json', 'w') as f:
                f.write(credentials_json)
            print("Created credentials.json from environment variable")
        else:
            print("ERROR: credentials.json not found and TALABAT_GCLOUD_KEY_JSON not set!")
            return
    scraper = MainScraper()
    await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())
else:
    asyncio.get_event_loop().run_until_complete(main())

# class MainScraper:
#     def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
#         self.target_url = target_url
#         self.base_url = "https://www.talabat.com"
#         self.json_file = "talabat_groceries.json"
#         self.excel_file = "dhaher.xlsx"
#         self.groceries_data = {}
#         print(f"Initialized MainScraper with target URL: {self.target_url}")

#         if not os.path.exists(self.json_file):
#             print(f"Creating new JSON file: {self.json_file}")
#             with open(self.json_file, 'w') as f:
#                 json.dump({}, f)
#         else:
#             print(f"Loading data from JSON file: {self.json_file}")
#             with open(self.json_file, 'r') as f:
#                 try:
#                     self.groceries_data = json.load(f)
#                     print(f"Loaded data: {self.groceries_data}")
#                 except json.JSONDecodeError:
#                     print("Error decoding JSON file. Starting with an empty dictionary.")
#                     self.groceries_data = {}

#     async def extract_grocery_info(self, page):
#         print("Attempting to extract grocery information")
#         retries = 3
#         while retries > 0:
#             try:
#                 vendor_containers = await page.query_selector_all('div[data-testid="one-vendor-container"]')
#                 print(f"Found {len(vendor_containers)} grocery vendors on page")

#                 groceries_info = []
#                 for i, container in enumerate(vendor_containers, 1):
#                     try:
#                         title_element = await container.query_selector('a div h2')
#                         grocery_title = await title_element.inner_text() if title_element else f"Unknown Grocery {i}"
#                         print(f"  Grocery title: {grocery_title}")

#                         link_element = await container.query_selector('a')
#                         grocery_link = self.base_url + await link_element.get_attribute('href') if link_element else None
#                         print(f"  Grocery link: {grocery_link}")

#                         delivery_info = await container.query_selector('div.deliveryInfo')
#                         delivery_time_text = await delivery_info.inner_text() if delivery_info else "N/A"
#                         print(f"  Delivery time text: {delivery_time_text}")

#                         if delivery_time_text != "N/A":
#                             digits = re.findall(r'\d+', delivery_time_text)
#                             delivery_time = f"{digits[0]} mins" if digits else "N/A"
#                         else:
#                             delivery_time = "N/A"
#                         print(f"  Delivery time: {delivery_time}")

#                         if grocery_title and grocery_link and delivery_time:
#                             print(f"Found grocery: {grocery_title}, Delivery time: {delivery_time}")
#                             groceries_info.append({
#                                 'grocery_title': grocery_title,
#                                 'grocery_link': grocery_link,
#                                 'delivery_time': delivery_time
#                             })
#                     except Exception as e:
#                         print(f"Error processing grocery {i}: {e}")
#                 return groceries_info
#             except Exception as e:
#                 print(f"Error extracting grocery information: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return []

#     def save_to_json(self):
#         """Save scraped data to JSON file"""
#         print("Saving data to JSON file")
#         try:
#             with open(self.json_file, 'w') as f:
#                 json.dump(self.groceries_data, f, indent=4)
#             print(f"Data saved to {self.json_file}")
#         except Exception as e:
#             print(f"Error saving to JSON file: {e}")
               
#     def save_to_excel(self):
#         """Save data from JSON to Excel file with each grocery in a separate sheet"""
#         print("Saving data to Excel file")
#         try:
#             writer = pd.ExcelWriter(self.excel_file, engine='xlsxwriter')

#             for grocery_title, grocery_data in self.groceries_data.items():
#                 flattened_data = []

#                 general_info = {
#                     'grocery_title': grocery_title,
#                     'delivery_time': grocery_data.get('delivery_time', 'N/A'),
#                     'delivery_fees': grocery_data.get('grocery_details', {}).get('delivery_fees', 'N/A'),
#                     'minimum_order': grocery_data.get('grocery_details', {}).get('minimum_order', 'N/A')
#                 }

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

#                 if not flattened_data:
#                     flattened_data.append(general_info)

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
#         for browser_type in ["chromium", "firefox"]:
#             try:
#                 async with async_playwright() as p:
#                     browser = await p[browser_type].launch(headless=True)
#                     page = await browser.new_page()

#                     grocery_details = await talabat_grocery.extract_categories(page)

#                     # Store data in groceries_data dictionary
#                     self.groceries_data[grocery_title] = {
#                         'grocery_link': grocery_link,
#                         'delivery_time': delivery_time,
#                         'grocery_details': grocery_details
#                     }

#                     # Save progress after each grocery
#                     self.save_to_json()
#                     print(f"Successfully processed and saved data for {grocery_title}")
#                     break  # Exit the loop if successful
#             except Exception as e:
#                 print(f"Error processing {grocery_title} with {browser_type}: {e}")
#                 continue  # Try the next browser type
#             finally:
#                 await browser.close()

#     async def run(self):
#         """Main method to run the scraper"""
#         print("Starting the scraper")
#         retries = 3
#         while retries > 0:
#             try:
#                 print(f"Target URL: {self.target_url}")

#                 # Initialize playwright and navigate to target URL
#                 async with async_playwright() as p:
#                     browser = await p.chromium.launch(headless=True)  # Always use headless mode
#                     page = await browser.new_page()

#                     # Set longer timeouts and wait for page load
#                     page.set_default_timeout(240000)  # 240 seconds

#                     # Navigate to the target URL
#                     await page.goto(self.target_url, timeout=240000)
#                     await page.wait_for_load_state("networkidle", timeout=240000)
#                     print("Page loaded successfully")

#                     # Wait for grocery vendor elements to load
#                     try:
#                         await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=240000)
#                         print("Grocery vendor elements found")
#                     except Exception as e:
#                         print(f"Error waiting for vendor elements: {e}")
#                         print("Attempting to continue anyway...")

#                     # Extract grocery information directly from page
#                     groceries_info = await self.extract_grocery_info(page)
#                     await browser.close()

#                     print(f"Found {len(groceries_info)} groceries to process")

#                     # Process each grocery sequentially
#                     for grocery_info in groceries_info:
#                         await self.process_grocery(grocery_info)

#                     # Save all data to Excel
#                     self.save_to_excel()
#                     print("Scraping completed successfully")
#                 break  # Exit retry loop if successful
#             except Exception as e:
#                 print(f"Error in main scraper: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#                 if retries == 0:
#                     print("Failed to complete the scraping process after multiple attempts.")


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

