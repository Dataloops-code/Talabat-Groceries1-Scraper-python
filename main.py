import asyncio
import json
import os
import re
import nest_asyncio
from bs4 import BeautifulSoup
import pandas as pd
from playwright.async_api import async_playwright

nest_asyncio.apply()

class TalabatGroceries:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.talabat.com"
        print(f"Initialized TalabatGroceries with URL: {self.url}")

    async def get_general_link(self, page):
        print("Attempting to get general link")
        retries = 3
        while retries > 0:
            try:
                link_element = await page.wait_for_selector('//a[@data-testid="view-all-link"]', timeout=60000)
                if link_element:
                    full_link = self.base_url + await link_element.get_attribute('href')
                    print(f"General link found: {full_link}")
                    return full_link
                else:
                    print("General link not found")
                    return None
            except Exception as e:
                print(f"Error getting general link: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return None

    async def get_delivery_fees(self, page):
        print("Attempting to get delivery fees")
        retries = 3
        while retries > 0:
            try:
                delivery_fees_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[1]')
                delivery_fees = await delivery_fees_element.inner_text() if delivery_fees_element else "N/A"
                print(f"Delivery fees: {delivery_fees}")
                return delivery_fees
            except Exception as e:
                print(f"Error getting delivery fees: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return "N/A"

    async def get_minimum_order(self, page):
        print("Attempting to get minimum order")
        retries = 3
        while retries > 0:
            try:
                minimum_order_element = await page.query_selector('xpath=/html/body/div/div/div[1]/div/div[1]/div/div/div/div[2]/div[2]/div[1]/div/div[2]/span[3]')
                minimum_order = await minimum_order_element.inner_text() if minimum_order_element else "N/A"
                print(f"Minimum order: {minimum_order}")
                return minimum_order
            except Exception as e:
                print(f"Error getting minimum order: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return "N/A"

    async def extract_category_names(self, page):
        print("Attempting to extract category names")
        retries = 3
        while retries > 0:
            try:
                category_name_elements = await page.query_selector_all('//span[@data-testid="category-name"]')
                category_names = [await element.inner_text() for element in category_name_elements]
                print(f"Category names extracted: {category_names}")
                return category_names
            except Exception as e:
                print(f"Error extracting category names: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_category_links(self, page):
        print("Attempting to extract category links")
        retries = 3
        while retries > 0:
            try:
                category_link_elements = await page.query_selector_all('//a[@data-testid="category-item-container"]')
                category_links = [self.base_url + await element.get_attribute('href') for element in category_link_elements]
                print(f"Category links extracted: {category_links}")
                return category_links
            except Exception as e:
                print(f"Error extracting category links: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_sub_categories(self, page, category_xpath):
        print(f"Attempting to extract sub-categories using XPath: {category_xpath}")
        retries = 3
        while retries > 0:
            try:
                sub_category_elements = await page.query_selector_all(f'{category_xpath}//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
                sub_categories = []
                for element in sub_category_elements:
                    try:
                        sub_category_name = await element.inner_text()
                        sub_category_link = self.base_url + await element.get_attribute('href')
                        print(f"    Processing sub-category: {sub_category_name}")
                        print(f"    Sub-category link: {sub_category_link}")

                        # Open a new tab for each sub-category link to ensure it loads successfully
                        items = await self.extract_all_items_from_sub_category(sub_category_link)

                        sub_categories.append({
                            "sub_category_name": sub_category_name,
                            "sub_category_link": sub_category_link,
                            "Items": items
                        })
                    except Exception as e:
                        print(f"Error processing sub-category: {e}")
                return sub_categories
            except Exception as e:
                print(f"Error extracting sub-categories: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return []

    async def extract_item_details(self, item_link):
        print(f"Attempting to extract item details for link: {item_link}")
        for browser_type in ["chromium", "firefox", "webkit"]:
            try:
                return await self.extract_item_details_new_tab(item_link, browser_type)
            except Exception as e:
                print(f"Error extracting item details for {item_link} using {browser_type}: {e}")
                continue  # Try the next browser type
        return {
            "item_price": "N/A",
            "item_description": "N/A",
            "item_delivery_time_range": "N/A",
            "item_images": []
        }

    async def extract_all_items_from_sub_category(self, sub_category_link):
        print(f"Attempting to extract all items from sub-category: {sub_category_link}")
        for browser_type in ["chromium", "firefox", "webkit"]:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    sub_page = await browser.new_page()
                    await sub_page.goto(sub_category_link, timeout=240000)
                    await sub_page.wait_for_load_state("networkidle", timeout=240000)
                    await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)

                    pagination_element = await sub_page.query_selector('//div[@class="sc-104fa483-0 fCcIDQ"]//ul[@class="paginate-wrap"]')
                    total_pages = 1

                    if pagination_element:
                        page_numbers = await pagination_element.query_selector_all('//li[contains(@class, "paginate-li f-16 f-500")]//a')
                        total_pages = len(page_numbers) if page_numbers else 1
                    print(f"      Found {total_pages} pages in this sub-category")

                    items = []
                    for page_number in range(1, total_pages + 1):
                        print(f"      Processing page {page_number} of {total_pages}")
                        page_url = f"{sub_category_link}&page={page_number}"
                        await sub_page.goto(page_url, timeout=240000)
                        await sub_page.wait_for_load_state("networkidle", timeout=240000)

                        await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)

                        item_elements = await sub_page.query_selector_all('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]//a[@data-testid="grocery-item-link-nofollow"]')
                        print(f"        Found {len(item_elements)} items on page {page_number}")

                        for i, element in enumerate(item_elements):
                            try:
                                item_name_element = await element.query_selector('div[data-test="item-name"]')
                                item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {i+1}"
                                print(f"        Item name: {item_name}")

                                item_link = self.base_url + await element.get_attribute('href')
                                print(f"        Item link: {item_link}")

                                item_details = await self.extract_item_details(item_link)

                                items.append({
                                    "item_name": item_name,
                                    "item_link": item_link,
                                    **item_details
                                })
                            except Exception as e:
                                print(f"        Error processing item {i+1}: {e}")

                    await browser.close()
                    return items
            except Exception as e:
                print(f"Error extracting items from sub-category {sub_category_link} using {browser_type}: {e}")
                continue  # Try the next browser type
        return []

    async def extract_item_details_new_tab(self, item_link, browser_type):
        print(f"Attempting to extract item details in a new tab for link: {item_link} using {browser_type}")
        retries = 3
        while retries > 0:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    page = await browser.new_page()
                    await page.goto(item_link, timeout=240000)

                    await page.wait_for_load_state("networkidle", timeout=240000)

                    item_price_element = await page.query_selector('//div[@class="price"]//span[@class="currency "]')
                    item_price = await item_price_element.inner_text() if item_price_element else "N/A"
                    print(f"Item price: {item_price}")

                    item_description_element = await page.query_selector('//div[@class="description"]//p[@data-testid="item-description"]')
                    item_description = await item_description_element.inner_text() if item_description_element else "N/A"
                    print(f"Item description: {item_description}")

                    delivery_time_element = await page.query_selector('//div[@data-testid="delivery-tag"]//span')
                    delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"
                    print(f"Delivery time range: {delivery_time}")

                    item_image_elements = await page.query_selector_all('//div[@data-testid="item-image"]//img')
                    item_images = [await img.get_attribute('src') for img in item_image_elements]
                    print(f"Item images: {item_images}")

                    await browser.close()

                    return {
                        "item_price": item_price,
                        "item_description": item_description,
                        "item_delivery_time_range": delivery_time,
                        "item_images": item_images
                    }
            except Exception as e:
                print(f"Error extracting item details for {item_link} in new tab using {browser_type}: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return {
            "item_price": "N/A",
            "item_description": "N/A",
            "item_delivery_time_range": "N/A",
            "item_images": []
        }

    async def extract_categories(self, page):
        print(f"Processing grocery: {self.url}")
        retries = 3
        while retries > 0:
            try:
                await page.goto(self.url, timeout=240000)
                await page.wait_for_load_state("networkidle", timeout=240000)
                print("Page loaded successfully")

                delivery_fees = await self.get_delivery_fees(page)
                minimum_order = await self.get_minimum_order(page)
                view_all_link = await self.get_general_link(page)

                print(f"  Delivery fees: {delivery_fees}")
                print(f"  Minimum order: {minimum_order}")

                if view_all_link:
                    print(f"  Navigating to view all link: {view_all_link}")
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True)
                        category_page = await browser.new_page()
                        await category_page.goto(view_all_link, timeout=240000)
                        await category_page.wait_for_load_state("networkidle", timeout=240000)

                        category_names = await self.extract_category_names(category_page)
                        category_links = await self.extract_category_links(category_page)

                        print(f"  Found {len(category_names)} categories")

                        categories_data = []
                        for index, (name, link) in enumerate(zip(category_names, category_links)):
                            print(f"  Processing category {index+1}/{len(category_names)}: {name}")
                            print(f"  Category link: {link}")
                            category_xpath = f'//div[@data-testid="category-item-component"][{index + 1}]'
                            async with async_playwright() as p:
                                browser = await p.chromium.launch(headless=True)
                                sub_category_page = await browser.new_page()
                                await sub_category_page.goto(link, timeout=240000)
                                await sub_category_page.wait_for_load_state("networkidle", timeout=240000)

                                sub_categories = await self.extract_sub_categories(sub_category_page, category_xpath)
                                await browser.close()

                            print(f"  Found {len(sub_categories)} sub-categories in {name}")
                            category_data = {
                                "name": name,
                                "link": link,
                                "sub_categories": sub_categories
                            }
                            categories_data.append(category_data)

                        await browser.close()

                grocery_data = {
                    "delivery_fees": delivery_fees,
                    "minimum_order": minimum_order,
                    "categories": categories_data
                }

                return grocery_data
            except Exception as e:
                print(f"Error extracting categories: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
        return {"error": "Failed to extract categories after multiple attempts"}

class MainScraper:
    def __init__(self, target_url="https://www.talabat.com/kuwait/groceries/59/dhaher"):
        self.target_url = target_url
        self.base_url = "https://www.talabat.com"
        self.json_file = "talabat_groceries.json"
        self.excel_file = "dhaher.xlsx"
        self.groceries_data = {}
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
        """Save scraped data to JSON file"""
        print("Saving data to JSON file")
        try:
            with open(self.json_file, 'w') as f:
                json.dump(self.groceries_data, f, indent=4)
            print(f"Data saved to {self.json_file}")
        except Exception as e:
            print(f"Error saving to JSON file: {e}")

    def save_to_excel(self):
        """Save data from JSON to Excel file with each grocery in a separate sheet"""
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
        for browser_type in ["chromium", "firefox", "webkit"]:
            try:
                async with async_playwright() as p:
                    browser = await p[browser_type].launch(headless=True)
                    page = await browser.new_page()

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
                    break  # Exit the loop if successful
            except Exception as e:
                print(f"Error processing {grocery_title} with {browser_type}: {e}")
                continue  # Try the next browser type
            finally:
                await browser.close()

    async def run(self):
        """Main method to run the scraper"""
        print("Starting the scraper")
        retries = 3
        while retries > 0:
            try:
                print(f"Target URL: {self.target_url}")

                # Initialize playwright and navigate to target URL
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)  # Always use headless mode
                    page = await browser.new_page()

                    # Set longer timeouts and wait for page load
                    page.set_default_timeout(240000)  # 240 seconds

                    # Navigate to the target URL
                    await page.goto(self.target_url, timeout=240000)
                    await page.wait_for_load_state("networkidle", timeout=240000)
                    print("Page loaded successfully")

                    # Wait for grocery vendor elements to load
                    try:
                        await page.wait_for_selector('div[data-testid="one-vendor-container"]', timeout=240000)
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
                break  # Exit retry loop if successful
            except Exception as e:
                print(f"Error in main scraper: {e}")
                retries -= 1
                print(f"Retries left: {retries}")
                await asyncio.sleep(5)
                if retries == 0:
                    print("Failed to complete the scraping process after multiple attempts.")


# Main execution point - now compatible with notebook environments
async def main():
    # Initialize and run the scraper
    scraper = MainScraper()
    await scraper.run()

# Handle both script and notebook execution
if __name__ == "__main__":
    asyncio.run(main())
else:
    # For notebook/IPython environment, use this method to run
    asyncio.get_event_loop().run_until_complete(main())





# import asyncio
# import json
# import os
# import re
# import nest_asyncio
# from bs4 import BeautifulSoup
# import pandas as pd
# from playwright.async_api import async_playwright

# nest_asyncio.apply()

# class TalabatGroceries:
#     def __init__(self, url):
#         self.url = url
#         self.base_url = "https://www.talabat.com"
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

#     async def extract_sub_categories(self, page, category_xpath):
#         print(f"Attempting to extract sub-categories using XPath: {category_xpath}")
#         retries = 3
#         while retries > 0:
#             try:
#                 sub_category_elements = await page.query_selector_all(f'{category_xpath}//div[@data-test="sub-category-container"]//a[@data-testid="subCategory-a"]')
#                 sub_categories = []
#                 for element in sub_category_elements:
#                     try:
#                         sub_category_name = await element.inner_text()
#                         sub_category_link = self.base_url + await element.get_attribute('href')
#                         print(f"    Processing sub-category: {sub_category_name}")
#                         print(f"    Sub-category link: {sub_category_link}")

#                         # Open a new tab for each sub-category link to ensure it loads successfully
#                         for _ in range(3):
#                             try:
#                                 async with async_playwright() as p:
#                                     browser = await p.chromium.launch(headless=True)
#                                     sub_page = await browser.new_page()
#                                     await sub_page.goto(sub_category_link, timeout=240000)
#                                     await sub_page.wait_for_load_state("networkidle", timeout=240000)
#                                     await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)
#                                     items = await self.extract_all_items_from_sub_category(sub_page, sub_category_link)
#                                     await browser.close()
#                                 break
#                             except Exception as e:
#                                 print(f"Error processing sub-category with Chrome: {e}")
#                                 async with async_playwright() as p:
#                                     browser = await p.firefox.launch(headless=True)
#                                     sub_page = await browser.new_page()
#                                     await sub_page.goto(sub_category_link, timeout=240000)
#                                     await sub_page.wait_for_load_state("networkidle", timeout=240000)
#                                     await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)
#                                     items = await self.extract_all_items_from_sub_category(sub_page, sub_category_link)
#                                     await browser.close()
#                                 break

#                         sub_categories.append({
#                             "sub_category_name": sub_category_name,
#                             "sub_category_link": sub_category_link,
#                             "Items": items
#                         })
#                     except Exception as e:
#                         print(f"Error processing sub-category: {e}")
#                 return sub_categories
#             except Exception as e:
#                 print(f"Error extracting sub-categories: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return []

#     async def extract_item_details(self, item_link):
#         print(f"Attempting to extract item details for link: {item_link}")
#         retries = 3
#         while retries > 0:
#             try:
#                 async with async_playwright() as p:
#                     browser = await p.chromium.launch(headless=True)
#                     page = await browser.new_page()
#                     await page.goto(item_link, timeout=240000)

#                     await page.wait_for_load_state("networkidle", timeout=240000)

#                     item_price_element = await page.query_selector('//div[@class="price"]//span[@class="currency "]')
#                     item_price = await item_price_element.inner_text() if item_price_element else "N/A"
#                     print(f"Item price: {item_price}")

#                     item_description_element = await page.query_selector('//div[@class="description"]//p[@data-testid="item-description"]')
#                     item_description = await item_description_element.inner_text() if item_description_element else "N/A"
#                     print(f"Item description: {item_description}")

#                     delivery_time_element = await page.query_selector('//div[@data-testid="delivery-tag"]//span')
#                     delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"
#                     print(f"Delivery time range: {delivery_time}")

#                     item_image_elements = await page.query_selector_all('//div[@data-testid="item-image"]//img')
#                     item_images = [await img.get_attribute('src') for img in item_image_elements]
#                     print(f"Item images: {item_images}")

#                     await browser.close()

#                     return {
#                         "item_price": item_price,
#                         "item_description": item_description,
#                         "item_delivery_time_range": delivery_time,
#                         "item_images": item_images
#                     }
#             except Exception as e:
#                 print(f"Error extracting item details for {item_link}: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return {
#             "item_price": "N/A",
#             "item_description": "N/A",
#             "item_delivery_time_range": "N/A",
#             "item_images": []
#         }

#     async def extract_all_items_from_sub_category(self, sub_page, sub_category_link):
#         print(f"Attempting to extract all items from sub-category: {sub_category_link}")
#         retries = 3
#         while retries > 0:
#             try:
#                 pagination_element = await sub_page.query_selector('//div[@class="sc-104fa483-0 fCcIDQ"]//ul[@class="paginate-wrap"]')
#                 total_pages = 1

#                 if pagination_element:
#                     page_numbers = await pagination_element.query_selector_all('//li[contains(@class, "paginate-li f-16 f-500")]//a')
#                     total_pages = len(page_numbers) if page_numbers else 1
#                 print(f"      Found {total_pages} pages in this sub-category")

#                 items = []
#                 for page_number in range(1, total_pages + 1):
#                     print(f"      Processing page {page_number} of {total_pages}")
#                     page_url = f"{sub_category_link}&page={page_number}"
#                     await sub_page.goto(page_url, timeout=240000)
#                     await sub_page.wait_for_load_state("networkidle", timeout=240000)

#                     await sub_page.wait_for_selector('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]', timeout=240000)

#                     item_elements = await sub_page.query_selector_all('//div[@class="category-items-container all-items w-100"]//div[@class="col-8 col-sm-4"]//a[@data-testid="grocery-item-link-nofollow"]')
#                     print(f"        Found {len(item_elements)} items on page {page_number}")

#                     for i, element in enumerate(item_elements):
#                         try:
#                             item_name_element = await element.query_selector('div[data-test="item-name"]')
#                             item_name = await item_name_element.inner_text() if item_name_element else f"Unknown Item {i+1}"
#                             print(f"        Item name: {item_name}")

#                             item_link = self.base_url + await element.get_attribute('href')
#                             print(f"        Item link: {item_link}")

#                             item_details = await self.extract_item_details(item_link)

#                             if item_details["item_price"] == "N/A" and item_details["item_description"] == "N/A" and item_details["item_delivery_time_range"] == "N/A":
#                                 print(f"Retrying item details for link: {item_link}")
#                                 item_details = await self.extract_item_details_new_tab(item_link, browser_type="firefox")

#                             items.append({
#                                 "item_name": item_name,
#                                 "item_link": item_link,
#                                 **item_details
#                             })
#                         except Exception as e:
#                             print(f"        Error processing item {i+1}: {e}")

#                 return items
#             except Exception as e:
#                 print(f"Error extracting items from sub-category {sub_category_link}: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return []

#     async def extract_item_details_new_tab(self, item_link, browser_type="firefox"):
#         print(f"Attempting to extract item details in a new tab for link: {item_link} using {browser_type}")
#         retries = 3
#         while retries > 0:
#             try:
#                 async with async_playwright() as p:
#                     if browser_type == "firefox":
#                         browser = await p.firefox.launch(headless=True)  # Use Firefox browser instance
#                     else:
#                         browser = await p.chromium.launch(headless=True)  # Use Chromium browser instance

#                     page = await browser.new_page()
#                     await page.goto(item_link, timeout=240000)

#                     await page.wait_for_load_state("networkidle", timeout=240000)

#                     item_price_element = await page.query_selector('//div[@class="price"]//span[@class="currency "]')
#                     item_price = await item_price_element.inner_text() if item_price_element else "N/A"
#                     print(f"Item price: {item_price}")

#                     item_description_element = await page.query_selector('//div[@class="description"]//p[@data-testid="item-description"]')
#                     item_description = await item_description_element.inner_text() if item_description_element else "N/A"
#                     print(f"Item description: {item_description}")

#                     delivery_time_element = await page.query_selector('//div[@data-testid="delivery-tag"]//span')
#                     delivery_time = await delivery_time_element.inner_text() if delivery_time_element else "N/A"
#                     print(f"Delivery time range: {delivery_time}")

#                     item_image_elements = await page.query_selector_all('//div[@data-testid="item-image"]//img')
#                     item_images = [await img.get_attribute('src') for img in item_image_elements]
#                     print(f"Item images: {item_images}")

#                     await browser.close()

#                     return {
#                         "item_price": item_price,
#                         "item_description": item_description,
#                         "item_delivery_time_range": delivery_time,
#                         "item_images": item_images
#                     }
#             except Exception as e:
#                 print(f"Error extracting item details for {item_link} in new tab using {browser_type}: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return {
#             "item_price": "N/A",
#             "item_description": "N/A",
#             "item_delivery_time_range": "N/A",
#             "item_images": []
#         }

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

#                 if view_all_link:
#                     print(f"  Navigating to view all link: {view_all_link}")
#                     async with async_playwright() as p:
#                         browser = await p.chromium.launch(headless=True)
#                         category_page = await browser.new_page()
#                         await category_page.goto(view_all_link, timeout=240000)
#                         await category_page.wait_for_load_state("networkidle", timeout=240000)

#                         category_names = await self.extract_category_names(category_page)
#                         category_links = await self.extract_category_links(category_page)

#                         print(f"  Found {len(category_names)} categories")

#                         categories_data = []
#                         for index, (name, link) in enumerate(zip(category_names, category_links)):
#                             print(f"  Processing category {index+1}/{len(category_names)}: {name}")
#                             print(f"  Category link: {link}")
#                             category_xpath = f'//div[@data-testid="category-item-component"][{index + 1}]'
#                             async with async_playwright() as p:
#                                 browser = await p.chromium.launch(headless=True)
#                                 sub_category_page = await browser.new_page()
#                                 await sub_category_page.goto(link, timeout=240000)
#                                 await sub_category_page.wait_for_load_state("networkidle", timeout=240000)

#                                 sub_categories = await self.extract_sub_categories(sub_category_page, category_xpath)
#                                 await browser.close()

#                             print(f"  Found {len(sub_categories)} sub-categories in {name}")
#                             category_data = {
#                                 "name": name,
#                                 "link": link,
#                                 "sub_categories": sub_categories
#                             }
#                             categories_data.append(category_data)

#                         await browser.close()

#                 grocery_data = {
#                     "delivery_fees": delivery_fees,
#                     "minimum_order": minimum_order,
#                     "categories": categories_data
#                 }

#                 return grocery_data
#             except Exception as e:
#                 print(f"Error extracting categories: {e}")
#                 retries -= 1
#                 print(f"Retries left: {retries}")
#                 await asyncio.sleep(5)
#         return {"error": "Failed to extract categories after multiple attempts"}

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




# # import asyncio
# # import json
# # import os
# # import re
# # import nest_asyncio
# # import pandas as pd
# # from bs4 import BeautifulSoup
# # from playwright.async_api import async_playwright
# # from talabat_groceries import TalabatGroceries

# # # Apply nest_asyncio to allow running asyncio.run() in a notebook/IPython environment
# # nest_asyncio.apply()


# # # Main execution point - now compatible with notebook environments
# # async def main():
# #     # Initialize and run the scraper
# #     scraper = MainScraper()
# #     await scraper.run()

# # # Handle both script and notebook execution
# # if __name__ == "__main__":
# #     asyncio.run(main())
# # else:
# #     # For notebook/IPython environment, use this method to run
# #     asyncio.get_event_loop().run_until_complete(main())
