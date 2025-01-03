import requests
import json
import sys  
import os
from bs4 import BeautifulSoup
import re
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import MongoClient
from bson import json_util
import traceback
import time  # To add delays for troubleshooting



uri = os.environ['DB_ACCESS_URI']
base_url = os.environ['BASE_URL']




def fetch_product_page(itemId,base_url):
    # Generate the URL based on the provided itemId
    url = f'{base_url}{itemId}'

    # Define the headers
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': '_ga=GA1.1.141094158.1735798558; XSRF-TOKEN=eyJpdiI6InRtUmU3YUc0MGtpS2xIU0ZReXQrOUE9PSIsInZhbHVlIjoid2toZGg2ZDkvQWw3YmxQOUxQYU5BZWV6ME9FZ3U2SUJFMlVKc2N3bUZoSmFMcnJSOHloY0VLSkJqOFNsQUpySDdNQjdpQ3hQZnU5cE9ZWGk2Si9KN3VsV3VnTmJiNHFoWFRDN3Vtd0FGaGFTcGREbkxDWUF6WFZIL2dJVjRnQXEiLCJtYWMiOiI4MjkzODAyZWUxNmEyMTEzYzlhYWY0YTliMGRmNmJkY2VkZWNjNzUwOTRhZWFjNzdjYmZiZTFjYjc5ZDI5MTQ5IiwidGFnIjoiIn0%3D; laravel_session=eyJpdiI6ImlpT3BmMFg0QmpnNjRHa21INUprT3c9PSIsInZhbHVlIjoiNjJsMDgrb3pJZGxrQXhyMG8vQ2dIVUxBUmxSZlJpWllPRDVoZ2MvUlludGcrQktwNEZPTklFbFhBeWFMMVkrWklEME9EZ3FkY3F0RFZwMm5zUmdnZUE2bUt0NlcyalhRWlF6WmVXUjJ6ZXduVEU5dVlKWVpBd2FLOFdoRTYySlQiLCJtYWMiOiJkNmM2NmFkMDdkOWM0NjFhNmY0NWU1ODA0MjI4YWE4MGFkZjg1NjQwZGIzZjBjZWZjYjg0ZWNiYzg3MTJmZTYxIiwidGFnIjoiIn0%3D; _ga_Z5QXH7PSMN=GS1.1.1735825800.6.1.1735825803.0.0.0',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': f'?{itemId}',
        'Upgrade-Insecure-Requests': f'{itemId}',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    # Send the GET request to fetch the page
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(response.status_code)
    # Return the response status code
    return response.text,response.status_code

def extract_product_info(html_content):
    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the product info table
    table = soup.find('table', class_='product_info')

    # Initialize an empty dictionary to store the table data
    table_data = {}

    # If the table exists, iterate through each row to extract header-value pairs
    if table:
        for row in table.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            if cells:
                row_data = [cell.get_text(strip=True) for cell in cells]
                # Ensure there are exactly two columns (header and value)
                if len(row_data) == 2:
                    header = row_data[0].strip(':')  # Clean the header text (remove colon)
                    value = row_data[1].strip()  # Clean the value text

                    # If the header is "Size Charts", handle multiple links
                    if header == "Size Charts":
                        value = [link.get('href') for link in row.find_all('a')]

                    # Add the header-value pair to the dictionary
                    table_data[header] = value

    # Return the data as a JSON object (Python dictionary)
    return json.dumps(table_data, indent=4)

def extract_availability_quantity_and_eta(availability_text):
    # Initialize the default values
    quantity = 0
    eta = None

    # Search for the quantity in stock (number followed by "in stock")
    match_quantity = re.search(r'(\d+)\s*in\s*stock', availability_text)
    if match_quantity:
        quantity = int(match_quantity.group(1))

    # Search for ETA in the format "(ETA: <date>)"
    match_eta = re.search(r'\(ETA:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})\)', availability_text)
    if match_eta:
        # Parse the ETA date (e.g., "Jan 8, 2025") and convert it to the required ISO format
        eta_string = match_eta.group(1)
        eta_date = datetime.strptime(eta_string, "%b %d, %Y")
        eta = eta_date.isoformat() + '+00:00'  # Add UTC offset as '+00:00'
    else:
        eta = 0

    return quantity, eta



def connect_to_mongodb():
    """Connects to MongoDB and verifies the connection."""
    client = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
    except Exception as e:
        print(e)    
    return client  # Return the client object

def insert_to_mongodb(client, item_id, table_data):
    """Inserts product data into MongoDB."""
    if not table_data:
        print(f"No data to insert for item {item_id}.")
        return

    # Access or create the 'inventory' database
    db = client["Inventory"]

    # Create the 'ProductsMaster' collection
    products_master = db["Products Vendor Inventory Master"]

    try:
        result = products_master.insert_one(table_data)
        print(f"Document inserted for item {item_id} with ID: {result.inserted_id}")
    except Exception as e:
        print(f"Failed to insert data for item {item_id}: {e}")

def preprocess_prices(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    price_elements = soup.find_all('p', class_='product_price')
    prices = [price.get_text(strip=True) for price in price_elements] if price_elements else None

    processed_prices = {}
    if prices:
        prices_split = prices[0].replace("Wholesale: ", "").replace("Your Price: ", "").split("$")
        wholesale_price = float(prices_split[1].strip()) if len(prices_split) > 1 else 0.0
        your_price = float(prices_split[2].strip()) if len(prices_split) > 2 else None
        if your_price is None:
            your_price = wholesale_price

        processed_prices = {
            "Wholesale": round(wholesale_price, 2),
            "Your Price": round(your_price, 2)
        }

    map_price = 0.0  # Default to 0 if MAP Price is not found
    map_restriction = soup.find('p', class_='product_message red')
    if map_restriction:
        message_text = map_restriction.get_text(strip=True)
        match = re.search(r"MAP Price:\s*\$([\d.]+)", message_text)
        if match:
            map_price = float(match.group(1))  # Extract and convert to float
            map_price = round(map_price, 2)  # Round the MAP Price to 2 decimals
    processed_prices.update({'MAP':map_price})

    return processed_prices

def check_amazon_restriction(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    amazon_restriction = soup.find('div', class_='product_message')
    amazon_restricted = 1 if amazon_restriction else 0
    return amazon_restricted

def get_mpn_value(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    mpn_span = soup.find('span', itemprop='mpn')
    mpn_value = mpn_span.get_text(strip=True) if mpn_span else "MPN not found."
    return mpn_value



# Example usage:
# 43891

def main():
    for itemId in range(28074, 43891):
        item_id = itemId
        product_information = {}

        try:
            print(f"Fetching product for item ID {item_id}...")

            product, status_code = fetch_product_page(item_id, base_url)
            print(f"Status code for {item_id}: {status_code}")

            if status_code != 200 or not product:
                print(f"Failed to fetch product page for item ID {item_id}. Status code: {status_code}")
                continue

            soup = BeautifulSoup(product, 'html.parser')
            elements = soup.find_all('div', class_='col-lg-6 col-sm-12')
            if not elements:
                print(f"No product information found for item ID {item_id}")
                continue

            data = [element.text.strip() for element in elements if element.text.strip()]

            if len(data) > 1 and "stock" in data[1].lower():
                data[1] = f"Quantity: {data[1]}"

            processed_data = [item.replace('\n', ' ').replace('$', '') for item in data]

            data_dict = {
                item.split(":", 1)[0].strip(): item.split(":", 1)[1].strip()
                for item in processed_data if ":" in item
            }

            json_data = json.loads(json.dumps(data_dict, indent=4))
            print(f"Processed data for item ID {item_id}: {json_data}")

            product_information['Vendor SKU'] = json_data.get('SKU')
            product_information['Buy Price'] = json_data.get('Wholesale')
            product_information['Promotion Price'] = json_data.get('Your Price')
            product_information['MAP Price'] = json_data.get('MAP Price')
            product_information['Vendor Quantity'] = json_data.get('Quantity')
            product_information['Amazon Restricted'] = check_amazon_restriction(product)
            product_information['Inserted On'] = datetime.utcnow().isoformat()

            print(f"Inserting item ID {item_id} into MongoDB...")
            client = connect_to_mongodb()
            insert_to_mongodb(client, item_id, product_information)
            print(f"Successfully inserted item ID {item_id} into MongoDB.")

        except Exception as e:
            print(f"Error processing item ID {item_id}: {e}")
            traceback.print_exc()  # Print detailed error stack trace
            continue

if __name__ == "__main__":
    main()