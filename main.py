import os
import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import re
import pandas as pd
from bs4 import BeautifulSoup
import os


uri = os.environ['DB_ACCESS_URI']
base_url = os.environ['BASE_URL']
headers = {
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
          'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
          'Cache-Control': 'max-age=0',
          'Connection': 'keep-alive',
          'Cookie': '_ga=GA1.1.491217983.1734183528; MCPopupClosed=yes; age=eyJpdiI6InMzRFg4SzJwSW90U0N5eGhTRVp4S3c9PSIsInZhbHVlIjoiODhVd3RpZ1VNeElsTCswaDRzYlwvQWJ4VEk3S1dzZmRTTzROcTBWa1paSzcrT0dSMnZ5MCs1WFIwZlwvbjRkOGlqWUFXZlVtaFVac1ZNYktHMnN2MTUwRUUzVG9xTGZtVkRoeGpJSkhxVG55Qm9TWDZTZnJDMzMwdk15a25RN2QxWU1DUTJybElOOTlUMTY0cmVhYXVNcTF5NUYzOU00VVhJYStzU2dHa1RTVUVHK0dpK2czWnc1eXdPVG9mdXRSd2pFcmRValF3cVJLbUZJeFM4d2d3R1NqV2dIU2ZyV0tJXC93QUt3NHpGSDdtWkg3WlR5V2dqbGxKUG5QV2xzRUdidDQ3V0kyWFZwblwvaXR5M0VEMkpcL3FjQT09IiwibWFjIjoiYzA2ZWYwMDlkYjM0OWQ3NjU3MThlYjkyNWI2MGIxNzEyMjM5NDdlZGNjZmEwODQyNjliYjU2NzcyMjhiNDdiZCJ9; remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d=eyJpdiI6Iklqd3JoSUc3SG8yU2VFeldscno0amc9PSIsInZhbHVlIjoia3pSOEZwdHlBeUIyQ291QStwd3RoUzBLQjUxdjdPSmpsTnN4dVlndlU0TVVpUXVtQUhKQlBHZ3dpWjdrZlpicEZySHo2WTBwbnJKYUhKb0xQU1BGZHN4bWVVYXZXajBqYUlPc0NrT2JaYnc9IiwibWFjIjoiMWMyODI4YzE1ZTIxMWEzMzIyOWIxY2E5NmZiNTI4ZTIzYTQ3MjU4NWY5MTZkM2VlZjM0ZDY3YmM2ODQ0ZDljNSJ9; laravel_session=eyJpdiI6ImdtcFRVcWVqVmVBdjhUUVp6SFo2S0E9PSIsInZhbHVlIjoieU5mRWRUSVlZUEd2VmFwN00zT1pobFwvYUtsY3loeDFyRWFOTjV5SjRQb3o3YXVreFBINFlTUG9rSTkzWHFlbmNvRG9SalBoSk9MN3hlVkhZK09EV3lRPT0iLCJtYWMiOiIxMWEyMDhhYmQ2YTE4YzBkMjM1MDE2NGY4ZGEwMzMxYTRiNmZiODQ4MjZlOTgwNzJhMzdkYTZkYzQxOThlYjUxIn0%3D; _ga_Z5QXH7PSMN=GS1.1.1735136798.14.1.1735139096.0.0.0',
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'none',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
          'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"macOS"',
      }

def connect_to_mongodb():
    """Connects to MongoDB and verifies the connection."""
    client = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
    except Exception as e:
        print(e)    
    return client  # Return the client object

def fetch_product_data(item_id, headers):
    """Fetches product data from Honeysplace using dynamic URL."""
    url = f'{base_url}{item_id}'  # Dynamically generate the URL
    print(url)    

    try:
        response = requests.get(url, headers=headers)
        print(response.status_code)        
        # Check if the request was successful
        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')

            table = soup.find('table', class_='product_info')
            table_data = {}
            if table:
                for row in table.find_all('tr'):
                    cells = row.find_all(['th', 'td'])
                    if cells:
                        row_data = [cell.get_text(strip=True) for cell in cells]
                        if len(row_data) == 2:
                            table_data[row_data[0].strip(':')] = row_data[1].strip()

            mpn_span = soup.find('span', itemprop='mpn')
            mpn_value = mpn_span.get_text(strip=True) if mpn_span else "MPN not found."

            price_elements = soup.find_all('p', class_='product_price')
            prices = [price.get_text(strip=True) for price in price_elements] if price_elements else None

            # Process prices into a dictionary
            processed_prices = {}
            if prices:
                prices_split = prices[0].replace("Wholesale: ", "").replace("Your Price: ", "").split("$")
                processed_prices = {
                    "Wholesale": f"${prices_split[1].strip()}",
                    "Your Price": f"${prices_split[2].strip()}" if len(prices_split) > 2 else None
                }

            availability = table_data.get('Availability', '')
            stock_number = re.search(r'\d+', availability).group() if re.search(r'\d+', availability) else "Stock number not found."

            # Clean the prices
            def clean_price(price):
                return round(float(re.sub(r'[^0-9]', '', price)) / 100, 2)

            cleaned_prices = {key: clean_price(value) for key, value in processed_prices.items() if value}

            # Combine all data into a single dictionary
            table_data.update({
                "MPN": mpn_value,
                "Stock Number": stock_number,
                "Wholesale": cleaned_prices.get('Wholesale', None),
                "Price": cleaned_prices.get('Your Price', None)
            })
            
            return table_data
        else:
            print("Error May Be!!")
            return None
        
    except Exception as e:
        print(f"Failed to fetch product data for item {item_id}: {e}")
        return None

def insert_to_mongodb(client, item_id, table_data):
    """Inserts product data into MongoDB."""
    if not table_data:
        print(f"No data to insert for item {item_id}.")
        return

    # Access or create the 'inventory' database
    db = client["Inventory"]

    # Create the 'ProductsMaster' collection
    products_master = db["Products Master"]

    # Insert a document with dynamic 'created_at'
    product = {
        "ID": item_id,
        "Product ID": item_id,
        "Vendor SKU": table_data.get('MPN', 'N/A'),
        "UPC": table_data.get('Product UPC', 'N/A'),
        "Manufacturer": table_data.get('Manufacturer', 'N/A'),
        "Inserted On": datetime.now()  # Dynamic server time
    }

    try:
        result = products_master.insert_one(product)
        print(f"Document inserted for item {item_id} with ID: {result.inserted_id}")
    except Exception as e:
        print(f"Failed to insert data for item {item_id}: {e}")

def main():
    client = connect_to_mongodb()

    for item_id in range(17622,42000):
        print(f"Fetching data for item {item_id}...")
        table_data = fetch_product_data(item_id,headers)
        print(f"Fetching data for item {table_data}...")
        insert_to_mongodb(client,item_id, table_data)

if __name__ == "__main__":
    main()
