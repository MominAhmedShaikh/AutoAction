import os
import logging
import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import re
import pandas as pd
from bs4 import BeautifulSoup
import logging
import os

def create_directories_dynamically(root_directory, sub_directories):
  """
  Creates the specified directories dynamically.

  Args:
    root_directory: The root directory path.
    sub_directories: A list of subdirectories to create within the root directory.

  Returns:
    The full path to the created directory.
  """
  try:
    full_path = os.path.join(root_directory, *sub_directories) 
    os.makedirs(full_path, exist_ok=True)
    return full_path
  except OSError as error:
    print(f"Error creating directories: {error}")
    return None

# Example usage:
root_dir = os.getcwd()  # Get the current working directory
sub_dirs = ["AutoAction", "AutoAction", "logs"] 
log_dir = create_directories_dynamically(root_dir, sub_dirs)

if log_dir:
  log_file_path = os.path.join(log_dir, "app.log")
  # Configure logging (as before)
  logging.basicConfig(filename=log_file_path, level=logging.INFO, 
                      format='%(asctime)s - %(levelname)s - %(message)s')
  logger = logging.getLogger(__name__)
  logger.info('Starting the process...')
  
# Setup logging
LOG_FILE = log_file_path

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

uri = os.environ('DB_ACCESS_URI')
base_url = os.environ('BASE_URL')
headers = os.environ('HEADERS')

def connect_to_mongodb():
    """Connects to MongoDB and verifies the connection."""
    client = MongoClient(uri, server_api=ServerApi('1'))
    
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        logging.info("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
    
    return client  # Return the client object

def fetch_product_data(item_id, headers):
    """Fetches product data from Honeysplace using dynamic URL."""
    url = f'{base_url}{item_id}'  # Dynamically generate the URL
    logging.info(f"Fetching URL: {url}")  # Log the generated URL
    
    try:
        response = requests.get(url, headers=headers)
        logging.info(f"Status Code for item {item_id}: {response.status_code}")
        
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
            logging.error(f"Failed to fetch data for item {item_id}. Status Code: {response.status_code}")
            return None
        
    except Exception as e:
        logging.error(f"Failed to fetch product data for item {item_id}: {e}")
        return None

def insert_to_mongodb(client, item_id, table_data):
    """Inserts product data into MongoDB."""
    if not table_data:
        logging.error(f"No data to insert for item {item_id}.")
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
        logging.info(f"Document inserted for item {item_id} with ID: {result.inserted_id}")
    except Exception as e:
        logging.error(f"Failed to insert data for item {item_id}: {e}")

def main():
    client = connect_to_mongodb()

    for item_id in range(17621, 42000):
        logging.info(f"Fetching data for item {item_id}...")
        table_data = fetch_product_data(item_id,headers)
        logging.info(f"Fetching data for item {table_data}...")
        insert_to_mongodb(client,item_id, table_data)

if __name__ == "__main__":
    main()
