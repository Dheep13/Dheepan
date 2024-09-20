import requests
import json
import sqlite3
from cred import api_username, api_password

# SAP Commissions REST API URL
api_url = "https://0509.callidusondemand.com/api/v2"

# Database connection
db_connection = sqlite3.connect('local.db')
cursor = db_connection.cursor()

def fetch_and_store_data():
    # Define API endpoint for salesTransactions (replace with the actual endpoint)
    transactions_endpoint = "https://0509.callidusondemand.com/api/v2/salesTransactions?$filter=compensationDate ge 2023-01-01 and compensationDate le 2023-12-31"
        # credits_endpoint = "https://0509.callidusondemand.com/api/v2/credits?$filter=compensationDate ge 2023-01-01 and compensationDate le 2023-12-31"

    # Initialize variables for pagination
    page = 1
    page_size = 100  # Adjust the page size as needed
    has_more_data = True

    while has_more_data:
        # Build the API request URL with pagination parameters
        page_url = f"{transactions_endpoint}&skip={(page - 1) * page_size}&top={page_size}"

        # Fetch data from the API for the current page
        transactions_data = fetch_data(page_url)
        # credits_data = fetch_data(credits_endpoint)

        # Check if there is more data to retrieve
        if transactions_data and len(transactions_data.get("salesTransactions", [])) > 0:
            # Store data in the local cs_salestransaction table
            store_data_in_db(transactions_data, "cs_salestransaction")
            # store_data_in_db(credits_data, "cs_credit")

            # Increment the page number for the next request
            page += 1
        else:
            has_more_data = False

# Define a function to fetch data from a specific API endpoint
def fetch_data(endpoint):
    url = endpoint
    headers = {"Content-Type": "application/json"}
    auth = (api_username, api_password)

    response = requests.get(url, headers=headers, auth=auth)

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(f"Failed to fetch data from {endpoint}. Status code: {response.status_code}")
        return None

# Define a function to store data in the local cs_salestransaction table
def store_data_in_db(data, table_name):
    if data is not None:
        for item in data.get("salesTransactions", []):
            print("compensationDate:", item.get("compensationDate"))
            print("processingUnitSeq:", item.get("processingUnitSeq"))
            print("preadjustedValue:", item.get("preadjustedValue", {}).get("value"))
            print("eventTypeSeq:", item.get("eventTypeSeq"))
            print("salesOrderSeq:", item.get("salesOrderSeq"))
            print("salesTransactionSeq:", item.get("salesTransactionSeq"))
            print("value:", item.get("value", {}).get("value"))
            print("lineNumber:", item.get("lineNumber", {}).get("value"))
            print("subLineNumber:", item.get("subLineNumber", {}).get("value"))
            print("modificationDate:", item.get("modificationDate"))
            print("-" * 50)  # Separating lines for clarity
            # Assuming your cs_salestransaction table has columns matching the JSON structure
            # Adjust the column names accordingly in the INSERT statement
            cursor.execute(
                f"INSERT INTO {table_name} (compensationDate, originTypeId, processingUnitSeq, preadjustedValue,UNITTYPEFORPREADJUSTEDVALUE,eventTypeseq, salesOrderSeq, salesTransactionSeq, value,UNITTYPEFORVALUE, lineNumber, subLineNumber, modificationDate,isRunnable,modelSeq ) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    item.get("compensationDate"),
                    item.get("originTypeId"),
                    item.get("processingUnit"),
                    item.get("preadjustedValue", {}).get("value"),
                    item.get("preadjustedValue", {}).get("unitType",{}).get("unitTypeSeq"),
                    item.get("eventType"),
                    item.get("salesOrder"),
                    # item.get("transactionAssignments", {}).get("key"),
                    # item.get("transactionAssignments", {}).get("ownedKey"),
                    item.get("salesTransactionSeq"),
                    item.get("value", {}).get("value"),
                    item.get("value", {}).get("unitType",{}).get("unitTypeSeq"),
                    item.get("lineNumber", {}).get("value"),
                    item.get("subLineNumber", {}).get("value"),
                    item.get("modificationDate"),
                    item.get("isRunnable"),
                    item.get("modelSeq")
                ),
            )

        db_connection.commit()
        print(f"{len(data['salesTransactions'])} records inserted into {table_name} table.")

# Execute the data retrieval and storage process
fetch_and_store_data()

# Close the database connection
db_connection.close()