import requests
import csv
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression

# SAP Commissions API endpoint and authentication details
api_credits_url = 'https://0509.callidusondemand.com/api/v2/credits'
api_txns_url = 'https://0509.callidusondemand.com/api/v2/transactions'

api_token = 'your_api_token'

# Make the API request
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}

response = requests.get(api_url, headers=headers)

# Check if the API request was successful
if response.status_code == 200:
    # Extract sales transaction data
    sales_transactions = response.json()

    # Define the CSV file path
    csv_file = 'sales_transactions.csv'

    # Extract relevant information from the sales transactions
    extracted_data = []
    for transaction in sales_transactions:
        transaction_id = transaction['id']
        amount = transaction['amount']
        customer_name = transaction['customer_name']
        # Add more fields as needed

        # Append extracted data to the list
        extracted_data.append([transaction_id, amount, customer_name])  # Add more fields as needed

    # Save the extracted data as a CSV file
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Transaction ID', 'Amount', 'Customer Name'])  # Add more fields as needed
        writer.writerows(extracted_data)

    print(f"Sales transaction data has been saved to {csv_file} successfully.")
else:
    print("Failed to retrieve sales transaction data. Please check your API request.")



x = np.array([-2, -1, 0, 1, 2])  # Independent variable (input)
y = np.array([6, 1, 0, 1, 6])    # Dependent variable (output)
X = np.vstack((np.ones(len(x)), x, x**2)).T
print(X)
coefficients = np.linalg.inv(X.T.dot(X)).dot(X.T).dot(y)
beta0, beta1, beta2 = coefficients

# Predict the values
y_pred = beta0 + beta1 * x + beta2 * x**2

plt.scatter(x, y, color='blue', label='Actual')
plt.plot(x, y_pred, color='red', label='Linear Regression',linestyle='dashed')
plt.xlabel('x')
plt.ylabel('y')
plt.title('Linear Regression in One Variable')
plt.legend()
plt.show()