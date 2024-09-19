import pandas as pd

# Load the CSV files into DataFrames
file1 = 'stel_BCC-SCII-BundleOrders_20240727060122.txt.csv'  # Replace with your first file's path
file2 = '1756_BCCSCIIBundleOrders_20240727060122.txt.csv'  # Replace with your second file's path

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Define the keys for merging
# keys = ['ORDERID', 'LINENUMBER', 'SUBLINENUMBER', 'EVENTTYPEID']
keys = ['ORDER_ACTION_ID', 'ORDERTYPE', 'CUSTOMER_ID', 'SERVICE_NO','SALESMAN_CODE']
# ORDER_ACTION_ID , ORDERTYPE, CUSTOMER_ID, SERVICE_NO, SALESMAN_CODE
# Merge the two DataFrames on the specified keys
merged_df = pd.merge(df1, df2, on=keys, suffixes=('_file1', '_file2'))

# Initialize a list to store the differences
differences = []

# List of columns to exclude from comparison (the first four columns)
# excluded_columns = ['FILEDATE', 'FILENAME', 'RECORDSTATUS', 'DOWNLOADED']
excluded_columns = ['FILEDATE', 'FILENAME', 'RECORDSTATUS']
# Get the columns to compare by excluding the keys and excluded_columns
columns_to_compare = [col for col in df1.columns if col not in keys + excluded_columns]

# Loop through each row and compare the relevant columns
for index, row in merged_df.iterrows():
    for column in columns_to_compare:
        column_file1 = f"{column}_file1"
        column_file2 = f"{column}_file2"

                # Check if both values are NaN (null), treat them as equal
        if pd.isna(row[column_file1]) and pd.isna(row[column_file2]):
            continue  # Skip this comparison

        # if row[column_file1] != row[column_file2]:
        #     differences.append({
        #         'ORDERID': row['ORDERID'],
        #         'LINENUMBER': row['LINENUMBER'],
        #         'SUBLINENUMBER': row['SUBLINENUMBER'],
        #         'EVENTTYPEID': row['EVENTTYPEID'],
        #         'COLUMN_NAME': column,
        #         'ORACLE': row[column_file1],
        #         'HANA': row[column_file2]
        #     })

        if row[column_file1] != row[column_file2]:
            differences.append({
                'ORDER_ACTION_ID': row['ORDER_ACTION_ID'],
                'ORDERTYPE': row['ORDERTYPE'],
                'CUSTOMER_ID': row['CUSTOMER_ID'],
                'SERVICE_NO': row['SERVICE_NO'],
                'SALESMAN_CODE': row['SALESMAN_CODE'],
                'COLUMN_NAME': column,
                'ORACLE': row[column_file1],
                'HANA': row[column_file2]
            })

# Convert the list of differences to a DataFrame
differences_df = pd.DataFrame(differences)

# Save the differences to a CSV file
differences_df.to_csv('differences_' + file2, index=False)


print("Comparison completed. Differences have been saved to 'differences.csv'.")

