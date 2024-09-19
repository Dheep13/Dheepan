import pandas as pd

# Set the file paths
excel_file_path = 'data.csv'
output_file_path = 'output.txt'

# Read the Excel file into a pandas DataFrame
df = pd.read_csv(excel_file_path)

# Drop any rows with missing values (blank rows)
df.dropna(axis=0, how='all', inplace=True)

# Convert DataFrame to pipe-delimited text without gaps between rows
text_data = df.to_csv(sep='|', index=False)

# Strip leading and trailing whitespace from each row
text_data = '\n'.join(row.strip() for row in text_data.split('\n'))

# Write the text data to a file with UTF-8 encoding
with open(output_file_path, 'w', encoding='utf-8') as file:
    file.write(text_data)

print("Conversion complete. Output file saved as:", output_file_path)