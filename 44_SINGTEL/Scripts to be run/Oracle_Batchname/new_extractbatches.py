import re
import csv
from datetime import datetime

# Open the log file
with open('Autoloader_202408.log', 'r') as file:
    lines = file.readlines()

# Initialize variables
data_summary = []
results = []
count = 0

# Iterate through the lines of the log file
for line in lines:
    # Check for the start of the section
    if line.strip() == 'DataFileSummaryEnd':
        data_summary = []
        count = 0
    # Check for the end of the section
    elif count == 8:
        if data_summary:
            results.append(data_summary)
    # If the line is within the section, add it to the data summary
    elif data_summary is not None:
        data_summary.append(line.strip())
        count = count + 1

# Function to extract timestamp from filename
def extract_timestamp(filename):
    match = re.search(r'_(\d{8})(\d{6})\.txt$', filename)
    if match:
        return f"{match.group(1)}_{match.group(2)}"
    return None

# Function to parse the timestamp
def parse_timestamp(timestamp):
    return datetime.strptime(timestamp, '%Y%m%d_%H%M%S')

# Create a list of unique results
unique_results = []
seen = set()

for result in results:
    file_name = result[1]
    timestamp = extract_timestamp(file_name)
    if timestamp and (file_name, timestamp) not in seen:
        seen.add((file_name, timestamp))
        unique_results.append((file_name, timestamp))

# Sort the unique results by timestamp in descending order
sorted_results = sorted(unique_results, key=lambda x: parse_timestamp(x[1]), reverse=True)

# Write the sorted results to a CSV file
with open('aoutput.csv', 'w', newline='') as output_file:
    csv_writer = csv.writer(output_file)
    csv_writer.writerow(['File Name', 'Timestamp'])  # Write header
    for file_name, timestamp in sorted_results:
        csv_writer.writerow([file_name, timestamp])

print(f"Processed {len(sorted_results)} unique records.")