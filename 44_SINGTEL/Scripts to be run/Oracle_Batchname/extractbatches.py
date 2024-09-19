import re
import csv

 

# Open the log file
with open('Autoloader_202408.log', 'r') as file:
    lines = file.readlines()

# Initialize variables
data_summary = []
results = []
count=0
# Iterate through the lines of the log file
for line in lines:
    # Check for the start of the section
    if line.strip() == 'DataFileSummaryEnd':
        data_summary = []
        count=0
    # Check for the end of the section
    elif count==8:
        if data_summary:
            results.append(data_summary)
    # If the line is within the section, add it to the data summary
    elif data_summary is not None:
        data_summary.append(line.strip())
        count=count+1

# Write the results to a CSV file

unique_results = set()  # Set to store unique file names and timestamps
with open('aoutput.csv', 'w') as output_file:
    for result in results:
        file_name = result[1]
        timestamp = result[6]
        if (file_name, timestamp) not in unique_results:  # Check if the file name and timestamp have already been added
            unique_results.add((file_name, timestamp))  # Add the file name and timestamp to the set
            output_file.write(f"{file_name},{timestamp}\n")
