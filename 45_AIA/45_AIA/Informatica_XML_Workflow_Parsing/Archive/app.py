import xml.etree.ElementTree as ET
import pandas as pd

# Load your XML data
xml_file = 'workflow_xml.xml'  # Update this with the correct file path
tree = ET.parse(xml_file)
root = tree.getroot()

# Initialize data dictionary with lists for each key tag
tags_to_extract = [
    'SCHEDULER', 'TASK', 'SESSION', 'SESSTRANSFORMATIONINST', 'PARTITION', 
    'CONFIGREFERENCE', 'SESSIONCOMPONENT', 'SESSIONEXTENSION', 'WORKFLOWLINK', 
    'WORKFLOWVARIABLE', 'ATTRIBUTE'
]
data = {tag: [] for tag in tags_to_extract}

# Function to extract attributes and add to data dictionary
def extract_attributes(element, tag_name):
    for item in element.findall('.//' + tag_name):
        attributes = {**item.attrib, 'parent': element.attrib.get('NAME', 'N/A')}
        # Extracting embedded ATTRIBUTE tags within any element
        attributes.update({attr.get('NAME'): attr.get('VALUE') for attr in item.findall('.//ATTRIBUTE')})
        data[tag_name].append(attributes)

# Extract details for all specified tags
for tag in tags_to_extract:
    extract_attributes(root, tag)

# Convert data to DataFrames for each tag
dataframes = {tag: pd.DataFrame(data[tag]) for tag in tags_to_extract if data[tag]}

# Optionally, save each DataFrame to an Excel file with a sheet for each tag
with pd.ExcelWriter('workflow_details.xlsx', engine='openpyxl') as writer:
    for tag, df in dataframes.items():
        df.to_excel(writer, sheet_name=tag, index=False)

print("Data has been processed and saved to Excel.")
