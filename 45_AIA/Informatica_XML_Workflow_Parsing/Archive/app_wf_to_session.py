import xml.etree.ElementTree as ET
import pandas as pd

# Load your XML file
tree = ET.parse('workflows.XML')
root = tree.getroot()

# Define a list to store session data
sessions_data = []

# Iterate over each SESSION element
for session in root.findall('.//SESSION'):
    session_data = {
        'Name': session.get('NAME'),
        'MappingName': session.get('MAPPINGNAME'),
        'IsValid': session.get('ISVALID'),
        'Reusable': session.get('REUSABLE'),
        'VersionNumber': session.get('VERSIONNUMBER'),
        'Description': session.get('DESCRIPTION')
    }
    
    # Collect data from transformation instances and other sub-elements
    transformations = []
    for trans in session.findall('.//SESSTRANSFORMATIONINST'):
        transformations.append({
            'InstanceName': trans.get('SINSTANCENAME'),
            'TransformationName': trans.get('TRANSFORMATIONNAME'),
            'Type': trans.get('TRANSFORMATIONTYPE'),
            'Pipeline': trans.get('PIPELINE'),
            'Stage': trans.get('STAGE'),
            'IsRepartitionPoint': trans.get('ISREPARTITIONPOINT'),
            'PartitionType': trans.get('PARTITIONTYPE')
        })
    session_data['Transformations'] = transformations
    
    # Add session data to the list
    sessions_data.append(session_data)

# Convert list of dictionaries to DataFrame
df_sessions = pd.json_normalize(sessions_data, 'Transformations', ['Name', 'MappingName', 'IsValid', 'Reusable', 'VersionNumber', 'Description'])

# Sort the DataFrame by Name and Stage
df_sessions.sort_values(by=['Name', 'Stage'], ascending=[True, True], inplace=True)

# Save the DataFrame to an Excel file
df_sessions.to_excel('session_details.xlsx', index=False)

print("Excel file has been created with session details sorted by Name and Stage.")
