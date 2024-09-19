import xml.etree.ElementTree as ET
import pandas as pd

# Load your XML file
tree = ET.parse('workflows.XML')
root = tree.getroot()

# Define a list to store task instance data
task_instances_data = []

# Iterate over each TASKINSTANCE element
for task_instance in root.findall('.//TASKINSTANCE'):
    task_data = {
        'Name': task_instance.get('NAME'),
        'TaskName': task_instance.get('TASKNAME'),
        'TaskType': task_instance.get('TASKTYPE'),
        'IsEnabled': task_instance.get('ISENABLED'),
        'Reusable': task_instance.get('REUSABLE'),
        'FailParentIfInstanceDidNotRun': task_instance.get('FAIL_PARENT_IF_INSTANCE_DID_NOT_RUN'),
        'FailParentIfInstanceFails': task_instance.get('FAIL_PARENT_IF_INSTANCE_FAILS'),
        'TreatInputLinkAsAnd': task_instance.get('TREAT_INPUTLINK_AS_AND'),
        'Description': task_instance.get('DESCRIPTION')
    }
    
    # Collect attributes
    attributes = []
    for attr in task_instance.findall('.//ATTRIBUTE'):
        attributes.append({
            'AttributeName': attr.get('NAME'),
            'AttributeValue': attr.get('VALUE')
        })

    # Collect session extensions if any
    session_extensions = []
    for extension in task_instance.findall('.//SESSIONEXTENSION'):
        session_extensions.append({
            'ExtensionName': extension.get('NAME'),
            'InstanceName': extension.get('SINSTANCENAME'),
            'SubType': extension.get('SUBTYPE'),
            'TransformationType': extension.get('TRANSFORMATIONTYPE'),
            'Type': extension.get('TYPE')
        })

    # Collect config references if any
    config_references = []
    for config in task_instance.findall('.//CONFIGREFERENCE'):
        config_references.append({
            'RefObjectName': config.get('REFOBJECTNAME'),
            'Type': config.get('TYPE')
        })

    # Combine all data into the task data dictionary
    task_data['Attributes'] = attributes
    task_data['SessionExtensions'] = session_extensions
    task_data['ConfigReferences'] = config_references

    # Add task data to the list
    task_instances_data.append(task_data)

# Convert list of dictionaries to DataFrame for attributes, extensions, and config references
df_tasks = pd.json_normalize(task_instances_data, 'Attributes', ['Name', 'TaskName', 'TaskType', 'IsEnabled', 'Reusable', 'FailParentIfInstanceDidNotRun', 'FailParentIfInstanceFails', 'TreatInputLinkAsAnd', 'Description'],
                             errors='ignore', record_prefix='Attr_')

df_extensions = pd.json_normalize(task_instances_data, 'SessionExtensions', ['Name', 'TaskName'],
                                  errors='ignore', record_prefix='Ext_')

df_configs = pd.json_normalize(task_instances_data, 'ConfigReferences', ['Name', 'TaskName'],
                               errors='ignore', record_prefix='Config_')

# Combine all data into one DataFrame
df_final = pd.concat([df_tasks, df_extensions, df_configs], axis=1)

# Save the DataFrame to an Excel file
df_final.to_excel('task_instance_details.xlsx', index=False)

print("Excel file has been created with task instance details.")
