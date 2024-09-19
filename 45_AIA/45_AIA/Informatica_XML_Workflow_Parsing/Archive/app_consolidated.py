import xml.etree.ElementTree as ET
import pandas as pd

# Function to extract workflow details and sequences
def extract_workflow_details(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    workflow_details = []

    workflows = root.findall(".//WORKFLOW")
    for workflow in workflows:
        workflow_name = workflow.attrib.get('NAME', 'No Name')
        workflow_description = workflow.attrib.get('DESCRIPTION', 'No Description')
        has_sessions = 'No'

        # Extract session details if any
        sessions = workflow.findall(".//SESSION")
        if sessions:
            has_sessions = 'Yes'

        workflow_details.append({
            'Workflow Name': workflow_name,
            'Description': workflow_description,
            'Has Sessions': has_sessions
        })

    return pd.DataFrame(workflow_details)


# Function to extract session-level details
def extract_session_details(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    session_details = []

    sessions = root.findall(".//SESSION")
    for session in sessions:
        session_name = session.attrib.get('NAME', 'No Name')
        mapping_name = session.attrib.get('MAPPINGNAME', 'No Mapping')

        session_details.append({
            'Session Name': session_name,
            'Mapping Name': mapping_name
        })

    return pd.DataFrame(session_details)


# Function to extract task-instance details
def extract_task_instance_details(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    task_instance_details = []

    task_instances = root.findall(".//TASKINSTANCE")
    for task_instance in task_instances:
        task_name = task_instance.attrib.get('NAME', 'No Name')

        task_instance_details.append({
            'Task Instance Name': task_name
        })

    return pd.DataFrame(task_instance_details)


# Function to combine all the extractions and write to Excel
def combine_and_save_to_excel(workflow_xml_path, session_xml_path, output_excel_path):
    # Extract details
    workflow_df = extract_workflow_details(workflow_xml_path)
    session_df = extract_session_details(session_xml_path)
    task_instance_df = extract_task_instance_details(workflow_xml_path)

    # Create Excel writer object to write into multiple sheets
    with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
        # Write each DataFrame to a different worksheet
        workflow_df.to_excel(writer, sheet_name='Workflows', index=False)
        session_df.to_excel(writer, sheet_name='Sessions', index=False)
        task_instance_df.to_excel(writer, sheet_name='Task Instances', index=False)
    
    print(f"Data successfully written to {output_excel_path}")


# Example XML paths and output file path
workflow_xml_path = 'workflows.xml'  # Replace with actual path
session_xml_path = 'sample_session.xml'    # Replace with actual path
output_excel_path = 'workflow_session_task_details.xlsx'

# Call the function to combine and save
combine_and_save_to_excel(workflow_xml_path, session_xml_path, output_excel_path)
