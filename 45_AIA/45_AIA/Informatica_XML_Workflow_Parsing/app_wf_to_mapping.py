import xml.etree.ElementTree as ET
import pandas as pd
import os

# Ensure Excel file exists before writing
def ensure_excel_file_exists(excel_file):
    if not os.path.exists(excel_file):
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            pd.DataFrame().to_excel(writer)
        print(f"Created a new Excel file: {excel_file}")

# Function from previous script 1 (add workflow details and task sequences)
def extract_workflow_details_and_sequence(xml_path, excel_writer):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    workflow_details = []
    task_sequences = []

    workflows = root.findall(".//WORKFLOW")
    for workflow in workflows:
        workflow_name = workflow.attrib.get('NAME', 'No Name')
        workflow_description = workflow.attrib.get('DESCRIPTION', 'No Description')

        workflow_details.append({
            'Workflow Name': workflow_name,
            'Workflow Description': workflow_description,
        })

        # Extract workflow links to determine the sequence of tasks
        links = workflow.findall(".//WORKFLOWLINK")
        for link in links:
            from_task = link.attrib.get('FROMTASK')
            to_task = link.attrib.get('TOTASK')
            condition = link.attrib.get('CONDITION', 'N/A')
            task_sequences.append({
                'Workflow Name': workflow_name,
                'From Task': from_task,
                'To Task': to_task,
                'Condition': condition
            })

    # details_df = pd.DataFrame(workflow_details)
    # sequence_df = pd.DataFrame(task_sequences)

    # details_df.to_excel(excel_writer, sheet_name='Workflow Details2', index=False)
    # sequence_df.to_excel(excel_writer, sheet_name='Task Sequence2', index=False)

# Function from new script (extract all workflows and sequences)
def extract_all_workflows_details_and_sequences(xml_path, excel_writer):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    all_workflows_details = []
    all_task_sequences = []

    workflows = root.findall(".//WORKFLOW")
    for workflow in workflows:
        workflow_name = workflow.attrib.get('NAME', 'No Name')

        # Extract session details
        sessions = workflow.findall(".//SESSION")
        for session in sessions:
            session_name = session.attrib.get('NAME')
            mapping_name = session.attrib.get('MAPPINGNAME')
            all_workflows_details.append({
                'Workflow Name': workflow_name,
                'Session Name': session_name,
                'Mapping Name': mapping_name
            })

        # Extract workflow links to determine task sequence
        links = workflow.findall(".//WORKFLOWLINK")
        for link in links:
            from_task = link.attrib.get('FROMTASK')
            to_task = link.attrib.get('TOTASK')
            condition = link.attrib.get('CONDITION', 'N/A')
            all_task_sequences.append({
                'Workflow Name': workflow_name,
                'From Task': from_task,
                'To Task': to_task,
                'Condition': condition
            })

    details_df = pd.DataFrame(all_workflows_details)
    # sequence_df = pd.DataFrame(all_task_sequences)

    details_df.to_excel(excel_writer, sheet_name='All Workflows Details', index=False)
    # sequence_df.to_excel(excel_writer, sheet_name='All Task Sequences', index=False)

# Combined script execution
excel_file_path = 'consolidated_workflow_task_details.xlsx'
xml_file_path = 'workflows.XML'

# Ensure the Excel file exists
ensure_excel_file_exists(excel_file_path)

# Open the Excel writer in append mode and add new sheets
with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='a') as excel_writer:
    extract_workflow_details_and_sequence(xml_file_path, excel_writer)
    extract_all_workflows_details_and_sequences(xml_file_path, excel_writer)

print(f"Data has been written to {excel_file_path}")
