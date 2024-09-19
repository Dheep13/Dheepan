import xml.etree.ElementTree as ET
import pandas as pd
import os

def extract_workflow_details_and_sequence(xml_path, excel_writer):
    # Parse the XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Lists to store workflow details, task sequences, and task instance details
    workflow_details = []
    task_sequences = []

    # Iterate over each workflow in the XML
    workflows = root.findall(".//WORKFLOW")
    for workflow in workflows:
        workflow_name = workflow.attrib.get('NAME', 'No Name')
        workflow_description = workflow.attrib.get('DESCRIPTION', 'No Description')

        # Workflow-level details
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

    # Convert lists to DataFrames
    details_df = pd.DataFrame(workflow_details)
    sequence_df = pd.DataFrame(task_sequences)

    # Write to Excel (to specified sheets)
    details_df.to_excel(excel_writer, sheet_name='Workflow Details', index=False)
    sequence_df.to_excel(excel_writer, sheet_name='Task Sequence', index=False)

def ensure_excel_file_exists(excel_file):
    # Check if the Excel file exists
    if not os.path.exists(excel_file):
        # Create an empty Excel file with no content (so that it's recognized by openpyxl)
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            pd.DataFrame().to_excel(writer)  # Creating a blank Excel file
        print(f"Created a new Excel file: {excel_file}")

# Example usage
excel_file_path = 'consolidated_workflow_task_details.xlsx'

# Ensure the Excel file exists
ensure_excel_file_exists(excel_file_path)

# Open the Excel writer in append mode and add new sheets
with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='a') as excel_writer:
    extract_workflow_details_and_sequence('workflows.XML', excel_writer)

print(f"Data has been written to {excel_file_path}")
