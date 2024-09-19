import xml.etree.ElementTree as ET
import pandas as pd
import os

def extract_all_details(element):
    """ Recursively extract all details from an element, including nested elements. """
    details = ', '.join([f"{key}='{value}'" for key, value in element.attrib.items()])
    for child in element:
        child_details = extract_all_details(child)
        details += ', ' + child_details if child_details else ''
    return details

def extract_workflow_details_and_sequence(xml_path):
    # Parse the XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Lists to store workflow details and task sequences
    workflow_details = []
    task_sequences = []
    task_details = []

    # Iterate over each workflow in the XML
    workflows = root.findall(".//WORKFLOW")
    for workflow in workflows:
        workflow_name = workflow.attrib.get('NAME', 'No Name')

        # Extract session details within this workflow
        sessions = workflow.findall(".//SESSION")
        for session in sessions:
            session_name = session.attrib.get('NAME')
            mapping_name = session.attrib.get('MAPPINGNAME', 'No Mapping')
            workflow_details.append({
                'Workflow Name': workflow_name,
                'Session Name': session_name,
                'Mapping Name': mapping_name
            })

        # Extract and consolidate task information
        tasks = workflow.findall(".//TASK")
        for task in tasks:
            task_info = extract_all_details(task)
            task_details.append({
                'Workflow Name': workflow_name,
                'Task Name': task.attrib.get('NAME'),
                'Task Info': task_info
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
    tasks_df = pd.DataFrame(task_details)

    # Save DataFrames to Excel
    excel_file_path = 'consolidated_workflow_details_and_sequence.xlsx'
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        details_df.to_excel(writer, sheet_name='Workflow Details', index=False)
        sequence_df.to_excel(writer, sheet_name='Task Sequence', index=False)
        tasks_df.to_excel(writer, sheet_name='Task Details', index=False)

    return details_df, sequence_df, tasks_df, excel_file_path

# Example usage
xml_file_path = 'workflows.XML'  # Adjust path as needed
details_df, sequence_df, tasks_df, excel_path = extract_workflow_details_and_sequence(xml_file_path)
print(f"Workflow details, sequence, and task details extracted and saved to {excel_path}")
