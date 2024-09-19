import xml.etree.ElementTree as ET
import pandas as pd
import os

def extract_workflow_details_and_sequence(xml_path):
    # Parse the XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Lists to store workflow details, task sequences, and task instance details
    workflow_details = []
    task_sequences = []
    task_details = []
    task_instance_details = []

    # Iterate over each workflow in the XML
    workflows = root.findall(".//WORKFLOW")
    for workflow in workflows:
        workflow_name = workflow.attrib.get('NAME', 'No Name')
        workflow_description = workflow.attrib.get('DESCRIPTION', 'No Description')

        # Workflow-level details
        workflow_details.append({
            'Workflow Name': workflow_name,
            'Workflow Description': workflow_description,
            'Has Sessions': 'No'  # Default value, updated below if sessions are found
        })

        # Extract session and taskinstance details within this workflow
        sessions = workflow.findall(".//SESSION")
        for session in sessions:
            session_name = session.attrib.get('NAME')
            mapping_name = session.attrib.get('MAPPINGNAME', 'No Mapping')
            
            # Update the last entry if sessions are found
            workflow_details[-1]['Has Sessions'] = 'Yes'

            workflow_details.append({
                'Workflow Name': workflow_name,
                'Session Name': session_name,
                'Mapping Name': mapping_name,
                'Workflow Description': ''  # Avoid duplicating workflow description
            })

        # Extract task instance details
        task_instances = workflow.findall(".//TASKINSTANCE")
        for task_instance in task_instances:
            task_instance_details.append({
                'Workflow Name': workflow_name,
                'Task Instance Name': task_instance.attrib.get('NAME'),
                'Task Name': task_instance.attrib.get('TASKNAME'),
                'Task Type': task_instance.attrib.get('TASKTYPE'),
                'Is Enabled': task_instance.attrib.get('ISENABLED', 'No'),
                'Reusable': task_instance.attrib.get('REUSABLE', 'No')
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
    tasks_df = pd.DataFrame(task_instance_details)

    # Save DataFrames to Excel
    excel_file_path = 'consolidated_workflow_details_and_sequence_test.xlsx'
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        details_df.to_excel(writer, sheet_name='Workflow Details', index=False)
        sequence_df.to_excel(writer, sheet_name='Task Sequence', index=False)
        tasks_df.to_excel(writer, sheet_name='Task Instance Details', index=False)

    return details_df, sequence_df, tasks_df, excel_file_path

# Example usage
xml_file_path = 'workflows.XML'  # Adjust path as needed
details_df, sequence_df, tasks_df, excel_path = extract_workflow_details_and_sequence(xml_file_path)
print(f"Workflow details, sequence, and task instance details extracted and saved to {excel_path}")
