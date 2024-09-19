import xml.etree.ElementTree as ET
import pandas as pd

def extract_all_workflows_details_and_sequences(xml_path):
    # Parse the XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Initialize lists to store all workflows' details and sequences
    all_workflows_details = []
    all_task_sequences = []

    # Iterate over each workflow in the XML
    workflows = root.findall(".//WORKFLOW")
    for workflow in workflows:
        workflow_name = workflow.attrib.get('NAME', 'No Name')

        # Extract session details within this workflow
        sessions = workflow.findall(".//SESSION")
        for session in sessions:
            session_name = session.attrib.get('NAME')
            mapping_name = session.attrib.get('MAPPINGNAME')
            all_workflows_details.append({
                'Workflow Name': workflow_name,
                'Session Name': session_name,
                'Mapping Name': mapping_name
            })

        # Extract workflow links within this workflow to determine the sequence of tasks
        links = workflow.findall(".//WORKFLOWLINK")
        for link in links:
            from_task = link.attrib.get('FROMTASK')
            to_task = link.attrib.get('TOTASK')
            condition = link.attrib.get('CONDITION', 'N/A')  # Capture conditions if any
            all_task_sequences.append({
                'Workflow Name': workflow_name,
                'From Task': from_task,
                'To Task': to_task,
                'Condition': condition
            })

    # Convert lists to DataFrames
    details_df = pd.DataFrame(all_workflows_details)
    sequence_df = pd.DataFrame(all_task_sequences)

    # Save DataFrames to Excel
    excel_file_path = 'consolidated_workflow_details_and_sequence.xlsx'
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        details_df.to_excel(writer, sheet_name='Workflow Details', index=False)
        sequence_df.to_excel(writer, sheet_name='Task Sequence', index=False)

    return details_df, sequence_df, excel_file_path

# Example usage
xml_file_path = 'workflows.XML'  # Update this with the correct file path
details_df, sequence_df, excel_path = extract_all_workflows_details_and_sequences(xml_file_path)
print(f"All workflows details and sequence extracted and saved to {excel_path}")
print("Workflow Details:")
print(details_df)
print("\nTask Sequence:")
print(sequence_df)
