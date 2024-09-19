import xml.etree.ElementTree as ET
import pandas as pd

def extract_session_details(xml_path, excel_writer):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Session details
    session_details = []
    workflows = root.findall(".//WORKFLOW")
    for workflow in workflows:
        workflow_name = workflow.attrib.get('NAME', 'No Name')

        sessions = workflow.findall(".//SESSION")
        for session in sessions:
            session_name = session.attrib.get('NAME')
            mapping_name = session.attrib.get('MAPPINGNAME', 'No Mapping')
            session_details.append({
                'Workflow Name': workflow_name,
                'Session Name': session_name,
                'Mapping Name': mapping_name
            })

    # Convert to DataFrame
    session_df = pd.DataFrame(session_details)

    # Write to Excel
    session_df.to_excel(excel_writer, sheet_name='Session Details', index=False)

# Example usage
excel_writer = pd.ExcelWriter('consolidated_workflow_task_details.xlsx', engine='openpyxl', mode='a')
extract_session_details('workflows.XML', excel_writer)
excel_writer._save()
