import xml.etree.ElementTree as ET
import pandas as pd
import os

def parse_mapping_xml(mapping_xml_path):
    """ Parses the mapping XML for mappings and transformations information. """
    tree = ET.parse(mapping_xml_path)
    root = tree.getroot()
    mapping_details = []

    # Extract mapping and transformation details
    mappings = root.findall(".//MAPPING")
    for mapping in mappings:
        mapping_name = mapping.attrib.get('NAME')
        transformations = mapping.findall(".//TRANSFORMATION")
        for transformation in transformations:
            trans_name = transformation.attrib.get('NAME')
            trans_type = transformation.attrib.get('TYPE')
            for table_attr in transformation.findall('.//TABLEATTRIBUTE'):
                attr_name = table_attr.attrib.get('NAME')
                attr_value = table_attr.attrib.get('VALUE')
                mapping_details.append({
                    'Mapping Name': mapping_name,
                    'Transformation Name': trans_name,
                    'Type': trans_type,
                    'Table Attribute Name': attr_name,
                    'Table Attribute Value': attr_value
                })

    return mapping_details

def parse_workflow_xml(workflow_xml_path):
    """ Parses the workflow XML for workflow details and sequence information. """
    tree = ET.parse(workflow_xml_path)
    root = tree.getroot()
    workflow_details = []
    task_details = []

    # Extract information from workflows
    workflows = root.findall(".//WORKFLOW")
    for workflow in workflows:
        workflow_name = workflow.attrib.get('NAME', 'No Name')

        # Tasks within workflows
        tasks = workflow.findall(".//TASK")
        for task in tasks:
            task_info = ', '.join([f"{key}='{value}'" for key, value in task.attrib.items()])
            task_details.append({
                'Workflow Name': workflow_name,
                'Task Name': task.attrib.get('NAME'),
                'Task Info': task_info
            })

    return workflow_details, task_details

def create_combined_excel(output_path, mapping_details, workflow_details, task_details):
    """ Save the extracted data to an Excel file with multiple sheets. """
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        pd.DataFrame(mapping_details).to_excel(writer, sheet_name='Mapping Details', index=False)
        pd.DataFrame(workflow_details).to_excel(writer, sheet_name='Workflow Details', index=False)
        pd.DataFrame(task_details).to_excel(writer, sheet_name='Task Details', index=False)

        # Merge mapping and workflow details for a combined view
        combined_df = pd.DataFrame(mapping_details)
        combined_df['Workflow Name'] = 'Workflow Name Here'  # This would need to be determined or joined based on additional logic
        combined_df.to_excel(writer, sheet_name='Combined Details', index=False)

# Example usage
mapping_xml_path = 'mappings.XML'
workflow_xml_path = 'workflows.XML'
mapping_details = parse_mapping_xml(mapping_xml_path)
workflow_details, task_details = parse_workflow_xml(workflow_xml_path)
excel_output_path = 'consolidated_workflow_and_mapping_details.xlsx'
create_combined_excel(excel_output_path, mapping_details, workflow_details, task_details)

print(f"Data has been extracted and saved to {excel_output_path}")
