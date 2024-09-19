import xml.etree.ElementTree as ET
import pandas as pd

def extract_task_instance_details(xml_path, excel_writer):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Task instance details
    task_instances_data = []
    for task_instance in root.findall('.//TASKINSTANCE'):
        task_data = {
            'Name': task_instance.get('NAME'),
            'TaskName': task_instance.get('TASKNAME'),
            'TaskType': task_instance.get('TASKTYPE'),
            'IsEnabled': task_instance.get('ISENABLED'),
            'Reusable': task_instance.get('REUSABLE'),
        }
        task_instances_data.append(task_data)

    # Convert to DataFrame
    task_instance_df = pd.DataFrame(task_instances_data)

    # Write to Excel
    task_instance_df.to_excel(excel_writer, sheet_name='Task Instance Details', index=False)

# Example usage
excel_writer = pd.ExcelWriter('consolidated_workflow_task_details.xlsx', engine='openpyxl', mode='a')
extract_task_instance_details('workflows.XML', excel_writer)
excel_writer._save()
