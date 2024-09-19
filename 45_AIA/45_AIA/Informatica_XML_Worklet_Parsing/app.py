import xml.etree.ElementTree as ET
import pandas as pd

def parse_and_combine(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    # List to hold all combined data
    combined_data = []

    # Iterate over worklets
    for worklet in root.findall('.//WORKLET'):
        worklet_name = worklet.attrib.get('NAME', 'Unnamed')

        # Gather tasks to create detailed transition flows later
        tasks = {task.get('NAME'): task for task in worklet.findall('.//TASKINSTANCE')}
        
        # Tasks
        for task in worklet.findall('.//TASK'):
            combined_data.append({
                'Type': 'Task',
                'Worklet Name': worklet_name,
                'Name': task.attrib.get('NAME'),
                'Task Type': task.attrib.get('TYPE'),
                'Version': task.attrib.get('VERSIONNUMBER', ''),
                'Description': task.attrib.get('DESCRIPTION', '')
            })
        
        # Session Attributes
        for session in worklet.findall('.//SESSION'):
            combined_data.append({
                'Type': 'Session',
                'Worklet Name': worklet_name,
                'Name': session.attrib.get('NAME'),
                'Mapping Name': session.attrib.get('MAPPINGNAME', ''),
                'Version': session.attrib.get('VERSIONNUMBER', ''),
                'Description': session.attrib.get('DESCRIPTION', '')
            })
        
        # Workflow Variables
        for var in worklet.findall('.//WORKFLOWVARIABLE'):
            combined_data.append({
                'Type': 'Workflow Variable',
                'Worklet Name': worklet_name,
                'Name': var.attrib.get('NAME'),
                'Data Type': var.attrib.get('DATATYPE', ''),
                'Default Value': var.attrib.get('DEFAULTVALUE', ''),
                'Description': var.attrib.get('DESCRIPTION', '')
            })

        # Workflow Links
        for link in worklet.findall('.//WORKFLOWLINK'):
            from_task = link.get('FROMTASK')
            to_task = link.get('TOTASK')
            condition = link.get('CONDITION')

            combined_data.append({
                'Type': 'Workflow Link',
                'Worklet Name': worklet_name,
                'From Task': from_task,
                'To Task': to_task,
                'Condition': condition or "None",
                'From Task Details': f"Type: {tasks.get(from_task, {}).get('TASKTYPE', 'Unknown')}",
                'To Task Details': f"Type: {tasks.get(to_task, {}).get('TASKTYPE', 'Unknown')}"
            })
        
        # Additional sections can be added similarly

    # Create DataFrame from combined data
    df = pd.DataFrame(combined_data)
    
    # Save to Excel
    excel_path = 'consolidated_worklet_details.xlsx'
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Consolidated Details', index=False)
    
    return excel_path

# Usage example
xml_file_path = 'worklets.XML'  # Adjust this path as needed
output_excel = parse_and_combine(xml_file_path)
print(f"Details extracted and saved to {output_excel}")
