import xml.etree.ElementTree as ET

# Load your XML data
xml_file = 'workflow_xml.xml'  # Update this with the correct file path
tree = ET.parse(xml_file)
root = tree.getroot()

# Dictionary to hold the sequence
sequence = {}

# Extract task instances for reference
task_names = {task.get('NAME'): task.get('TASKNAME') for task in root.findall('.//TASKINSTANCE')}

# Extract workflow links to determine the sequence
for link in root.findall('.//WORKFLOWLINK'):
    from_task = link.get('FROMTASK')
    to_task = link.get('TOTASK')
    sequence[task_names[from_task]] = task_names[to_task]

# Function to print the sequence starting from 'Start'
def print_sequence(task, sequence, level=0):
    print('  ' * level + task)
    if task in sequence:
        print_sequence(sequence[task], sequence, level + 1)

# Assuming 'Start' is the initial task
print_sequence('Start', sequence)
