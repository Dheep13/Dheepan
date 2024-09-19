import re
from xml.dom.minidom import parseString

def format_xml(xml_string):
    """
    Formats an XML string for readability using minidom and removes blank lines.
    
    Parameters:
    - xml_string (str): The XML string to format.
    
    Returns:
    - str: A formatted XML string with no blank lines.
    """
    pretty_xml = parseString(xml_string).toprettyxml()
    # Remove blank lines
    lines = pretty_xml.split('\n')
    non_empty_lines = [line for line in lines if line.strip() != '']
    return '\n'.join(non_empty_lines)

def split_and_save_xml(file_path):
    """
    Splits an XML file into multiple files based on <RULE_SET> tags, each with a custom header and footer,
    and saves them to individual files.
    
    Parameters:
    - file_path (str): Path to the original XML file.
    """
    # Read the content from the original XML file
    with open(file_path, 'r', encoding='UTF-8') as file:
        content = file.read()
    
    # Find all <RULE_SET>...</RULE_SET> blocks
    rule_sets = re.findall(r'<RULE_SET>.*?</RULE_SET>', content, re.DOTALL)

    # Process each rule set
    for index, rule_set in enumerate(rule_sets, start=1):
        # Prepare the content with the XML declaration and DATA_IMPORT tags
        formatted_xml = f'<?xml version="1.0" encoding="UTF-8"?>\n<DATA_IMPORT LOCALE="en_GB" VERSION="46.0">\n{rule_set}\n</DATA_IMPORT>'
        formatted_xml = format_xml(formatted_xml)  # Format for readability
        
        # Define the new file path
        new_file_path = f'Split_Rule_{index}.xml'
        
        # Write the formatted content to a new XML file
        with open(new_file_path, 'w', encoding='UTF-8') as new_file:
            new_file.write(formatted_xml)

    print(f"Split and saved {index} files.")

if __name__ == "__main__":
    # Specify the path to your original XML file here
    file_path = 'Rule.xml'
    split_and_save_xml(file_path)
