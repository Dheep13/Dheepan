import xml.etree.ElementTree as ET
import pandas as pd
import logging
import os

# Setup basic logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def process_xml_to_excel(xml_file, excel_folder):
    logging.info(f'Processing XML file: {xml_file}')
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    # Data storage for DataFrame
    data_rows = []
    
    # Process each mapping in the XML
    for mapping in root.findall('.//MAPPING'):
        mapping_details = mapping.attrib
        mapping_name = mapping_details.get('NAME', 'Unknown')

        # Iterate through each transformation within a mapping
        for trans in mapping.findall('.//TRANSFORMATION'):
            trans_details = {
                'Mapping Name': mapping_name,
                'Transformation Name': trans.get('NAME', 'Unknown'),
                'Type': trans.get('TYPE', 'Unknown'),
                **{attr.tag: attr.text for attr in trans}  # Incorporate any child elements directly under TRANSFORMATION
            }

            # Get all transform fields if present
            transform_fields = []
            for field in trans.findall('.//TRANSFORMFIELD'):
                field_details = {f.attrib['NAME']: f.attrib['VALUE'] for f in field.findall('.//TABLEATTRIBUTE')}
                transform_fields.append(field_details)

            trans_details['Transform Fields'] = transform_fields
            
            # Collect transformation attributes (common approach, adjust as needed)
            for attr in trans.findall('.//TABLEATTRIBUTE'):
                attr_name = attr.get('NAME')
                attr_value = attr.get('VALUE')
                trans_details[attr_name] = attr_value

            # Add to row data
            data_rows.append(trans_details)

        # Integrate data for INSTANCE, CONNECTOR, etc., similarly

    # Create DataFrame
    df = pd.DataFrame(data_rows)

    # Saving DataFrame to Excel
    excel_filename = os.path.join(excel_folder, f'{mapping_name}_details.xlsx')
    df.to_excel(excel_filename, index=False, engine='openpyxl')
    logging.info(f'Excel file generated: {excel_filename}')

def main():
    source_xml = 'path_to_your_xml_file.xml'
    excel_folder = 'path_to_your_excel_folder'
    process_xml_to_excel(source_xml, excel_folder)

if __name__ == '__main__':
    main()
