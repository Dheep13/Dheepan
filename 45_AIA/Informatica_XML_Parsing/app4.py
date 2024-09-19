import os
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import pandas as pd
import logging
import shutil

# Setup basic logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_FOLDER = os.path.join(CURRENT_DIR, 'source')
UPLOAD_FOLDER = os.path.join(CURRENT_DIR, 'uploads')
EXCEL_FOLDER = os.path.join(CURRENT_DIR, 'excel_files')
ZIP_NAME = 'processed_excel_files.zip'
CONSOLIDATED_EXCEL_NAME = 'consolidated_mappings_attributes.xlsx'

# Ensure directories exist
os.makedirs(SOURCE_FOLDER, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXCEL_FOLDER, exist_ok=True)

def find_zip_file(source_folder):
    for file in os.listdir(source_folder):
        if file.endswith('.zip'):
            logging.info(f'ZIP file found: {file}')
            return os.path.join(source_folder, file)
    logging.error('No ZIP file found in the source folder.')
    return None

def split_large_xml(xml_file, output_folder):
    logging.info(f'Splitting large XML file: {xml_file}')
    tree = ET.parse(xml_file)
    root = tree.getroot()

    for index, mapping in enumerate(root.findall('.//MAPPING'), start=1):
        mapping_name = mapping.attrib.get('NAME', f'mapping_{index}')  # Default if NAME is missing
        sanitized_name = ''.join(char for char in mapping_name if char.isalnum() or char in (' ', '_')).rstrip()
        small_tree = ET.ElementTree(mapping)
        output_path = os.path.join(output_folder, f'{sanitized_name}.xml')
        small_tree.write(output_path)
        logging.info(f'Generated small XML file: {output_path}')

def process_xml_to_excel(xml_file, all_data):
    logging.info(f'Processing XML file: {xml_file}')
    tree = ET.parse(xml_file)
    root = tree.getroot()

    mapping_name = root.attrib.get('NAME', os.path.basename(xml_file).rsplit('.', 1)[0])  # Default if NAME is missing
    transformations_data = []

    for trans in root.findall('.//TRANSFORMATION'):
        for table_attr in trans.findall('.//TABLEATTRIBUTE'):
            attr_name = table_attr.attrib.get('NAME')
            attr_value = table_attr.attrib.get('VALUE')
            transformations_data.append({
                'Mapping Name': mapping_name,
                'Transformation Name': trans.attrib.get('NAME'),
                'Type': trans.attrib.get('TYPE'),
                'Table Attribute Name': attr_name,
                'Table Attribute Value': attr_value
            })

    # Integrate INSTANCE data
    for instance in root.findall('.//INSTANCE'):
        instance_type = instance.attrib.get('TYPE')
        instance_name = instance.attrib.get('NAME')
        transformations_data.append({
            'Mapping Name': mapping_name,
            'Transformation Name': 'N/A',  # Not applicable here
            'Type': instance_type,
            'Table Attribute Name': instance_name,
            'Table Attribute Value': 'N/A'  # Not applicable here
        })

    df = pd.DataFrame(transformations_data)
    all_data.extend(transformations_data)  # Append to all data for consolidated file

    excel_filename = os.path.join(EXCEL_FOLDER, f'{mapping_name}_attributes.xlsx')
    df.to_excel(excel_filename, index=False, engine='openpyxl')
    logging.info(f'Excel file generated: {excel_filename}')
    return os.path.basename(excel_filename)

def zip_excel_files(zip_name):
    logging.info('Zipping Excel files...')
    excel_files = os.listdir(EXCEL_FOLDER)
    if not excel_files:
        logging.error('No Excel files found to zip. Check previous steps.')
        return

    with ZipFile(zip_name, 'w') as zipf:
        for file in excel_files:
            file_path = os.path.join(EXCEL_FOLDER, file)
            logging.info(f'Adding {file} to ZIP.')
            zipf.write(file_path, arcname=file)
    logging.info(f'ZIP file created at: {zip_name}')

def main():
    zip_file_path = find_zip_file(SOURCE_FOLDER)
    if not zip_file_path:
        return

    with ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(UPLOAD_FOLDER)
    logging.info(f'Files extracted to {UPLOAD_FOLDER}: {os.listdir(UPLOAD_FOLDER)}')

    large_xml_file = os.path.join(UPLOAD_FOLDER, 'mappings.XML')
    split_large_xml(large_xml_file, UPLOAD_FOLDER)

    all_data = []  # List to hold data for the consolidated Excel file
    for root, dirs, files in os.walk(UPLOAD_FOLDER):
        for file in files:
            if file.lower().endswith('.xml') and file.lower() != 'mappings.xml':
                xml_path = os.path.join(root, file)
                process_xml_to_excel(xml_path, all_data)

    # Generate the consolidated Excel file
    if all_data:
        df_all = pd.DataFrame(all_data)
        consolidated_excel_path = os.path.join(EXCEL_FOLDER, CONSOLIDATED_EXCEL_NAME)
        df_all.to_excel(consolidated_excel_path, index=False, engine='openpyxl')
        logging.info(f'Consolidated Excel file generated: {consolidated_excel_path}')

    final_zip_path = os.path.join(CURRENT_DIR, ZIP_NAME)
    zip_excel_files(final_zip_path)

    logging.info(f'All done. Excel files ZIP: {final_zip_path}')

if __name__ == '__main__':
    main()
