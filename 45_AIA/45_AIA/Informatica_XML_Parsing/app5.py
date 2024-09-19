import os
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import pandas as pd
import logging

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

def process_xml_to_excel(xml_file, all_data):
    logging.info(f'Processing XML file: {xml_file}')
    tree = ET.parse(xml_file)
    root = tree.getroot()

    mapping_name = root.attrib.get('NAME', os.path.basename(xml_file).rsplit('.', 1)[0])
    transformations_data = []

    for trans in root.findall('.//TRANSFORMATION'):
        trans_type = trans.attrib.get('TYPE')
        trans_name = trans.attrib.get('NAME')
        transform_fields = []
        if trans_type == "Expression":
            for field in trans.findall('.//TRANSFORMFIELD'):
                field_details = {key: field.attrib.get(key, '') for key in field.attrib}
                field_details_str = ', '.join(f"{key}: {value}" for key, value in field_details.items())
                transform_fields.append(field_details_str)
        transform_fields_str = "; ".join(transform_fields)

        for table_attr in trans.findall('.//TABLEATTRIBUTE'):
            transformations_data.append({
                'Mapping Name': mapping_name,
                'Transformation Name': trans_name,
                'Type': trans_type,
                'Table Attribute Name': table_attr.attrib['NAME'],
                'Table Attribute Value': table_attr.attrib['VALUE'],
                'Transform Fields': transform_fields_str
            })

    # Processing <INSTANCE> elements with TYPE="TARGET" and their TABLEATTRIBUTE elements
    for instance in root.findall('.//INSTANCE[@TYPE="TARGET"]'):
        instance_name = instance.attrib.get('TRANSFORMATION_NAME', 'Unnamed')
        table_attributes = instance.findall('.//TABLEATTRIBUTE')
        if table_attributes:
            for attr in table_attributes:
                transformations_data.append({
                    'Mapping Name': mapping_name,
                    'Transformation Name': instance_name,
                    'Type': instance.attrib['TYPE'],
                    'Table Attribute Name': attr.attrib.get('NAME', 'N/A'),
                    'Table Attribute Value': attr.attrib.get('VALUE', 'N/A'),
                    'Transform Fields': 'N/A'
                })
        else:
            # Include this if no TABLEATTRIBUTE found
            transformations_data.append({
                'Mapping Name': mapping_name,
                'Transformation Name': instance_name,
                'Type': instance.attrib['TYPE'],
                'Table Attribute Name': 'N/A',
                'Table Attribute Value': 'N/A',
                'Transform Fields': 'N/A'
            })

    df = pd.DataFrame(transformations_data)
    all_data.extend(transformations_data)
    excel_filename = os.path.join(EXCEL_FOLDER, f'{mapping_name}_attributes.xlsx')
    df.to_excel(excel_filename, index=False, engine='openpyxl')
    logging.info(f'Excel file generated: {excel_filename}')


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

    all_data = []
    for root, dirs, files in os.walk(UPLOAD_FOLDER):
        for file in files:
            if file.lower().endswith('.xml'):
                xml_path = os.path.join(root, file)
                process_xml_to_excel(xml_path, all_data)

    if all_data:
        df_all = pd.DataFrame(all_data)
        consolidated_excel_path = os.path.join(EXCEL_FOLDER, CONSOLIDATED_EXCEL_NAME)
        df_all.to_excel(consolidated_excel_path, index=False, engine='openpyxl')
        logging.info(f'Consolidated Excel file generated: {consolidated_excel_path}')

    final_zip_path = os.path.join(CURRENT_DIR, ZIP_NAME)
    zip_excel_files(final_zip_path)
    logging.info('All done. Excel files ZIP: ' + final_zip_path)

if __name__ == '__main__':
    main()
