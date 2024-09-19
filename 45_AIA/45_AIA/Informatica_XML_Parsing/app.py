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

def process_xml_to_excel(xml_file):
    logging.info(f'Processing XML file: {xml_file}')
    tree = ET.parse(xml_file)
    root = tree.getroot()

    mapping_name = root.find('.//MAPPING').attrib.get('NAME') if root.find('.//MAPPING') is not None else "No Mapping Name Found"

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

    df = pd.DataFrame(transformations_data)
    excel_filename = os.path.join(EXCEL_FOLDER, os.path.basename(xml_file).rsplit('.', 1)[0] + '_attributes.xlsx')
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

    for root, dirs, files in os.walk(UPLOAD_FOLDER):
        for file in files:
            if file.lower().endswith('.xml'):
                print('Inside looping through upload folder')
                xml_path = os.path.join(root, file)
                process_xml_to_excel(xml_path)

    final_zip_path = os.path.join(CURRENT_DIR, ZIP_NAME)
    zip_excel_files(final_zip_path)

    # Optional: Cleanup
    # shutil.rmtree(UPLOAD_FOLDER, ignore_errors=True)
    # shutil.rmtree(EXCEL_FOLDER, ignore_errors=True)
    # os.makedirs(EXCEL_FOLDER, exist_ok=True)

    logging.info(f'All done. Excel files ZIP: {final_zip_path}')

if __name__ == '__main__':
    main()
