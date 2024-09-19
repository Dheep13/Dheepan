
import os
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import pandas as pd
import logging
import openai

# Setup basic logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_FOLDER = os.path.join(CURRENT_DIR, 'source')
UPLOAD_FOLDER = os.path.join(CURRENT_DIR, 'uploads')
EXCEL_FOLDER = os.path.join(CURRENT_DIR, 'excel_files')
ZIP_NAME = 'processed_excel_files.zip'
CONSOLIDATED_EXCEL_PATH = os.path.join(EXCEL_FOLDER, 'consolidated_workflow_task_details.xlsx')

# Ensure directories exist
os.makedirs(SOURCE_FOLDER, exist_ok=True)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXCEL_FOLDER, exist_ok=True)

# Set your OpenAI API key
# openai.api_key = 'your-api-key'  # Make sure to replace this with your actual API key
openai.api_key = 'sk-_-6Tjb40f0tPnB1c2_ejhi3b5pHY0bUth6b8wIxUjlT3BlbkFJ8HX2RgSo4zOegefO39Y6yll936-Q744g0irMmPJt0A'

def ensure_excel_file_exists(excel_file):
    if not os.path.exists(excel_file):
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            pd.DataFrame().to_excel(writer)
        logging.info(f"Created a new Excel file: {excel_file}")

def find_zip_file(source_folder):
    for file in os.listdir(source_folder):
        if file.endswith('.zip'):
            logging.info(f'ZIP file found: {file}')
            return os.path.join(source_folder, file)
    logging.error('No ZIP file found in the source folder.')
    return None

def generate_llm_summary(mapping_name, transformation_details):
    # Construct messages in chat format
    messages = [
        {"role": "system", "content": "You are a helpful assistant who summarizes Informatica mappings."},
        {"role": "user", "content": f"Summarize the following mapping '{mapping_name}' in terms of data flow from source to target:\n\n" + "\n".join(transformation_details)}
    ]

    # Call the Chat Completions API
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",  # Use the appropriate model name
            messages=messages
        )
        # Extract the assistant's message from the response
        summary = response['choices'][0]['message']['content']
        return summary
    except Exception as e:
        logging.error(f"Error generating summary for {mapping_name}: {e}")
        return "Summary generation failed."

def process_xml_to_excel(xml_file, all_data, excel_writer):
    logging.info(f'Processing XML file: {xml_file}')
    tree = ET.parse(xml_file)
    root = tree.getroot()

    mapping_name = root.attrib.get('NAME', os.path.basename(xml_file).rsplit('.', 1)[0])
    transformations_data = []
    transformation_details = []

    transformations = root.findall('.//TRANSFORMATION')

    for trans in transformations:
        trans_type = trans.attrib.get('TYPE')
        trans_name = trans.attrib.get('NAME')
        transformation_details.append(f"Transformation '{trans_name}' of type '{trans_type}'")

        for table_attr in trans.findall('.//TABLEATTRIBUTE'):
            transformations_data.append({
                'Mapping Name': mapping_name,
                'Transformation Name': trans_name,
                'Type': trans_type,
                'Table Attribute Name': table_attr.attrib['NAME'],
                'Table Attribute Value': table_attr.attrib['VALUE']
            })

    # Call the LLM to generate a summary based on the transformations
    llm_summary = generate_llm_summary(mapping_name, transformation_details)

    # Write transformation data and LLM-generated summary to Excel
    df = pd.DataFrame(transformations_data)
    all_data.extend(transformations_data)

    # Write mapping details to individual sheet
    df.to_excel(excel_writer, sheet_name=f'{mapping_name}_attributes', index=False)

    # Write the LLM-generated summary to a separate sheet
    summary_df = pd.DataFrame([llm_summary], columns=['LLM-Generated Summary'])
    summary_df.to_excel(excel_writer, sheet_name=f'{mapping_name}_summary', index=False)

    logging.info(f'Summary for {mapping_name} generated and written to Excel.')

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
    # Ensure the consolidated Excel file exists
    ensure_excel_file_exists(CONSOLIDATED_EXCEL_PATH)

    zip_file_path = find_zip_file(SOURCE_FOLDER)
    if not zip_file_path:
        return
    with ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(UPLOAD_FOLDER)
    logging.info(f'Files extracted to {UPLOAD_FOLDER}: {os.listdir(UPLOAD_FOLDER)}')

    all_data = []
    with pd.ExcelWriter(CONSOLIDATED_EXCEL_PATH, engine='openpyxl', mode='a') as writer:
        for root, dirs, files in os.walk(UPLOAD_FOLDER):
            for file in files:
                if file.lower().endswith('.xml'):
                    xml_path = os.path.join(root, file)
                    process_xml_to_excel(xml_path, all_data, writer)

    logging.info(f'All XML files processed. Excel file saved: {CONSOLIDATED_EXCEL_PATH}')

    final_zip_path = os.path.join(CURRENT_DIR, ZIP_NAME)
    zip_excel_files(final_zip_path)
    logging.info('All done. Excel files ZIP: ' + final_zip_path)

if __name__ == '__main__':
    main()
