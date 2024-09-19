import xml.etree.ElementTree as ET
import pandas as pd
import openai
import os
import re

# OpenAI API key
# openai.api_key = 'your-api-key'  # Replace with your actual OpenAI API key
openai.api_key = 'sk-_-6Tjb40f0tPnB1c2_ejhi3b5pHY0bUth6b8wIxUjlT3BlbkFJ8HX2RgSo4zOegefO39Y6yll936-Q744g0irMmPJt0A'


# Function to extract mapping details from the XML
def parse_mapping(xml_content):
    root = ET.fromstring(xml_content)
    mapping_name = root.attrib.get('NAME', 'Unknown Mapping')
    
    # Extract transformation details
    transformations = []
    for trans in root.findall('.//TRANSFORMATION'):
        trans_name = trans.attrib['NAME']
        trans_type = trans.attrib['TYPE']
        trans_fields = [f.attrib for f in trans.findall('.//TRANSFORMFIELD')]
        transformations.append({
            'name': trans_name,
            'type': trans_type,
            'fields': trans_fields
        })
    
    # Extract instance details
    instances = []
    for instance in root.findall('.//INSTANCE'):
        instance_name = instance.attrib['NAME']
        trans_name = instance.attrib['TRANSFORMATION_NAME']
        trans_type = instance.attrib['TRANSFORMATION_TYPE']
        instances.append({
            'name': instance_name,
            'trans_name': trans_name,
            'trans_type': trans_type
        })
    
    # Extract connector details
    connectors = []
    for conn in root.findall('.//CONNECTOR'):
        from_instance = conn.attrib['FROMINSTANCE']
        to_instance = conn.attrib['TOINSTANCE']
        connectors.append({
            'from_instance': from_instance,
            'to_instance': to_instance
        })
    
    return mapping_name, transformations, instances, connectors

# Function to clean the GPT-4-generated summary by removing unwanted characters
def clean_summary(summary):
    # Remove any non-printable characters and problematic characters
    clean_text = re.sub(r'[^\x00-\x7F]+', '', summary)  # Removes non-ASCII characters
    clean_text = clean_text.replace('**', '')
    return clean_text

# Function to use GPT-4 to summarize the mapping
def generate_llm_summary(mapping_name, transformations, instances, connectors):
    # Construct a detailed message for GPT-4 to process
    prompt = f"Summarize the following mapping '{mapping_name}' in terms of data flow from source to target:\n\n"
    
    prompt += "Transformations:\n"
    for trans in transformations:
        prompt += f" - {trans['name']} ({trans['type']})\n"
    
    prompt += "\nInstances:\n"
    for instance in instances:
        prompt += f" - {instance['name']} ({instance['trans_type']})\n"
    
    prompt += "\nConnectors:\n"
    for conn in connectors:
        prompt += f" - Data flows from '{conn['from_instance']}' to '{conn['to_instance']}'\n"
    
    prompt += "\nPlease summarize this data flow."

    # Use GPT-4 to generate the summary
    response = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that summarizes data mappings"},
            {"role": "user", "content": prompt}
        ]
    )

    # Extract the summary text from GPT-4's response
    summary = response.choices[0].message.content
        # Clean the summary to remove unwanted characters
    cleaned_summary = clean_summary(summary)

    return cleaned_summary

# Function to write the GPT-generated summary to an Excel file
def write_summary_to_excel(mapping_name, summary, excel_file):
    # Create a Pandas DataFrame to store the summary
    df_summary = pd.DataFrame([summary], columns=['LLM-Generated Summary'])
    
    # Check if the Excel file exists, if not, create it
    if not os.path.exists(excel_file):
        # If the file doesn't exist, create a new Excel file
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            df_summary.to_excel(writer, sheet_name=f'Summary', index=False)
        print(f"Created new Excel file: {excel_file} and wrote summary.")
    else:
        # If the file exists, open it in append mode and add the new sheet
        with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a') as writer:
            df_summary.to_excel(writer, sheet_name=f'Summary', index=False)
        print(f"Appended summary to existing Excel file: {excel_file}")

# Main function to parse XML, generate the summary, and write it to Excel
def main():
    # Example XML content (replace this with your actual XML data)
    xml_content = '''<MAPPING DESCRIPTION ="" ISVALID ="YES" NAME ="m_RPT_Populate_Data_PAQPB" OBJECTVERSION ="1" VERSIONNUMBER ="1">
        <TRANSFORMATION DESCRIPTION ="" NAME ="EXPTRANS" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Expression" VERSIONNUMBER ="1">
            <TRANSFORMFIELD DATATYPE ="string" NAME ="DUMMY" PORTTYPE ="INPUT" PRECISION ="1" SCALE ="0"/>
            <TRANSFORMFIELD DATATYPE ="string" EXPRESSION =":SP.RPT_COMMON_PKG()" NAME ="OUT_DUMMY" PORTTYPE ="OUTPUT" PRECISION ="1" SCALE ="0"/>
        </TRANSFORMATION>
        <TRANSFORMATION DESCRIPTION ="" NAME ="SQ_DUAL" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
            <TRANSFORMFIELD DATATYPE ="string" NAME ="DUMMY" PORTTYPE ="INPUT/OUTPUT" PRECISION ="1" SCALE ="0"/>
        </TRANSFORMATION>
        <TRANSFORMATION DESCRIPTION ="" NAME ="RPT_COMMON_PKG" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Stored Procedure" VERSIONNUMBER ="1">
            <TABLEATTRIBUTE NAME ="Stored Procedure Name" VALUE ="RPT_COMMON_PKG.GENERATE_MASTER_AGENT"/>
        </TRANSFORMATION>
        <INSTANCE NAME ="DUAL1" TRANSFORMATION_NAME ="DUAL" TRANSFORMATION_TYPE ="Source Definition" TYPE ="SOURCE"/>
        <INSTANCE NAME ="EXPTRANS" TRANSFORMATION_NAME ="EXPTRANS" TRANSFORMATION_TYPE ="Expression" TYPE ="TRANSFORMATION"/>
        <CONNECTOR FROMFIELD ="DUMMY" FROMINSTANCE ="DUAL1" TOFIELD ="DUMMY" TOINSTANCE ="SQ_DUAL"/>
        <CONNECTOR FROMFIELD ="DUMMY" FROMINSTANCE ="SQ_DUAL" TOFIELD ="DUMMY" TOINSTANCE ="EXPTRANS"/>
    </MAPPING>'''

    # Parse the mapping details
    mapping_name, transformations, instances, connectors = parse_mapping(xml_content)
    
    # Generate the summary using GPT-4
    summary = generate_llm_summary(mapping_name, transformations, instances, connectors)
    
    # Write the summary to an Excel file
    excel_file = 'mapping_summary.xlsx'  # Adjust file name/path as needed
    write_summary_to_excel(mapping_name, summary, excel_file)
    
    print(f"Summary for '{mapping_name}' written to {excel_file}")

if __name__ == '__main__':
    main()
