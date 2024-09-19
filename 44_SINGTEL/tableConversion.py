import re
import os

def clean_sql_oracle_to_hana(sql_text):
    table_names = []  # List to store table names
    drop_statements = []  # List to store DROP TABLE statements

    def clean_create_statement(statement):
        # Remove specifics like segment creation and tablespace
        statement = re.sub(r'SEGMENT CREATION IMMEDIATE', '', statement)
        statement = re.sub(r'TABLESPACE "TALLYDATA"', '', statement)
        statement = re.sub(r'^\s*GRANT.*?\n', '', statement, flags=re.MULTILINE)
        statement = re.sub(r'"', '', statement)
        statement = re.sub(r'AIASEXT\.', 'EXT.', statement)
        statement = re.sub(r'\bVARCHAR2\((\d+)\s+BYTE\)', r'NVARCHAR(\1)', statement)
        statement = re.sub(r'\bVARCHAR2\((\d+)\)', r'NVARCHAR(\1)', statement)
        statement = re.sub(r'\bNUMBER\((\d+),(\d+)\)', r'DECIMAL(\1,\2)', statement)
        statement = re.sub(r'\bNUMBER\(\*,0\)', 'BIGINT', statement)
        statement = re.sub(r'\bNUMBER\(\*,10\)', 'BIGINT', statement) # Additional pattern to catch
        statement = re.sub(r'\bNUMBER\b', 'DECIMAL', statement)  # General case for NUMBER without specifics
        statement = re.sub(r'PCTFREE \d+|PCTUSED \d+|INITRANS \d+|MAXTRANS \d+|NOCOMPRESS|LOGGING|'
                           r'STORAGE\(.*?\)|BUFFER_POOL DEFAULT|FLASH_CACHE DEFAULT|CELL_FLASH_CACHE DEFAULT', '', statement, flags=re.DOTALL)
        statement = re.sub(r'\)\s*([^()]*?);', r');', statement)  # Simplify end of create statement
        statement = re.sub(r'^(?:\s*,|PARTITION|SUBPARTITION).*', '', statement, flags=re.MULTILINE)
        statement = re.sub(r'^\s*COMMENT.*', '', statement, flags=re.MULTILINE)
        statement = re.sub(r'^\s*\,\s*$', '', statement, flags=re.MULTILINE)  # Remove lines that are just commas

        match = re.search(r'CREATE\s+TABLE\s+(\S+)', statement)
        if match:
            table_name = match.group(1).replace('EXT.', '')
            table_names.append(table_name)
            drop_statements.append(f'DROP TABLE EXT.{table_name};')

        return statement

    parts = re.split(r'(?i)(CREATE\s+TABLE)', sql_text)
    cleaned_sql = parts[0]
    for i in range(1, len(parts), 2):
        cleaned_sql += 'CREATE TABLE' + clean_create_statement(parts[i] + parts[i+1])

    return cleaned_sql, table_names, drop_statements

def convert_sql_file(input_file_path, output_file_path, table_names_file, drop_statements_file):
    with open(input_file_path, 'r') as file:
        oracle_sql = file.read()
    hana_sql, table_names, drop_statements = clean_sql_oracle_to_hana(oracle_sql)
    safe_open_write(output_file_path, hana_sql)
    safe_open_write(table_names_file, '\n'.join(table_names))
    safe_open_write(drop_statements_file, '\n'.join(drop_statements))
    print("Conversion complete. Check the converted and drop statements files.")

def safe_open_write(path, data):
    if os.path.exists(path):
        os.remove(path)
    with open(path, 'w') as file:
        file.write(data)

input_file_path = 'TABLES/sample_sql.txt'
output_file_path = 'TABLES/converted_to_hana.sql'
table_names_file = 'TABLES/table_names.txt'
drop_statements_file = 'TABLES/drop_statements.sql'

convert_sql_file(input_file_path, output_file_path, table_names_file, drop_statements_file)
