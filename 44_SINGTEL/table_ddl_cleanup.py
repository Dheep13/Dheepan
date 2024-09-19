import re
import os

def clean_hana_sql(input_file_path, output_file_path):
    with open(input_file_path, 'r') as file:
        sql_text = file.read()

    # Patterns to remove unnecessary lines
    patterns_to_remove = [
        r'--.*',  # Lines starting with -- (comments)
        r'SET SCHEMA.*',  # Lines starting with SET SCHEMA
        r'CALL SQLSCRIPT_PRINT.*',  # Lines starting with CALL SQLSCRIPT_PRINT
        r'CALL sapdbmtk.sp_dbmtk_object_drop.*',  # Lines starting with CALL sapdbmtk.sp_dbmtk_object_drop
        r'CALL sapdbmtk.sp_dbmtk_object_install_custom.*',  # Lines starting with CALL sapdbmtk.sp_dbmtk_object_install_custom
        r'go',  # Lines containing 'go'
        r'GRANT.*',  # Lines starting with GRANT
        r'CREATE INDEX.*',  # Lines starting with CREATE INDEX
        r'\(\s*PARTITION.*',  # Lines containing '( PARTITION'
        r'SUBPARTITION.*',  # Lines containing 'SUBPARTITION'
        r'COMMENT.*',  # Lines starting with 'COMMENT'
        r'^\s*,\s*$',  # Lines that are just commas or have only spaces and commas
        r'^\s*\)\s*PARTITION.*',  # Closing parenthesis followed by PARTITION
        r'^\s*,\s*$',  # Lines that are just commas or have only spaces and commas
        r'^\s*\(\s*SUBPARTITION.*',  # Lines containing '( SUBPARTITION'
        r'\bEXT\.',  # Lines containing schema
        r'\)\s*PARTITION BY LIST.*' # Remove partition definition after closing parenthesis
    ]

    for pattern in patterns_to_remove:
        sql_text = re.sub(pattern, '', sql_text, flags=re.MULTILINE)

    # Add a semicolon at the end of each CREATE COLUMN TABLE statement
    sql_text = re.sub(r'(\))\s*CREATE COLUMN TABLE', r'\1;\n\nCREATE COLUMN TABLE', sql_text)

    # Ensure the last CREATE COLUMN TABLE ends with a semicolon
    sql_text = re.sub(r'(\))\s*$', r'\1;', sql_text)

    # Remove empty lines
    sql_text = os.linesep.join([s for s in sql_text.splitlines() if s.strip()])

    with open(output_file_path, 'w') as file:
        file.write(sql_text)

    print("Cleanup complete. Check the cleaned file.")

# Specify your file paths here
input_file_path ='TABLES/converted_hana_sql_with_comments.txt'
output_file_path ='TABLES/cleaned_hana_sql_VIEWS.sql'

# Execute the cleanup
clean_hana_sql(input_file_path, output_file_path)
