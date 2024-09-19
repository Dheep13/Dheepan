import pandas as pd

# Load the CSV file
file_path = 'filenaming.csv'
df = pd.read_csv(file_path)

# Function to generate the CREATE PROCEDURE statement
def generate_procedure(file_type, target):
    return f"""
CREATE PROCEDURE EXT.SP_XDL_INB_{file_type.upper()} (IN FILENAME varchar(120))
LANGUAGE SQLSCRIPT
AS
BEGIN

DECLARE v_FILE_TYPE NVARCHAR(500) := '{file_type}';
DECLARE v_src_tbl varchar(200) :='{target}';
DECLARE v_file_date TIMESTAMP :=CURRENT_TIMESTAMP;
DECLARE v_file_name varchar(500):= :FILENAME;
    
SET SESSION 'v_FILE_TYPE' = :v_FILE_TYPE;
SET SESSION 'v_file_date' = :v_file_date;
SET SESSION 'v_file_name' = :v_file_name;
SET SESSION 'v_src_tbl' = :v_src_tbl;

CALL EXT.STEL_INITIAL_DYNAMIC_LOAD();
-- CALL EXT.INBOUND_TRIGGER();

END;
"""

# Generate the procedures
procedures = []
for index, row in df.iterrows():
    procedures.append(generate_procedure(row["New File type in XDL"], row["Target"]))

# Output the generated procedures
procedures_output = "\n".join(procedures)

# Write the output to a file
output_file_path = 'generated_procedures.sql'
with open(output_file_path, 'w') as file:
    file.write(procedures_output)

print(f"Procedures have been written to {output_file_path}")
