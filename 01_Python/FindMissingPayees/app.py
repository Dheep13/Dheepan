import pandas as pd

def find_missing_ids(excel_file, column1_name, column2_name, output_file1, output_file2):
    # Read the Excel file into a DataFrame
    df = pd.read_excel(excel_file)

    # Check if the specified columns exist in the DataFrame
    if column1_name not in df.columns or column2_name not in df.columns:
        print("Specified columns not found in the Excel file.")
        return

    # Convert the columns to sets for efficient comparison
    column1_set = set(df[column1_name].dropna())
    column2_set = set(df[column2_name].dropna())

    # Find the missing IDs
    missing_ids_in_column1 = column2_set - column1_set
    missing_ids_in_column2 = column1_set - column2_set

    # Write the missing IDs to separate output files
    with open(output_file1, 'w') as f1:
        f1.write("\n".join(map(str, missing_ids_in_column1)))

    with open(output_file2, 'w') as f2:
        f2.write("\n".join(map(str, missing_ids_in_column2)))

    print("Missing IDs have been written to:")
    print("File 1:", output_file1)
    print("File 2:", output_file2)

# Example usage
if __name__ == "__main__":
    excel_file_path = "Book2.xlsx"
    column1_name = "personIdExternal"
    column2_name = "Commissions"
    output_file_path1 = "output_file_Commissions.txt"
    output_file_path2 = "output_file_personIdExternal.txt"

    find_missing_ids(excel_file_path, column1_name, column2_name, output_file_path1, output_file_path2)
