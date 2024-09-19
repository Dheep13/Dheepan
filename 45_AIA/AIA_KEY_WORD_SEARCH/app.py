import os

# Define the keyword to search for (in lowercase for case-insensitive search)
keyword = "SEQUENCEGENLIB".lower()

# Define the source directory (current working directory)
source_directory = os.getcwd()

# Define the target directory
target_directory = os.path.join(source_directory, 'target_directory')

# Create the target directory if it doesn't exist
if not os.path.exists(target_directory):
    os.makedirs(target_directory)

# Function to search for the keyword in .sql files and save the first 3 lines to a .txt file
def search_and_extract_lines(directory):
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.sql'):
                file_path = os.path.join(root, filename)
                print(f"Checking file: {file_path}")
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        content = file.read().lower()  # Convert content to lowercase for case-insensitive search
                        # Check if the keyword is in the file
                        if keyword in content:
                            print(f"Keyword found in file: {file_path}")
                            # Close the file and reopen to read the first 3 lines
                            file.close()
                            with open(file_path, 'r', encoding='utf-8') as file:
                                lines = [file.readline().strip() for _ in range(3)]
                            # Define the output file path
                            parent_directory = os.path.basename(root)
                            output_file_path = os.path.join(target_directory, f"{parent_directory}_create.txt")
                            # Write the lines to the .txt file
                            with open(output_file_path, 'w', encoding='utf-8') as output_file:
                                output_file.write("\n".join(lines))
                            print(f"Extracted lines written to: {output_file_path}")
                            break  # Exit the loop after processing the file
                except PermissionError as e:
                    print(f"PermissionError for file {file_path}: {e}")

# Run the function on the source directory
search_and_extract_lines(source_directory)

print("Operation completed.")
