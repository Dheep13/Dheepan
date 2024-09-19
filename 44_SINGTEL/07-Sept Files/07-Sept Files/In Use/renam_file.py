import os

# Get the current working directory (same as where the script is located)
folder_path = os.getcwd()

# Function to rename files in the current directory
def rename_files_in_folder(folder_path):
    # Iterate over all files in the current directory
    for filename in os.listdir(folder_path):
        # Skip directories
        if os.path.isfile(os.path.join(folder_path, filename)):
            # Split the filename by underscores, limit to first 3 parts
            parts = filename.split('_', 2)  # This splits at the first two underscores
            if len(parts) > 2:
                # Concatenate the new filename with "1756_" and the rest of the original filename after the two underscores
                new_filename = f"1756_{parts[0]}{parts[1]}{parts[2]}"
                
                # Rename the file
                old_file_path = os.path.join(folder_path, filename)
                new_file_path = os.path.join(folder_path, new_filename)
                os.rename(old_file_path, new_file_path)
                print(f'Renamed: {filename} -> {new_filename}')

# Call the function
rename_files_in_folder(folder_path)
