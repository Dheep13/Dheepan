import os
import fnmatch

# Folder path where the files are located
base_folder_path = r'C:\Users\DeepanShanmugam\OneDrive - MuniConS GmbH\Documents\Visual Studio Code\44_SINGTEL'

# Function to rename files in final-level folders matching '%Sept Files%'
def rename_files_in_final_sept_folders(base_folder_path):
    # Traverse the base folder and all subdirectories
    for root, dirs, files in os.walk(base_folder_path):
        # Check if the folder name contains 'Sept Files' and has no subdirectories (final folder)
        if fnmatch.fnmatch(os.path.basename(root), '*Sept Files*') and not dirs:
            # Iterate over all files in this folder
            for filename in files:
                # Skip directories
                if os.path.isfile(os.path.join(root, filename)):
                    # Split the filename by underscores, limit to first 3 parts
                    parts = filename.split('_', 2)  # This splits at the first two underscores
                    if len(parts) > 2:
                        # Concatenate the new filename with "1756_" and the rest of the original filename after the two underscores
                        new_filename = f"1756_{parts[0]}{parts[1]}{parts[2]}"
                        
                        # Rename the file
                        old_file_path = os.path.join(root, filename)
                        new_file_path = os.path.join(root, new_filename)
                        os.rename(old_file_path, new_file_path)
                        print(f'Renamed: {filename} -> {new_filename}')

# Call the function
rename_files_in_final_sept_folders(base_folder_path)
