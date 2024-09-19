import os

def list_filenames_in_current_directory():
    try:
        # Get the current working directory
        current_directory = os.getcwd()
        
        # Get a list of all files in the current directory
        filenames = os.listdir(current_directory)
        
        # Filter the list to only include files (not directories)
        filenames = [f for f in filenames if os.path.isfile(os.path.join(current_directory, f))]
        
        return filenames
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

# Example usage
filenames = list_filenames_in_current_directory()

for filename in filenames:
    print(filename)
