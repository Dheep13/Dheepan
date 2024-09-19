import os
import gzip
import shutil

def gzip_files_in_directory(directory_path):
    # List all files in the directory
    os.chdir(directory_path)

    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        # Check if it's a file (not a directory)
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as f_in:
                with gzip.open(file_path + '.gz', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f"Gzipped: {filename}")


directory = r"C:\Users\I352471\Desktop\Singtel Files\29 30 31st Files\29th Files"

# Replace 'your_directory_path' with the actual directory path
gzip_files_in_directory(directory)
