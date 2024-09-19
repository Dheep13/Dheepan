import os
import argparse

# Function to read filenames from a directory and write them to a text file
def list_filenames_to_txt(source_directory, output_file):
    # List all files in the source directory
    filenames = []
    for root, dirs, files in os.walk(source_directory):
        for filename in files:
            filenames.append(filename)
            print(f"Found file: {filename}")

    # Write filenames to the output file
    with open(output_file, 'w', encoding='utf-8') as file:
        for filename in filenames:
            file.write(f"{filename}\n")
    
    print(f"Filenames have been written to {output_file}")

# Main function to parse arguments and run the script
def main():
    parser = argparse.ArgumentParser(description="List filenames in a directory and save to a text file")
    parser.add_argument('source_directory', type=str, help="Source directory to list filenames from")
    parser.add_argument('output_file', type=str, help="Output text file to save filenames")
    args = parser.parse_args()

    print(f"Source directory: {args.source_directory}")
    print(f"Output file: {args.output_file}")

    list_filenames_to_txt(args.source_directory, args.output_file)

if __name__ == '__main__':
    main()
