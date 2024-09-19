def split_file_by_location(input_file):
    data_by_location = {}

    # Read the pipe-delimited file
    with open(input_file, 'r') as file:
        lines = file.readlines()

    # Process each line in the file
    for line in lines[1:]:  # Skip the header row
        location, *rest = line.strip().split('|')
        if location not in data_by_location:
            data_by_location[location] = []
        data_by_location[location].append('|'.join([location] + rest))

    # Write data to separate files
    for location, data in data_by_location.items():
        output_file = f"{location}_data.txt"
        with open(output_file, 'w') as file:
            file.write(lines[0])  # Write the header row
            file.write('\n'.join(data))

if __name__ == "__main__":
    input_file = "ONTIME_DEV_2023072023.txt"
    split_file_by_location(input_file)
