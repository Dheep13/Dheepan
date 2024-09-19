import gzip

def gunzip_file(input_file, output_file):
    with gzip.open(input_file, 'rb') as gz_file:
        with open(output_file, 'wb') as out_file:
            out_file.write(gz_file.read())

# Example usage:
input_file_path = 'DataExtracts.log.gzip'
output_file_path ='DataExtracts.log'

gunzip_file(input_file_path, output_file_path)
