input_file = '1951_TXSTA_DEV_20230517_130332_SCDK002004.csv'
output_file = '1951_TXSTA_DEV_20230517_130332_SCDK002004.txt'

with open(input_file, 'r') as file_in, open(output_file, 'w') as file_out:
    for line in file_in:
        new_line = line.replace(',', '|')
        file_out.write(new_line)
