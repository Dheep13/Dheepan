import csv

input_file = "input_file.csv"

relationships = {}
lines_by_id = {}
all_files = []

header = None
original_rows_set = set()
duplicates_dict = {}  # {position_name: [duplicate rows...]}

input_file_count = 0
output_file_count = 0

# First, identify duplicates
with open(input_file, 'r') as f:
    reader = csv.reader(f, delimiter='|')
    header = next(reader)
    
    seen_positions = {}
    for row in reader:
        row_str = '|'.join(row)
        position_name = row[0]
        input_file_count += 1
        
        if position_name in seen_positions:
            duplicates_dict.setdefault(position_name, [seen_positions[position_name]]).append(row_str)
        else:
            seen_positions[position_name] = row_str
            original_rows_set.add(row_str)

# Remove all duplicate rows from the main set
for rows in duplicates_dict.values():
    for row in rows:
        original_rows_set.discard(row)

# Perform hierarchy logic on non-duplicate rows
for row_str in original_rows_set:
    row = row_str.split('|')
    employee_id = row[0]
    manager_id = row[6] if row[6] else None
    relationships[employee_id] = manager_id
    lines_by_id[employee_id] = row_str

no_manager = [emp for emp, mgr in relationships.items() if not mgr or mgr not in relationships]
all_files.append([lines_by_id[emp] for emp in no_manager])

for emp in no_manager:
    original_rows_set.discard(lines_by_id[emp])

next_level = no_manager
while next_level:
    subordinates = []
    for manager in next_level:
        subs = [emp for emp, mgr in relationships.items() if mgr == manager]
        subordinates.extend(subs)
        
        for sub in subs:
            row_string = lines_by_id.get(sub)
            original_rows_set.discard(row_string)

    if not subordinates:
        break

    all_files.append([lines_by_id[emp] for emp in subordinates])
    next_level = subordinates

missed_rows = list(original_rows_set)
if missed_rows:
    all_files.append(missed_rows)

# Flatten duplicate rows for final file
all_duplicates = [row for sublist in duplicates_dict.values() for row in sublist]
all_files.append(all_duplicates)

sequence_number = 1
for file_content in all_files:
    filename_suffix = "duplicates" if file_content == all_duplicates else sequence_number
    with open(f'2056_OGPO_{filename_suffix}.txt', 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerow(header)
        for row in file_content:
            writer.writerow(row.split('|'))
            output_file_count += 1
        if filename_suffix != "duplicates":
            sequence_number += 1

print("Files have been split based on the hierarchy!")
print(f"Total rows in input file: {input_file_count}")
print(f"Total rows in split files: {output_file_count}")

if input_file_count == output_file_count:
    print("Row counts match!")
else:
    print("Warning: Row counts do not match!")
