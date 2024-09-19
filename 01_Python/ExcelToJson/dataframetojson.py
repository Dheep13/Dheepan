import pandas as pd
import json
import xlrd
import os
# Read excel document

#excel_data_df = pandas.read_excel('scoping.xlsx', sheet_name='Scoping')
#excel_data_df = pandas.read_excel('scoping.xlsx', sheet_name='Scoping')

for filename in os.listdir(os.getcwd()):
   if filename.endswith(".xlsx"):
    # print(filename)
    file_name=filename

xl = pd.ExcelFile(file_name)
res = len(xl.sheet_names)
#print (df)
excel_data_df = pd.read_excel(file_name)
#xls = xlrd.open_workbook(file_name, on_demand=True)
#xls = xlrd.open_workbook(xl_file_path, on_demand=True)
excel_info= pd.ExcelFile(file_name)
sheet_names=excel_info.sheet_names
print('Sheet names type is' + str(type(sheet_names)))

#sheet_names=xls.sheet_names()
#print (xls.sheet_names())
#print(type(sheet_names))

os.remove("newjsondata.json")
f = open("newjsondata.json", "w")
f.close()
json_str=''

for sheet in sheet_names:
    excel_data_df = pd.read_excel('scoping.xlsx', sheet_name=sheet)
    thisisjson = excel_data_df.to_json(orient='records')
    json_str=json_str+'"' + sheet + '"'':' + str(thisisjson) + ','

l = len(json_str) 
json_str ='{' + json_str[:l-1] + '}'

f = open("newjsondata.json", "w")
f.write(json_str)
f.close()

# with open('newjsondata.json', 'r') as f:
#     data = f.read()
#     json_data = json.loads(data)
#     json_str = str(json_data)

# f = open("newjsondata.json", "w")
# f.write(json_str)
# f.close()


# Convert excel to string (define orientation of document in this case from up to down)
# thisisjson = excel_data_df.to_json(orient='records')
# Print out the result
#print('Excel Sheet to JSON:\n', thisisjson)
# Make the string into a list to be able to input in to a JSON-file
# thisisjson_dict = json.loads(thisisjson)
# json_str='{"Scoping": ' + str(thisisjson) + '}'
# f = open("newjsondata.json", "w")
# f.write(json_str)
# f.close()

# Define file to write to and 'w' for write option -> json.dump() defining the list to write from and file to write to
# with open('data.json', 'w') as json_file:
#     json.dump(thisisjson_dict, json_file)
# print(thisisjson)
# f= open('data.json', 'r')
# json_data=json.loads(f.read())
# json_data_final= '{"Scoping": ' + str(json_data) + '}'
# f = open("newjsondata.json", "w")
# f.write(json_data_final)
# f.close()