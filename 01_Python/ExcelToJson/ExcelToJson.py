# import pandas
# import json
# import flask as Flask
# import os

# app = Flask(__name__)
# cf_port = os.getenv("PORT")

# excel_data_df = pandas.read_excel('records.xlsx', sheet_name='Scoping')

# json_str = excel_data_df.to_json()

# print('Excel Sheet to JSON:\n', json_str)

# # Make the string into a list to be able to input in to a JSON-file
# thisisjson_dict = json.loads(json_str)
# # Define file to write to and 'w' for write option -> json.dump() defining the list to write from and file to write to
# with open('data.json', 'w') as json_file:
#     json.dump(thisisjson_dict, json_file)


from flask import Flask, request, jsonify
import pandas as pd
import json

app=Flask(__name__)

@app.route("/upload", methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        print(request.files['file'])
        f = request.files['file']
        data_xls = pd.read_excel(f)
        excel_data_df = pd.read_excel(f, sheet_name='Scoping')
        json_str = excel_data_df.to_json()
        return data_xls.to_html()
        thisisjson_dict = json.loads(json_str)
        with open('data.json', 'w') as json_file:
            json.dump(thisisjson_dict, json_file)
        #return json_file.to_html()

    return '''
    <!doctype html>
    <title>Upload an excel file</title>
    <h1>Excel to json converter for Sales and Service Cloud</h1>
    <form action="" method=post enctype=multipart/form-data>
    <p><input type=file name=file><input type=submit value=Upload>
    </form>
    '''

@app.route("/export", methods=['GET'])
def export_records():
    return 

if __name__ == "__main__":
    app.run()