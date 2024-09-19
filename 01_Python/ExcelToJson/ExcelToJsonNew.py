import os
from flask import Flask, render_template, abort, url_for, json, jsonify, request
import json
import html
from pandas.io import excel
import pandas as pd

app = Flask(__name__)

# read file
# with open('file.json', 'r') as myfile:
#     data = myfile.read()

@app.route("/Upload", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        print(request.files['file'])
        f = request.files['file']
        #data_xls = pd.read_excel(f)
        excel_data_df = pd.read_excel(f, sheet_name='Scoping')
        json_str = excel_data_df.to_json()
        thisisjson_dict = json.loads(json_str)
        with open('data.json', 'w') as json_file:
            json.dump(thisisjson_dict, json_file)
        return excel_data_df.to_html()
        #return render_template('index.html', title="page", jsonfile="data.json")
        #return render_template('index.html', title="page", excel=excel_data_df)    
    return '''
    <!doctype html>
    <title>Upload an excel file</title>
    <h1>Excel to json converter for Sales and Service Cloud</h1>
    <form action="" method=post enctype=multipart/form-data>
    <p><input type=file name=file><input type=submit value=Upload>
    </form>
    '''

if __name__ == '__main__':
    app.run(host='localhost', debug=True)