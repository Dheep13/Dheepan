import os
from werkzeug.utils import secure_filename
from flask import Flask,flash,request,redirect,send_file,render_template
import pandas as pd
from flask_sqlalchemy import SQLAlchemy 
from flask_login import LoginManager 

db = SQLAlchemy()

UPLOAD_FOLDER = 'uploads/'
#app = Flask(__name__)
app = Flask(__name__, template_folder='templates')
cf_port = os.getenv("PORT")
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Upload API
@app.route('/uploadfile', methods=['GET', 'POST'])
def upload_file():
    #Clear directory before proceeding
    curr_dir = app.config['UPLOAD_FOLDER']
    for f in os.listdir(curr_dir):
        os.remove(os.path.join(curr_dir, f))

    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            print('no file')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            print('no filename')
            return redirect(request.url)
        else:
            # for filename in os.listdir(os.getcwd()):
            #     if filename.endswith(".xlsx"):
            #      # print(filename)
            #         file_name=filename
            #xl_file_name = request.files['file']

            xl_file_name = secure_filename(file.filename)
            #if xl_file_name.endswith(".xlsx"):
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], xl_file_name))
            xl_file_path=os.path.join(app.config['UPLOAD_FOLDER'], xl_file_name)
            json_file_path=os.path.join(app.config['UPLOAD_FOLDER'], 'FinalData.json')
            
            print(xl_file_name)

            #xl = pd.ExcelFile(file_name)
            #res = len(xl.sheet_names)
            #print (df)
            #outfile.save(os.path.join(app.config['UPLOAD_FOLDER'], json_file))
            excel_data_df = pd.read_excel(xl_file_path)
            print (excel_data_df)
            #xls = xlrd.open_workbook(xl_file_path, on_demand=True)
            excel_info= pd.ExcelFile(xl_file_path)
            sheet_names=excel_info.sheet_names

            #sheet_names=xls.sheet_names()
            # print (sheet_names())
            #print(type(sheet_names))

# As file at filePath is deleted now, so we should check if file exists or not not before deleting them
            if os.path.exists(json_file_path):
                os.remove(json_file_path)

            #os.remove("newjsondata.json")
            f = open("FinalData.json", "w")
            f.close()
            json_str=''

            for sheet in sheet_names:
                excel_data_df = pd.read_excel(xl_file_path, sheet_name=sheet)
                thisisjson = excel_data_df.to_json(orient='records')
                json_str=json_str+'"' + sheet + '"'':' + str(thisisjson) + ','

            l = len(json_str) 
            json_str ='{' + json_str[:l-1] + '}'
            print('able')
            f = open(json_file_path, "w")
            f.write(json_str)
            f.close()
            
            # filename = secure_filename(file.filename)
            # f = request.files['file']
            # excel_data_df = pd.read_excel(f, sheet_name='Scoping')
            # thisisjson = excel_data_df.to_json(orient='records')
            # json_str='{"Scoping": ' + str(thisisjson) + '}'
            
            json_file_name='FinalData.json'
        # outfile.save(os.path.join(app.config['UPLOAD_FOLDER'], json_file))
            print("saved file successfully")
    #send file name as parameter to downlad
            return redirect('/downloadfile/'+ json_file_name)
    return render_template('upload_file.html')

def new_func():
    return 'w'
# Download API
@app.route("/downloadfile/<filename>", methods = ['GET'])

def download_file(filename):
   return render_template('download.html',value=filename)

@app.route('/return-files/<filename>')

def return_files_tut(filename):
    file_path = UPLOAD_FOLDER + filename
    return send_file(file_path, as_attachment=True, attachment_filename=filename,cache_timeout=0)

# #Clear Cache
# cache=Cache()
# cache.init_app(app)
# with app.app_context():
#         cache.clear()

if __name__ == '__main__':
    
	if cf_port is None:
		app.run(host='0.0.0.0', port=4000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)

    # app.run(host='localhost', debug=True)