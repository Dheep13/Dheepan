import os
from werkzeug.utils import secure_filename
from flask import Flask,flash,request,redirect,send_file,render_template
import pandas as pd
import json
UPLOAD_FOLDER = 'uploads/'
#app = Flask(__name__)
app = Flask(__name__, template_folder='templates')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# Upload API
@app.route('/uploadfile', methods=['GET', 'POST'])
def upload_file():
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
            filename = secure_filename(file.filename)
            f = request.files['file']
            excel_data_df = pd.read_excel(f, sheet_name='Scoping')
            thisisjson = excel_data_df.to_json(orient='records')
            # Serializing json 
            #json_object = json.loads(thisisjson)
            # Writing to sample.json
            #json_file ="sample.json"
            #thisisjson_dict = json.loads(thisisjson)
            json_str='{"Scoping": ' + str(thisisjson) + '}'
            f = open(os.path.join(app.config['UPLOAD_FOLDER'],"FinalData.json"), "w")
            f.write(json_str)
            f.close()
            # with open(os.path.join(app.config['UPLOAD_FOLDER'], 'sample.json'), "w") as json_file:
            #     json.dump(json_object, json_file)
    
            # Json with the scoping header--commented because it was fixed
            # f= open(os.path.join(app.config['UPLOAD_FOLDER'], 'sample.json'), "r")
            # json_data=json.loads(f.read())
            # json_data_final= '{'+ '"' + 'Scoping' +'"' +':' + str(json_data) + '}'
            # with open( os.path.join(app.config['UPLOAD_FOLDER'], json_file), "w") as json_file:
            #     json.dump(json_data_final, json_file)
            # with open(os.path.join(app.config['UPLOAD_FOLDER'], 'sample.json'), 'w') as json_file:
            #     json.dump(json_data_final, json_file)
            # f = open(os.path.join(app.config['UPLOAD_FOLDER'],"FinalData.json"), "w")
            # f.write(json_data_final)
            # f.close()

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
    return send_file(file_path, as_attachment=True, attachment_filename='')
if __name__ == "__main__":
    app.run(host='localhost', debug=True)