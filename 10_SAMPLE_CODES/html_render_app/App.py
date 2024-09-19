#Following is the example of connecting to database  
#Import module
from flask import Flask, render_template
import os

app = Flask(__name__)
cf_port = os.getenv("PORT")

@app.route("/images")
def index():
    # return ('Hello World')
    return render_template('index.html') # You have to save the html files
                                         # inside of a 'templates' folder.
    
if __name__ == '__main__':
    app.run(host="localhost", port=5000, debug=True)
