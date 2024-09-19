from flask import Flask, render_template

app = Flask(__name__)

# list of payees
payees = ["John Doe", "Jane Smith", "Bob Johnson"]

@app.route('/')
def index():
    return render_template('index.html', payees=payees)

if __name__ == '__main__':
    app.run(debug=True)