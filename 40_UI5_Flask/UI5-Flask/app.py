from flask import Flask, render_template, request, jsonify
import requests
import os

app = Flask(__name__)
port = int(os.getenv("PORT", 0))

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/generate_api_results', methods=['POST'])
def generate_api_results():
    # Get values from the request
    input1 = request.form.get('input1')
    input2 = request.form.get('input2')

    # Construct the API URL
    api_url = f"{input1}/{input2}"

    try:
        # Make a request to the API
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an exception for bad responses

        # Parse the JSON response
        api_results = response.json()

        return jsonify(api_results)
    
    except requests.exceptions.RequestException as e:
        # Handle request errors
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    if port != 0:
        app.run(host='0.0.0.0', port=port)
    else:
        app.run()  
