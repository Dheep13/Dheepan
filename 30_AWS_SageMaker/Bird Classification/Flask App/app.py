from flask import Flask, request, render_template, url_for
import boto3
import json
from PIL import Image
import io
import base64

app = Flask(__name__)

# Configure AWS credentials and region
import os
os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAU6VTTMBMNZ7T4ON7'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'
os.environ['AWS_DEFAULT_REGION'] = 'ap-southeast-1'

# SageMaker endpoint name
ENDPOINT_NAME = 'bird-classification-endpoint-2'

def predict_image(image_bytes):
    runtime = boto3.client('sagemaker-runtime')
    
    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType='application/x-image',
        Body=image_bytes
    )
    
    result = json.loads(response['Body'].read().decode())
    return result

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return render_template('index.html', error='No file part')
        
        file = request.files['file']
        
        if file.filename == '':
            return render_template('index.html', error='No selected file')
        
        if file:
            # Read the image file
            image_bytes = file.read()
            
            try:
                # Get prediction
                prediction = predict_image(image_bytes)
                
                # Convert image to base64 for displaying
                img = Image.open(io.BytesIO(image_bytes))
                img_base64 = base64.b64encode(image_bytes).decode('utf-8')
                
                return render_template('result.html', 
                                       image=img_base64, 
                                       class_id=prediction['class_id'],
                                       scientific_name=prediction['scientific_name'],
                                       predicted_label=prediction['predicted_label'])
            except Exception as e:
                return render_template('index.html', error=f'Error during prediction: {str(e)}')
    
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)