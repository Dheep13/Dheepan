import boto3
import json
from PIL import Image
import io

# Initialize the SageMaker runtime client
runtime = boto3.client('sagemaker-runtime')

# Your endpoint name
endpoint_name = 'bird-classification-endpoint'

def predict_image(image_path):
    # Open and read the image file
    with open(image_path, 'rb') as f:
        image_data = f.read()
    
    # Make a prediction
    response = runtime.invoke_endpoint(
        EndpointName=endpoint_name,
        ContentType='application/x-image',
        Body=image_data
    )
    
    # Parse the response
    result = json.loads(response['Body'].read().decode())
    return result

# Test the function
test_image_path = '3.jpg'  # Replace with your test image path
prediction = predict_image(test_image_path)
print(f"Prediction: {prediction}")