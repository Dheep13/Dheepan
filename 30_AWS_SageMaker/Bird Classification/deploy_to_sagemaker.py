import boto3
import sagemaker
from sagemaker.pytorch import PyTorchModel
import torch
import json
import os
from google.colab import drive, userdata
import pandas as pd

# Set up AWS credentials
os.environ['AWS_ACCESS_KEY_ID'] = userdata.get('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = userdata.get('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_DEFAULT_REGION'] = userdata.get('AWS_DEFAULT_REGION')

# Set up AWS session
session = boto3.Session()
sagemaker_session = sagemaker.Session()

# Mount Google Drive
drive.mount('/content/gdrive')

# Define variables
bucket = 'deepan-sagemaker-models'
prefix = 'bird-classification-model'
role = 'arn:aws:iam::340752818264:role/SageMakerDeepan'
model_path = '/content/gdrive/MyDrive/bird_classification_final_model_with_names.pth'

# Load the model and mappings
print("Loading model...")
model_info = torch.load(model_path, map_location=torch.device('cpu'))

# Print model_info keys for debugging
print("Keys in model_info:", model_info.keys())

# Save the model state dict
torch.save(model_info['model_state_dict'], 'model.pth')

# Prepare class mappings
print("Preparing class mappings...")
idx_to_label = model_info.get('idx_to_label', {})
label_to_class_id = model_info.get('label_to_class_id', {})
label_to_scientific = model_info.get('label_to_scientific', {})

# If mappings are missing, try to recreate them from CSV files
if not idx_to_label or not label_to_class_id or not label_to_scientific:
    print("Recreating mappings from CSV files...")
    train_df = pd.read_csv('/content/train.csv')
    val_df = pd.read_csv('/content/val.csv')
    combined_df = pd.concat([train_df, val_df])

    label_to_class_id = dict(zip(combined_df['labels'], combined_df['class id']))
    label_to_scientific = dict(zip(combined_df['labels'], combined_df['scientific name']))
    idx_to_label = {i: label for i, label in enumerate(combined_df['labels'].unique())}

# Save class mappings
with open('class_mapping.json', 'w') as f:
    json.dump({
        'idx_to_label': idx_to_label,
        'label_to_class_id': label_to_class_id,
        'label_to_scientific': label_to_scientific
    }, f)

# Create a tar.gz file
print("Creating tar.gz file...")
with tarfile.open('model.tar.gz', 'w:gz') as tar:
    tar.add('model.pth')
    tar.add('class_mapping.json')

# Upload to S3
print("Uploading to S3...")
sagemaker_session.upload_data('model.tar.gz', bucket=bucket, key_prefix=prefix)
s3_model_path = f's3://{bucket}/{prefix}/model.tar.gz'

# Create SageMaker model
print("Creating SageMaker model...")
pytorch_model = PyTorchModel(
    model_data=s3_model_path,
    role=role,
    framework_version="1.8",
    py_version="py3",
    entry_point="inference.py",
)

# Deploy the model
print("Deploying the model...")
try:
    predictor = pytorch_model.deploy(
        instance_type="ml.t2.medium",
        initial_instance_count=1,
        endpoint_name="bird-classification-endpoint-2"
    )
    print(f"Model deployed successfully. Endpoint name: {predictor.endpoint_name}")
except Exception as e:
    print(f"Error during deployment: {str(e)}")

print("Deployment process completed.")