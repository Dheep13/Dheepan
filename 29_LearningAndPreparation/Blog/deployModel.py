from flask import Flask, request, jsonify
import torch
import torch.nn as nn
import os
app = Flask(__name__)

input_dim =4
# Define the neural network model with the same architecture as the trained model
class CommissionPredictor(nn.Module):
    def __init__(self):
        super(CommissionPredictor, self).__init__()
        self.fc1 = nn.Linear(input_dim, 64)
        self.fc2 = nn.Linear(64, 32)
        self.fc3 = nn.Linear(32, 1)
        self.relu = nn.ReLU()

    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.fc3(x)
        return x

# Load the trained model
model = CommissionPredictor()
print("Current Working Directory:", os.getcwd())
model.load_state_dict(torch.load('commission_predictor_model.pth'))
model.eval()

# Define a route for making predictions
@app.route('/predict', methods=['POST'])
def predict():
    # Get input parameters from the POST request
    data = request.json
    policy_value = float(data['policy_value'])
    policy_type = data['policy_type']
    region = data['region']
    add_ons = 1 if data['add_ons'].lower() == 'yes' else 0

    # Preprocess input features (convert categorical features to numerical)
    policy_type_index = {'Standard': 0, 'Premium': 1}  # Mapping for policy_type
    region_index = {'North': 0, 'South': 1, 'East': 2, 'West': 3}  # Mapping for region
    policy_type_encoded = policy_type_index.get(policy_type, -1)
    region_encoded = region_index.get(region, -1)

    # Check for invalid inputs
    if policy_type_encoded == -1 or region_encoded == -1:
        return jsonify({'error': 'Invalid input parameters'})

    # Convert input features to a PyTorch tensor
    input_tensor = torch.tensor([policy_value, policy_type_encoded, region_encoded, add_ons], dtype=torch.float32)

    # Make prediction using the loaded model
    with torch.no_grad():
        prediction = model(input_tensor).item()

    # Return the predicted commission as JSON response
    return jsonify({'predicted_commission': prediction})

if __name__ == '__main__':
    app.run(debug=True)
