from flask import Flask, request, jsonify, abort
import joblib
import pandas as pd
import os
from sap import xssec
from cfenv import AppEnv

app = Flask(__name__)
cf_port = os.getenv("PORT")

# Initialize environment to get XSUAA service configuration
app_env = AppEnv()
xsuaa_service = app_env.get_service(label='xsuaa')

# Authentication decorator
def require_auth(f):
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            abort(401)  # Unauthorized if no header

        try:
            token = auth_header.replace('Bearer ', '', 1)
            security_context = xssec.create_security_context(token, xsuaa_service.credentials)
            # Perform additional checks, like security_context.check_scope('<your-scope>')
        except Exception as e:
            abort(401)  # Unauthorized if token is invalid

        return f(*args, **kwargs)
    return decorated_function

# Load the trained model
model = joblib.load('trained_model.pkl')

@app.route('/predict', methods=['POST'])
@require_auth  # Protect the /predict endpoint
def predict():
    data = request.get_json(force=True)
    df = pd.DataFrame(data, index=[0])
    prediction = model.predict(df)
    return jsonify(prediction.tolist())

if __name__ == '__main__':
    if cf_port is None:
        app.run(host='0.0.0.0', port=5000, debug=True)
    else:
        app.run(host='0.0.0.0', port=int(cf_port), debug=True)
