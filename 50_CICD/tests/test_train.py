# tests/test_train.py

import numpy as np
from sklearn.linear_model import LinearRegression

def test_model_training():
    # Create simple data
    X = np.array([[1], [2], [3], [4]])
    y = np.array([[3], [5], [7], [9]])

    # Train model
    model = LinearRegression()
    model.fit(X, y)

    # Check model predictions
    predictions = model.predict(np.array([[5]]))
    assert predictions == 11, "Model is not predicting correctly"
