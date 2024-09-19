import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, make_scorer
from sklearn.preprocessing import StandardScaler

# Load your data
train_data_path = 'train.csv'
test_data_path = 'test.csv'
train_data = pd.read_csv(train_data_path)
test_data = pd.read_csv(test_data_path)

# Data Preprocessing
categorical_vars = ['source', 'destination', 'cab_type', 'product_id', 'name', 'short_summary', 'long_summary', 'icon']


# Apply get_dummies() for categorical variables
train_data_encoded = pd.get_dummies(train_data, columns=categorical_vars, drop_first=True)
test_data_encoded = pd.get_dummies(test_data, columns=categorical_vars, drop_first=True)

# Ensure 'datetime' is converted properly and retained for feature engineering
train_data_encoded['datetime'] = pd.to_datetime(train_data_encoded['datetime'], errors='coerce')
test_data_encoded['datetime'] = pd.to_datetime(test_data_encoded['datetime'], errors='coerce')

# Extract datetime features
for df in [train_data_encoded, test_data_encoded]:
    df['hour'] = df['datetime'].dt.hour
    df['day_of_week'] = df['datetime'].dt.dayofweek
    df['month'] = df['datetime'].dt.month

# Drop the original 'datetime' column after extracting features
train_data_encoded.drop('datetime', axis=1, inplace=True, errors='ignore')
test_data_encoded.drop('datetime', axis=1, inplace=True, errors='ignore')

# Align train and test data to have the same columns, filling missing with 0s
common_columns = [col for col in train_data_encoded.columns if col in test_data_encoded.columns and col not in ['id', 'timezone', 'price']]
X = train_data_encoded[common_columns]
y = train_data['price']
X_test_aligned = test_data_encoded[common_columns]

# Model Training and Evaluation
model = LinearRegression()

# Cross-validation with 5 folds
rmse_scorer = make_scorer(mean_squared_error, squared=False)  # RMSE scorer
cross_val_scores = cross_val_score(model, X, y, cv=5, scoring=rmse_scorer)

print(f'RMSE scores across the folds: {cross_val_scores}')
print(f'Mean RMSE score: {np.mean(cross_val_scores)}')

model.fit(X, y)

# Kaggle Submission
test_predictions = model.predict(X_test_aligned)

submission_df = pd.DataFrame({
    'price': np.round(test_predictions, 1),
    'id': test_data.iloc[:, 0]
})
# Save to CSV (optional)
submission_df.to_csv('predictions.csv', index=False)
print("Predictions saved to 'predictions.csv'")
