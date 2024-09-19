import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
import tensorflow as tf
from tensorflow.keras import layers
import matplotlib.pyplot as plt

# Generate some random data for 100 payees over the past 5 years
num_payees = 100
num_years = 5
num_months = num_years * 12
Year = np.repeat(np.arange(num_years) + 1, 12)
Month = np.tile(np.arange(1, 13), num_years)
Season = pd.cut(Month, [0, 3, 6, 9, 12], labels=['Winter', 'Spring', 'Summer', 'Fall']).astype(str)
Product_Type = np.random.choice(['Product A', 'Product B', 'Product C'], size=num_months)
Commissions = np.random.rand(num_payees, num_years, 12) * 1000
Commissions = Commissions.reshape(num_payees, num_months).mean(axis=0)

# Check the length of each array
print(len(Year), len(Month), len(Season), len(Product_Type), len(Commissions))
assert len(Year) == len(Month) == len(Season) == len(Product_Type) == len(Commissions)

# Combine features into a single dataframe
df = pd.DataFrame({
    'Year': Year,
    'Month': Month,
    'Season': Season,
    'Product Type': Product_Type,
    'Commissions': Commissions
})

df = pd.get_dummies(df, columns=['Season', 'Product Type'])

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(df.drop(['Commissions'], axis=1), df['Commissions'], test_size=0.2, random_state=42)

# Define the deep neural network model
model = tf.keras.Sequential([
    layers.Dense(64, activation='relu', input_shape=[X_train.shape[1]]),
    layers.Dense(32, activation='relu'),
    layers.Dense(1)
])

model.compile(optimizer=tf.keras.optimizers.Adam(0.001),
              loss='mean_squared_error',
              metrics=['mean_absolute_error'])

# Train the deep neural network model
history = model.fit(X_train, y_train, epochs=100, validation_split=0.2, verbose=0)

# Make predictions for the next year
future_X = pd.DataFrame({
    'Year': [num_years + 1] * 12,
    'Month': np.arange(1, 13),
    'Season': pd.cut(np.arange(1, 13), [0, 3, 6, 9, 12], labels=['Winter', 'Spring', 'Summer', 'Fall']).astype(str),
    'Product Type': np.random.choice(['Product A', 'Product B', 'Product C'], size=12),
})
future_X = pd.get_dummies(future_X, columns=['Season', 'Product Type'])
future_predictions = model.predict(future_X)

# Plot the actual and predicted average monthly commission for each year
plt.figure(figsize=(10, 6))
plt.plot(df.groupby('Year')['Commissions'].mean(), 'bo-', label='Actual')
plt.plot([num_years + 1], future_predictions.mean(), 'go', label='Predicted')
plt.xlabel('Year')
plt.ylabel('Average Monthly Commission per Payee')
plt.title('Actual vs Predicted Average Monthly Commission for 100 Payees')
plt.legend()
plt.show()
