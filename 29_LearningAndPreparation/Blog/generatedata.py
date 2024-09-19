import pandas as pd
import numpy as np

# Define the number of rows for the dataset
num_rows = 10000

# Generate synthetic data for policy_value
policy_value = np.random.uniform(5000, 50000, size=num_rows)

# Generate synthetic data for policy_type
policy_type = np.random.choice(['Standard', 'Premium'], size=num_rows)

# Generate synthetic data for region
region = np.random.choice(['North', 'South', 'East', 'West'], size=num_rows)

# Generate synthetic data for add_ons
add_ons = np.random.choice(['Yes', 'No'], size=num_rows)

# Generate synthetic data for commission based on the features
# You can replace this with your actual commission calculation logic
commission = np.random.normal(500, 100, size=num_rows)

# Create a DataFrame from the synthetic data
data = pd.DataFrame({
    'policy_value': policy_value,
    'policy_type': policy_type,
    'region': region,
    'add_ons': add_ons,
    'commission': commission
})

# Save the synthetic dataset to a CSV file
data.to_csv('synthetic_dataset.csv', index=False)
