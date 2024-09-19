import pandas as pd
import numpy as np

# Define parameters for data generation
n_rows = 10000
policy_values = np.random.randint(5000, 50000, n_rows)  # Policy values between 5000 and 50000
policy_types = np.random.choice(['Standard', 'Premium'], n_rows)
regions = np.random.choice(['North', 'South', 'West', 'East'], n_rows)
add_ons = np.random.choice(['Yes', 'No'], n_rows)

# Calculate commission based on the formula
def calculate_commission(policy_value, policy_type, region, add_on):
    base_rate = 0.02
    type_bonus = 500 if policy_type == 'Premium' else 0
    region_bonus = {'North': 200, 'South': 150, 'West': 100, 'East': 50}[region]
    add_ons_bonus = 300 if add_on == 'Yes' else 0
    
    commission = (base_rate * policy_value) + type_bonus + region_bonus + add_ons_bonus
    return commission

# Generate commission for each row
commissions = [calculate_commission(policy_value, policy_type, region, add_on)
               for policy_value, policy_type, region, add_on in zip(policy_values, policy_types, regions, add_ons)]

# Create DataFrame
data = pd.DataFrame({
    'policy_value': policy_values,
    'policy_type': policy_types,
    'region': regions,
    'add_ons': add_ons,
    'commission': commissions
})

# Save to CSV
# csv_file_path = 'C:\Users\I520292\OneDrive - SAP SE\Visual Studio Code\29_LearningAndPreparation\Blog'
data.to_csv('commission_data.csv', index=False)

