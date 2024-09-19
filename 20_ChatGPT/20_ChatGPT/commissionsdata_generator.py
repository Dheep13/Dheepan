import numpy as np
import pandas as pd

# Set random seed for reproducibility
np.random.seed(42)

# Generate some random data for 100 payees over the past 5 years
num_payees = 100
num_years = 5
num_months = num_years * 12

# Define seasonal commission multipliers
winter_multiplier = 0.5
spring_multiplier = 1.2
summer_multiplier = 1.5
fall_multiplier = 1.1

# Define product commission multipliers
product_a_multiplier = 0.8
product_b_multiplier = 1.0
product_c_multiplier = 1.2

# Define product prices
product_a_price = 50
product_b_price = 100
product_c_price = 150

# Reshape commissions to have one row for each month
commissions = np.random.rand(num_payees, num_years, 12)

# Calculate commission multipliers based on season and product type
seasons = pd.cut(np.repeat(np.arange(1, 13), num_years), [0, 3, 6, 9, 12], labels=['Winter', 'Spring', 'Summer', 'Fall']).astype(str)
product_types = np.random.choice(['Product A', 'Product B', 'Product C'], size=num_months)
commission_multipliers = pd.DataFrame({
    'Season': seasons,
    'Product Type': product_types,
})
commission_multipliers['Season Multiplier'] = np.where(commission_multipliers['Season'] == 'Winter', winter_multiplier,
                                                       np.where(commission_multipliers['Season'] == 'Spring', spring_multiplier,
                                                                np.where(commission_multipliers['Season'] == 'Summer', summer_multiplier, fall_multiplier)))
commission_multipliers['Product Multiplier'] = np.where(commission_multipliers['Product Type'] == 'Product A', product_a_multiplier,
                                                        np.where(commission_multipliers['Product Type'] == 'Product B', product_b_multiplier, product_c_multiplier))
commission_multipliers['Price'] = np.where(commission_multipliers['Product Type'] == 'Product A', product_a_price,
                                           np.where(commission_multipliers['Product Type'] == 'Product B', product_b_price, product_c_price))

# Expand commission multipliers to have the same shape as commissions
commission_multipliers_expanded = np.repeat(commission_multipliers.values[:, np.newaxis, :], num_payees, axis=1)

# Calculate commissions by multiplying random commission data by commission multipliers
commissions *= commission_multipliers_expanded[:, :, [2, 3]] # multiply by season and product type multipliers
commissions *= commission_multipliers_expanded[:, :, 4][:, :, np.newaxis] # multiply by price

# Flatten commissions and add noise
commissions = commissions.reshape(num_payees, num_months).mean(axis=0)
commissions += np.random.normal(scale=50, size=num_months)

# Combine features into a single dataframe
df = pd.DataFrame({
    'Year': np.tile(np.arange(1, num_years + 1), 12),
    'Month': np.repeat(np.arange(1, 13), num_years),
    'Season': seasons,
    'Product Type': product_types,
    'Price': commission_multipliers['Price'],
    'Commissions': commissions
})

# One-hot encode categorical features
df = pd.get_dummies(df, columns=['Season', 'Product Type'])

# Print the first 5 rows of the dataframe
print(df.head())
