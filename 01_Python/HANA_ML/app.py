from hana_ml import dataframe
from hana_ml.algorithms.pal import linear_model
from hana_ml.algorithms.pal.partition import train_test_val_split
from db_config import HANA_CONNECTION_PARAMETERS

# Establishing the connection
conn = dataframe.ConnectionContext(**HANA_CONNECTION_PARAMETERS)

# Creating a HANA DataFrame. Assume 'MY_SCHEMA.MY_TABLE' is a table in HANA.
hana_df = conn.table('CS_CREDIT', schema='TCMP')


# Splitting the data into training and testing datasets
train_df, test_df, val_df= train_test_val_split(data=hana_df,
                                                       training_percentage=0.7,
                                                       testing_percentage=0.2,
                                                       validation_percentage=0.1)
print(train_df)

# Initializing the Linear Regression model
lr_model = linear_model.LinearRegression(conn_context=conn)
lr_model = linear_model.l

# Training the model
lr_model.fit(train_df, features=['GENERICATTRIBUTE1', 'GENERICATTRIBUTE2'], label='VALUE')

# Predicting on the test dataset
predictions = lr_model.predict(test_df)

# Displaying predictions
print(predictions.collect())  # Use collect() to bring the results into the client

# Close the connection
conn.close()
