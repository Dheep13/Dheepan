import alpaca_trade_api as tradeapi
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, LSTM

# Set your API key ID and secret key
API_KEY = 'PKU299ADBOYMTQU9K986'
SECRET_KEY = 'dGOQhqyAFrLoR9Dt6MJeSI4OfbgLvX05hQ19e4rO'

# Create an instance of the Alpaca API client
api = tradeapi.REST(API_KEY, SECRET_KEY, api_version='v2')

# Get the last 100 days of TCS (Tata Consultancy Services) stock data
symbol = 'TCS'
timeframe = '1D'
start_date = pd.Timestamp.now() - pd.Timedelta(days=100)
end_date = pd.Timestamp.now()
df = api.get_bars(symbol, timeframe, start=start_date, end=end_date).df

# Prepare the data for LSTM
data = df.filter(['Close'])
scaler = MinMaxScaler(feature_range=(0, 1))
scaled_data = scaler.fit_transform(data)

n_past = 30
n_future = 7

X, Y = [], []
for i in range(n_past, len(scaled_data) - n_future + 1):
    X.append(scaled_data[i - n_past:i, 0])
    Y.append(scaled_data[i + n_future - 1:i + n_future, 0])

X_train, Y_train = pd.DataFrame(X), pd.DataFrame(Y)
X_train = X_train.values.reshape((X_train.shape[0], X_train.shape[1], 1))
Y_train = Y_train.values.reshape((Y_train.shape[0], Y_train.shape[1], 1))

# Define the LSTM model
model = Sequential()
model.add(LSTM(50, return_sequences=True, input_shape=(n_past, 1)))
model.add(LSTM(50, return_sequences=False))
model.add(Dense(25))
model.add(Dense(1))

# Compile the model
model.compile(optimizer='adam', loss='mean_squared_error')

# Train the model
model.fit(X_train, Y_train, epochs=100, batch_size=32)

# Make predictions for the next 7 days
last_n_days = data[-n_past:].values.reshape((1, n_past, 1))
predictions = []
for i in range(n_future):
    prediction = model.predict(last_n_days)
    predictions.append(prediction[0, 0])
    last_n_days = np.append(last_n_days[:, 1:, :], prediction.reshape((1, 1, 1)), axis=1)

# Inverse transform the scaled data to get the actual stock prices
predictions = scaler.inverse_transform(np.array(predictions).reshape((-1, 1)))

# Print the predicted stock prices for the next 7 days
print(predictions)
