# from enum import Enum
# import time
# import alpaca_trade_api as tradeapi
# import asyncio
# import os
# import pandas as pd
# import sys
# from alpaca_trade_api.rest import TimeFrame, URL
# from alpaca_trade_api.rest_async import gather_with_concurrency, AsyncRest

# NY = 'America/New_York'


# class DataType(str, Enum):
#     Bars = "Bars"
#     Trades = "Trades"
#     Quotes = "Quotes"


# def get_data_method(data_type: DataType):
#     if data_type == DataType.Bars:
#         return rest.get_bars_async
#     elif data_type == DataType.Trades:
#         return rest.get_trades_async
#     elif data_type == DataType.Quotes:
#         return rest.get_quotes_async
#     else:
#         raise Exception(f"Unsupoported data type: {data_type}")


# async def get_historic_data_base(symbols, data_type: DataType, start, end,
#                                  timeframe: TimeFrame = None):
#     """
#     base function to use with all
#     :param symbols:
#     :param start:
#     :param end:
#     :param timeframe:
#     :return:
#     """
#     major = sys.version_info.major
#     minor = sys.version_info.minor
#     if major < 3 or minor < 6:
#         raise Exception('asyncio is not support in your python version')
#     msg = f"Getting {data_type} data for {len(symbols)} symbols"
#     msg += f", timeframe: {timeframe}" if timeframe else ""
#     msg += f" between dates: start={start}, end={end}"
#     print(msg)
#     step_size = 1000
#     results = []
#     for i in range(0, len(symbols), step_size):
#         tasks = []
#         for symbol in symbols[i:i+step_size]:
#             args = [symbol, start, end, timeframe.value] if timeframe else \
#                 [symbol, start, end]
#             tasks.append(get_data_method(data_type)(*args))

#         if minor >= 8:
#             results.extend(await asyncio.gather(*tasks, return_exceptions=True))
#         else:
#             results.extend(await gather_with_concurrency(500, *tasks))

#     bad_requests = 0
#     for response in results:
#         if isinstance(response, Exception):
#             print(f"Got an error: {response}")
#         elif not len(response[1]):
#             bad_requests += 1

#     print(f"Total of {len(results)} {data_type}, and {bad_requests} "
#           f"empty responses.")


# async def get_historic_bars(symbols, start, end, timeframe: TimeFrame):
#     await get_historic_data_base(symbols, DataType.Bars, start, end, timeframe)


# async def get_historic_trades(symbols, start, end, timeframe: TimeFrame):
#     await get_historic_data_base(symbols, DataType.Trades, start, end)


# async def get_historic_quotes(symbols, start, end, timeframe: TimeFrame):
#     await get_historic_data_base(symbols, DataType.Quotes, start, end)


# async def main(symbols):
#     start = pd.Timestamp('2021-05-01', tz=NY).date().isoformat()
#     end = pd.Timestamp('2021-08-30', tz=NY).date().isoformat()
#     timeframe: TimeFrame = TimeFrame.Day
#     await get_historic_bars(symbols, start, end, timeframe)
#     await get_historic_trades(symbols, start, end, timeframe)
#     await get_historic_quotes(symbols, start, end, timeframe)


# if __name__ == '__main__':
#     api_key_id = 'PKZTP2MYLB504MD1NOIQ'
#     api_secret = 'nhEZMkaTRjsgQKA0PNDva0ZDVjTk5CKiaBbhYTqs'
#     base_url = "https://paper-api.alpaca.markets"
#     feed = "iex"  # change to "sip" if you have a paid account

#     rest = AsyncRest(key_id=api_key_id,
#                      secret_key=api_secret)

#     api = tradeapi.REST(key_id=api_key_id,
#                         secret_key=api_secret,
#                         base_url=URL(base_url))

#     start_time = time.time()
#     symbols = [el.symbol for el in api.list_assets(status='active')]
#     symbols = symbols[:200]
#     asyncio.run(main(symbols))
#     print(f"took {time.time() - start_time} sec")


# import alpaca_trade_api as tradeapi
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# from sklearn.preprocessing import MinMaxScaler
# from keras.models import Sequential
# from keras.layers import Dense, LSTM


# # Set your API key ID and secret key
# API_KEY = 'PKZTP2MYLB504MD1NOIQ'
# SECRET_KEY = 'nhEZMkaTRjsgQKA0PNDva0ZDVjTk5CKiaBbhYTqs'

# # Create an instance of the Alpaca API client
# api = tradeapi.REST(API_KEY, SECRET_KEY, api_version='v2')

# # Set the stock symbol, date range, and frequency
# symbol = 'AAPL'
# start_date = pd.Timestamp('2018-01-01', tz='America/New_York').isoformat()
# end_date = pd.Timestamp('2022-01-01', tz='America/New_York').isoformat()
# # start_date = end_date - pd.DateOffset(years=2)
# timeframe = '1D'

# # Get the historic prices for the stock
# df = api.get_bars(symbol, timeframe, start=start_date, end=end_date).df


# # Prepare the data for LSTM
# data = df.filter(['close'])
# scaler = MinMaxScaler(feature_range=(0, 1))
# scaled_data = scaler.fit_transform(data)

# n_past = 365
# n_future = 7

# X, Y = [], []
# for i in range(n_past, len(scaled_data) - n_future + 1):
#     X.append(scaled_data[i - n_past:i, 0])
#     Y.append(scaled_data[i + n_future - 1:i + n_future, 0])

# X_train, Y_train = pd.DataFrame(X), pd.DataFrame(Y)
# X_train = X_train.values.reshape((X_train.shape[0], X_train.shape[1], 1))
# Y_train = Y_train.values.reshape((Y_train.shape[0], Y_train.shape[1], 1))

# # Define the LSTM model
# model = Sequential()
# model.add(LSTM(50, return_sequences=True, input_shape=(n_past, 1)))
# model.add(LSTM(50, return_sequences=False))
# model.add(Dense(25))
# model.add(Dense(1))

# # Compile the model
# model.compile(optimizer='adam', loss='mean_squared_error')

# # Train the model
# model.fit(X_train, Y_train, epochs=100, batch_size=32)

# # Make predictions for the next 7 days
# last_n_days = data[-n_past:].values.reshape((1, n_past, 1))
# predictions = []
# for i in range(n_future):
#     prediction = model.predict(last_n_days)
#     predictions.append(prediction[0, 0])
#     last_n_days = np.append(last_n_days[:, 1:, :], prediction.reshape((1, 1, 1)), axis=1)

# # Inverse transform the scaled data to get the actual stock prices
# predictions = scaler.inverse_transform(np.array(predictions).reshape((-1, 1)))
# actual_prices = df[-n_future:]['close'].values.reshape((-1, 1))

# # Print the predicted and actual stock prices for the next 7 days
# print('Predicted stock prices:')
# for i in range(len(predictions)):
#     print(f'Day {i+1}: ${predictions[i][0]:.2f}')

# # Plot the predicted and actual stock prices
# dates = pd.date_range(end=df.index[-1], periods=n_future + 1, freq='D')[1:]
# plt.plot(dates, actual_prices, label='Actual Prices')
# plt.plot(dates, predictions, label='Predicted Prices')
# plt.title(symbol + ' Stock Price Prediction')
# plt.xlabel('Date')
# plt.ylabel
# plt.show()


import alpaca_trade_api as tradeapi
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, LSTM
import matplotlib.pyplot as plt

# Set your API key ID and secret key
API_KEY = 'PKD2Y08PE1BSUV6J89JK'
SECRET_KEY = 'ld4dnW0oNhJMbHXFxs59pceaCDMM4sXmAquv6sKh'

# Create an instance of the Alpaca API client
api = tradeapi.REST(API_KEY, SECRET_KEY, api_version='v2')

# Set the stock symbol, date range, and frequency
symbol = 'AAPL'
end_date = pd.Timestamp('now', tz='America/New_York')
start_date = end_date - pd.DateOffset(years=1)
timeframe = 'day'

# Get the historic prices for the stock
bars = api.get_bars(symbol, timeframe, start=start_date.isoformat(), end=end_date.isoformat())
df = bars.df

# Prepare the data for LSTM
data = df['close'].values.reshape(-1, 1)
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
history = model.fit(X_train, Y_train, epochs=100, batch_size=32, validation_split=0.2)

# Make predictions for the next 7 days
last_n_days = data[-n_past:]
last_n_days_scaled = scaler.transform(last_n_days)
last_n_days_scaled = last_n_days_scaled.reshape((1, n_past, 1))
predictions = []
for i in range(n_future):
    prediction = model.predict(last_n_days_scaled)
    predictions.append(prediction[0, 0])
    last_n_days_scaled = np.append(last_n_days_scaled[:, 1:, :], prediction.reshape((1, 1, 1)), axis=1)

# Inverse transform the scaled data to get the actual stock prices
predictions = scaler.inverse_transform(np.array(predictions).reshape((-1, 1)))

# Plot the predicted stock prices
dates = pd.date_range(start=df.index[-1], periods=n_future + 1, freq='D')[1:]
plt.plot(df['close'])
plt.plot(dates, predictions, color='red')
plt.title(symbol + ' Stock Price Prediction')
plt.xlabel('Date')
plt.ylabel('Price')
plt.legend(['Actual', 'Predicted'])
plt.show()
