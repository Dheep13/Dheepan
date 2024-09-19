import joblib

# Load the encoder from the specified file path
encoder_file_path = r'C:\Users\I520292\OneDrive - SAP SE\Visual Studio Code\29_LearningAndPreparation\Blog\encoder.pkl'
loaded_encoder = joblib.load(encoder_file_path)

# Value to encode
value_to_encode = 'Standard'

# Encode the value using the loaded encoder
encoded_value = loaded_encoder.transform([value_to_encode])[0]

print("Encoded value:", encoded_value)
