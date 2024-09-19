import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib

# Step 2: Load the dataset
data = pd.read_csv('commission_data.csv')

# Step 3: Preprocess the data
# Separate features and target variable
X = data.drop(columns=['commission'])
y = data['commission']

# Specify categorical columns
categorical_features = ['policy_type', 'region', 'add_ons']

# Create a ColumnTransformer to encode categorical columns
# and standard scale the numerical features (if there are any)
preprocessor = ColumnTransformer(
    transformers=[
        ('cat', OneHotEncoder(), categorical_features)
    ],
    remainder='passthrough'  # This will leave numerical features untouched
)

# Step 4: Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Step 5: Define and train the model within a Pipeline
model = Pipeline([
    ('preprocessor', preprocessor),
    ('scaler', StandardScaler(with_mean=False)),  # Use with_mean=False to avoid issues with sparse matrices
    ('regression', LinearRegression())
])

model.fit(X_train, y_train)

# Step 6: Evaluate the model
train_score = model.score(X_train, y_train)
test_score = model.score(X_test, y_test)
print("Training R-squared score:", train_score)
print("Testing R-squared score:", test_score)

# Step 7: Save the trained model
joblib.dump(model, 'trained_model.pkl')
