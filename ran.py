import pandas as pd
import numpy as np

# Define number of rows and columns
num_rows = 1000
num_numeric_cols = 80
num_categorical_cols = 100
num_boolean_cols = 20

# Generate random numerical data
numeric_data = np.random.rand(num_rows, num_numeric_cols)

# Generate random categorical data (cities)
cities = ['Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Ahmedabad', 'Chennai', 'Kolkata', 'Surat', 'Pune', 'Jaipur']
categorical_data = np.random.choice(cities, size=(num_rows, num_categorical_cols))

# Generate random boolean data
boolean_data = np.random.choice([True, False], size=(num_rows, num_boolean_cols))

# Generate random student names
student_names = [f'Student_{i+1}' for i in range(num_rows)]

# Combine data
data = np.hstack((np.array(student_names).reshape(num_rows, 1), numeric_data, categorical_data, boolean_data))

# Create a DataFrame with appropriate column names
columns = ['student_name'] + \
          [f'numeric_feature_{i+1}' for i in range(num_numeric_cols)] + \
          [f'city_{i+1}' for i in range(num_categorical_cols)] + \
          [f'boolean_feature_{i+1}' for i in range(num_boolean_cols)]

df = pd.DataFrame(data, columns=columns)

# Display the first few rows
print(df.head())


df.to_csv("ran.csv")