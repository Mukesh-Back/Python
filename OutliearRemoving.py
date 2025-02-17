import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df = pd.read_csv("C:\\Python_Practice\\CSV\\data.csv")

plt.boxplot(df.select_dtypes(include=["int", "float"]).values)
plt.title('Initial Boxplot')
plt.show()

def iqr(column):
    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    return q1 - 1.5 * iqr, q3 + 1.5 * iqr

while True:
    previous_shape = df.shape
    for col in df.select_dtypes(include=["float", "int"]).columns:
        low, high = iqr(col)
        df = df[(df[col] >= low) & (df[col] <= high)]
    current_shape = df.shape
    print(f'Shape after removing outliers: {current_shape}')
    if previous_shape == current_shape:
        break
print(f'Final shape after outlier removal: {df.shape}')

plt.boxplot(df.select_dtypes(include=["int", "float"]).values)
plt.title('Boxplot After Outlier Removal')
plt.show()

numeric_data = df.select_dtypes(include=["int", "float"])
categorical_data = df.select_dtypes(include="object")

categorical_data = categorical_data.loc[:, categorical_data.nunique() < 10]

df_encoded = pd.get_dummies(categorical_data, drop_first=True).astype(float)

final_df = pd.concat([numeric_data, df_encoded], axis=1)

def iqr(column):
    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    return q1 - 1.5 * iqr, q3 + 1.5 * iqr

while True:
    previous_shape = final_df.shape
    for col in df.select_dtypes(include=["float", "int"]).columns:
        low, high = iqr(col)
        final_df = final_df[(final_df[col] >= low) & (final_df[col] <= high)]
    current_shape = final_df.shape
    print(f'Shape after removing outliers: {current_shape}')
    if previous_shape == current_shape:
        break
print(f'Final shape after outlier removal: {final_df.shape}')

plt.boxplot(final_df.select_dtypes(include=["int", "float"]).values)
plt.title('Boxplot After Outlier Removal')
plt.show()


final_df.to_csv("ml_ready_data.csv", index=False)
