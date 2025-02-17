uuuimport pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df1 = pd.read_csv("C:\\Python_Practice\\CSV\\data.csv",index_col=0)
df=df1.copy()

#Outliear Removing Code Start ***
for i in df.select_dtypes(include="object"):
    print(df[i].unique())

plt.boxplot(df.select_dtypes(include=["int", "float"]))
plt.title('Initial Boxplot')
plt.show()

def iqr_bounds(column):
    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    low_bound = q1 - 1.5 * iqr
    high_bound = q3 + 1.5 * iqr
    return [low_bound, high_bound]

while True:
    previous_shape = df.shape
    for col in df.select_dtypes(include=["float", "int"]).columns:
        low, high = iqr_bounds(col)
        df = df[(df[col] >= low) & (df[col] <= high)]
    current_shape = df.shape
    print(f'Shape after removing outliers: {current_shape}')
    if previous_shape == current_shape:
        break

plt.boxplot(df.select_dtypes(include=["int", "float"]))
plt.title('Boxplot After Outlier Removal')
plt.show()

#Outliear Removing Code Finish ***

