import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


data = pd.read_csv("c:\\Python_Practice\\CSV\\ml_ready_data.csv")

correlation_matrix = data.corr()

plt.figure(figsize=(15, 12))
sns.heatmap(correlation_matrix, cmap='coolwarm', annot=False)
plt.title("Correlation Heatmap")
plt.show()

for i in range(50):
    plt.figure(figsize=(8, 6))
    data.iloc[:, i].hist(bins=30, color='skyblue', edgecolor='black')
    plt.title(f"Histogram for Column: {data.columns[i]}")
    plt.xlabel(data.columns[i])
    plt.ylabel("Frequency")
    plt.show()


for i in range(50, 100):
    plt.figure(figsize=(8, 6))
    data.iloc[:, i].hist(bins=30, color='lightgreen', edgecolor='black')
    plt.title(f"Histogram for Column: {data.columns[i]}")
    plt.xlabel(data.columns[i])
    plt.ylabel("Frequency")
    plt.show()


for i in range(100,150):
    plt.figure(figsize=(8, 6))
    data.iloc[:, i].hist(bins=30, color='blue', edgecolor='black')
    plt.title(f"Histogram for Column: {data.columns[i]}")
    plt.xlabel(data.columns[i])
    plt.ylabel("Frequency")
    plt.show()





for i in range(50):
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=data.iloc[:, i])
    plt.title(f"Boxplot for Column: {data.columns[i]}")
    plt.show()

for i in range(50, 100):
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=data.iloc[:, i])
    plt.title(f"Boxplot for Column: {data.columns[i]}")
    plt.show()

for i in range(100,150):
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=data.iloc[:, i])
    plt.title(f"Boxplot for Column: {data.columns[i]}")
    plt.show()
