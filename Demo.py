import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
data=pd.read_csv("c:\\Python_Practice\\CSV\\ml_ready_data.csv")
sns.set(style="whitegrid")

numerical_data = data.select_dtypes(include=['float64', 'int64'])
plt.figure(figsize=(15, 10))
correlation_matrix = numerical_data.corr()
sns.heatmap(correlation_matrix, cmap="coolwarm", annot=False, fmt='.2f')
plt.title("Correlation Heatmap")
plt.show()


plt.figure(figsize=(10, 6))
sns.boxplot(data=data, x='Rice_Sowing_Crop Variety', y='Rice_Sowing_Duration')
plt.title("Rice Sowing Duration by Crop Variety")
plt.xlabel("Crop Variety")
plt.ylabel("Sowing Duration")
plt.show()


plt.figure(figsize=(10, 6))
sns.histplot(data['Rice_Sowing_Nitrogen Availability'])
plt.title("Distribution of Rice Sowing Nitrogen Availability")
plt.xlabel("Nitrogen Availability")
plt.ylabel("Frequency")
plt.show()


plt.figure(figsize=(10, 6))
data['Rice_Sowing_Season'].value_counts().plot(kind='bar', color='coral')
plt.title("Rice Sowing Season Distribution")
plt.xlabel("Season")
plt.ylabel("Frequency")
plt.show()


if 'Rice_Sowing_Temperature' in data.columns and 'Rice_Sowing_Plant Density' in data.columns:
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=data, x='Rice_Sowing_Temperature', y='Rice_Sowing_Plant Density', hue='Rice_Sowing_Crop Variety')
    plt.title("Temperature vs. Plant Density")
    plt.xlabel("Temperature")
    plt.ylabel("Plant Density")
    plt.legend(title="Crop Variety")
    plt.show()
else:
    print("Temperature or Yield data not found in the dataset.")
