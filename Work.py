import pandas as pd
from sklearn import preprocessing
import matplotlib.pyplot as plt
import seaborn as sns

data=pd.read_excel("mobile sales dataset.xlsx")
print(data.isnull().sum())

data.columns=data.columns.str.replace(' ', '_')
dataCopy=data.copy()
num=dataCopy.select_dtypes(include=["float","int"]).columns

dataCopy["Discount_Band"] = dataCopy["Discount_Band"].fillna("No Discount")
dataCopy.to_csv("dat.csv")
for col in ["Discount_Band", "Month_Name"]:
    dummies = pd.get_dummies(dataCopy[col], prefix=col).astype("int")
    dataCopy = dataCopy.join(dummies)
    dataCopy.drop(col, axis=1, inplace=True)

Label=preprocessing.LabelEncoder()
chacter=dataCopy.select_dtypes(include="object").columns
for col in chacter:        
        dataCopy[col] = Label.fit_transform(dataCopy[col])

Result=dataCopy
plt.boxplot(Result.select_dtypes(["float","int"]))
plt.show()


def iqr_bounds(column):
    q1 = Result[column].quantile(0.25)
    q3 = Result[column].quantile(0.75)
    iqr = q3 - q1
    low_bound = q1 - 1.5 * iqr
    high_bound = q3 + 1.5 * iqr
    return [low_bound, high_bound]

while True:
    previous_shape = Result.shape
    for col in Result.select_dtypes(include=["float", "int"]).columns:
        low, high = iqr_bounds(col)
        Result = Result[(Result[col] >= low) & (Result[col] <= high)]
    current_shape = Result.shape
    print(f'Shape after removing Resultliers: {current_shape}')
    if previous_shape == current_shape:
        break

for col in Result.columns:
     if (Result[col]==0).all():
          Result.drop(col,axis=1,inplace=True)
Result.to_csv("Final.csv",index=False)

plt.boxplot(Result.select_dtypes(include=["int", "float"]))
plt.title('Boxplot After Resultlier Removal')
plt.show()
sns.heatmap(Result.corr())
plt.show()

def data_quality(df):
    print("\nData Quality Report:")
    print("-" * 50)
    print(f"Total rows: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    print("\nMissing values:")
    print(df.isnull().sum().sum())
    print("\nData types:")
    print(df.dtypes)
    print("\nBasic statistics:")
    print(df.describe())
data_quality(Result)
print(Result)