import pandas as pd
import matplotlib.pyplot as plt
from sklearn import preprocessing
import joblib

def preprocess(df):
    df = pd.read_csv('germination_stage_export.csv')
    df['Date'] = pd.to_datetime(df['Date'])
    df1 = df.copy()
    df1['Day'] = df['Date'].dt.day
    df1['Month'] = df['Date'].dt.month
    df1['Year'] = df['Date'].dt.year
    df1.drop(columns="Date",axis=1,inplace=True)
    column_order = ['Day', 'Month', 'Year'] + [col for col in df1.columns if col not in ['Day', 'Month', 'Year']]
    df1 = df1[column_order]

    cat = df1.select_dtypes(include="object")
    num=df1.select_dtypes(include=["float","int"])

    Label=preprocessing.LabelEncoder()
    for col in cat.columns:        
        df1[col] = Label.fit_transform(df1[col])

    plt.boxplot(df1)
    plt.title('Boxplot Before Outlier removal') 
    plt.show()

    def iqr_bounds(column):
        q1 = df1[column].quantile(0.25)
        q3 = df1[column].quantile(0.75)
        iqr = q3 - q1
        low_bound = q1 - 1.5 * iqr
        high_bound = q3 + 1.5 * iqr
        return [low_bound, high_bound]

    while True:
        previous_shape = df1.shape
        for col in df1.select_dtypes(include=["float", "int"]).columns:
            low, high = iqr_bounds(col)
            df1 = df1[(df1[col] >= low) & (df1[col] <= high)]
        current_shape = df1.shape
        print(f'Shape after removing df1liers: {current_shape}')
        if previous_shape == current_shape:
            break

    plt.boxplot(df1)
    plt.title('Boxplot After Outlier removal') 
    plt.show()

    joblib.dump(df1,"Germination_stage.pkl")
    user=int(input(""))



preprocess("")