import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

def convert_categorical_to_numeric(df):

    df_encoded = df.copy()
    
    categorical_columns = df_encoded.select_dtypes(include=['object']).columns
    for col in categorical_columns:
        le = LabelEncoder()
        df_encoded[col] = le.fit_transform(df_encoded[col].astype(str).fillna('Unknown'))
    
    return df_encoded

def main():
    file_path = r"c:\\Users\\visualapp\\Music\\Results\\data.csv"
    
    try:
        data = pd.read_csv(file_path)
        ml_ready_data = convert_categorical_to_numeric(data)
        print("Columns before conversion:")
        print(data.dtypes)
        print("\nColumns after conversion:")
        print(ml_ready_data.dtypes)
        
        ml_ready_data.to_csv(r"c:\\Users\\visualapp\\Music\\Results\\ml_ready_data.csv", index=False)
        
        print("\nDataset Information:")
        print(f"Original dataset shape: {data.shape}")
        print(f"ML-ready dataset shape: {ml_ready_data.shape}")
        
        categorical_columns = data.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            print(f"\nUnique mappings for '{col}':")
            le = LabelEncoder()
            le.fit(data[col].astype(str).fillna('Unknown'))
            for original, encoded in zip(le.classes_, range(len(le.classes_))):
                print(f"{original} , {encoded}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()