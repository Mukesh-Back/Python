import pandas as pd

def diagnose_outliers(df):
    outlier_info = {}
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
    
    for column in numeric_columns:
        col_data = df[column]
        Q1 = col_data.quantile(0.25)
        Q3 = col_data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = col_data[(col_data < lower_bound) | (col_data > upper_bound)]
        
        outlier_info[column] = {
            'total_values': len(col_data),
            'outliers_count': len(outliers),
            'outliers_percentage': (len(outliers) / len(col_data)) * 100,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'min_value': col_data.min(),
            'max_value': col_data.max(),
            'mean': col_data.mean(),
            'median': col_data.median()
        }
    
    return outlier_info

def remove_outliers(df):
    df_cleaned = df.copy()
    numeric_columns = df_cleaned.select_dtypes(include=['float64', 'int64']).columns
    
    for column in numeric_columns:
        Q1 = df_cleaned[column].quantile(0.25)
        Q3 = df_cleaned[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        print(f"Processing column: {column}")
        print(f"Lower bound: {lower_bound}, Upper bound: {upper_bound}")
        
        mask = (df_cleaned[column] >= lower_bound) & (df_cleaned[column] <= upper_bound)
        print(f"Rows before filtering: {df_cleaned.shape[0]}")
        print(f"Number of outliers identified: {len(df_cleaned) - mask.sum()}")
        
        if mask.sum() > 0:
            df_cleaned = df_cleaned[mask]
        
        print(f"Rows after filtering: {df_cleaned.shape[0]}")
        print()
    
    return df_cleaned

def main():
    file_path = "c:\\Users\\visualapp\\Music\\Results\\data.csv"
    
    try:
        data = pd.read_csv(file_path)
        
        for col in data.columns:
            if data[col].dtype == 'object':
                data[col] = data[col].astype(str)
            else:
                data[col] = pd.to_numeric(data[col], errors='coerce')
        
        data.dropna(how='all', inplace=True)
        
        outlier_diagnosis = diagnose_outliers(data)
        
        print("Outlier Diagnosis:")
        for column, info in outlier_diagnosis.items():
            print(f"\nColumn: {column}")
            for key, value in info.items():
                print(f"{key}: {value}")
        
        data_cleaned = remove_outliers(data)
        
        for col in data.columns:
            if data[col].dtype == 'object':
                data_cleaned[col] = data[col]
        
        data_cleaned.to_csv("c:\\Users\\visualapp\\Music\\Results\\data_cleaned.csv", index=False)
        
        print("\nDataset Information:")
        print(f"Original dataset shape: {data.shape}")
        print(f"Cleaned dataset shape: {data_cleaned.shape}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
