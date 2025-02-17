import pandas as pd
import numpy as np

def diagnose_outliers(df, iqr_multiplier=1.5, z_score_threshold=3):
    """
    Diagnose outliers using both IQR and Z-score methods
    
    Parameters:
    - df: Input DataFrame
    - iqr_multiplier: Multiplier for IQR method (default 1.5)
    - z_score_threshold: Threshold for Z-score method (default 3)
    
    Returns:
    - Dictionary with outlier information for each numeric column
    """
    outlier_info = {}
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
    
    for column in numeric_columns:
        col_data = df[column]
        
        # IQR Method
        Q1 = col_data.quantile(0.25)
        Q3 = col_data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound_iqr = Q1 - (iqr_multiplier * IQR)
        upper_bound_iqr = Q3 + (iqr_multiplier * IQR)
        iqr_outliers = col_data[(col_data < lower_bound_iqr) | (col_data > upper_bound_iqr)]
        
        # Z-Score Method
        mean = col_data.mean()
        std = col_data.std()
        z_scores = np.abs((col_data - mean) / std)
        z_score_outliers = col_data[z_scores > z_score_threshold]
        
        outlier_info[column] = {
            'total_values': len(col_data),
            'iqr_outliers_count': len(iqr_outliers),
            'iqr_outliers_percentage': (len(iqr_outliers) / len(col_data)) * 100,
            'z_score_outliers_count': len(z_score_outliers),
            'z_score_outliers_percentage': (len(z_score_outliers) / len(col_data)) * 100,
            'iqr_lower_bound': lower_bound_iqr,
            'iqr_upper_bound': upper_bound_iqr,
            'min_value': col_data.min(),
            'max_value': col_data.max(),
            'mean': col_data.mean(),
            'median': col_data.median(),
            'std': col_data.std()
        }
    
    return outlier_info

def remove_outliers(df, method='iqr', iqr_multiplier=2.0, z_score_threshold=3, percentile_range=(5, 95)):
    """
    Remove outliers using different methods
    
    Parameters:
    - df: Input DataFrame
    - method: 'iqr', 'z_score', or 'percentile'
    - iqr_multiplier: Multiplier for IQR method
    - z_score_threshold: Threshold for Z-score method
    - percentile_range: Range to keep for percentile method
    
    Returns:
    - DataFrame with outliers removed
    """
    df_cleaned = df.copy()
    numeric_columns = df_cleaned.select_dtypes(include=['float64', 'int64']).columns
    
    for column in numeric_columns:
        col_data = df_cleaned[column]
        
        if method == 'iqr':
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - (iqr_multiplier * IQR)
            upper_bound = Q3 + (iqr_multiplier * IQR)
            mask = (col_data >= lower_bound) & (col_data <= upper_bound)
        
        elif method == 'z_score':
            mean = col_data.mean()
            std = col_data.std()
            z_scores = np.abs((col_data - mean) / std)
            mask = z_scores <= z_score_threshold
        
        elif method == 'percentile':
            lower_percentile, upper_percentile = percentile_range
            lower_bound = col_data.quantile(lower_percentile/100)
            upper_bound = col_data.quantile(upper_percentile/100)
            mask = (col_data >= lower_bound) & (col_data <= upper_bound)
        
        print(f"Processing column: {column}")
        print(f"Rows before filtering: {df_cleaned.shape[0]}")
        print(f"Number of outliers identified: {len(df_cleaned) - mask.sum()}")
        
        df_cleaned = df_cleaned[mask]
        print(f"Rows after filtering: {df_cleaned.shape[0]}")
        print()
    
    return df_cleaned

def main():
    file_path = "c:\\Users\\visualapp\\Music\\Results\\ml_ready_data_with_one-hot.csv"
    
    try:
        # Read data
        data = pd.read_csv(file_path)
        
        # Preprocessing
        for col in data.columns:
            if data[col].dtype == 'object':
                data[col] = data[col].astype(str)
            else:
                data[col] = pd.to_numeric(data[col], errors='coerce')
        
        data.dropna(how='all', inplace=True)
        
        # Diagnose Outliers
        outlier_diagnosis = diagnose_outliers(data, iqr_multiplier=2.0, z_score_threshold=3)
        
        print("Outlier Diagnosis:")
        for column, info in outlier_diagnosis.items():
            print(f"\nColumn: {column}")
            for key, value in info.items():
                print(f"{key}: {value}")
        
        # Remove Outliers with More Flexible Methods
        data_cleaned_iqr = remove_outliers(data, method='iqr', iqr_multiplier=2.0)
        data_cleaned_percentile = remove_outliers(data, method='percentile', percentile_range=(5, 95))
        data_cleaned_z_score = remove_outliers(data, method='z_score', z_score_threshold=3)
        
        # Restore categorical columns
        for col in data.columns:
            if data[col].dtype == 'object':
                data_cleaned_iqr[col] = data[col]
                data_cleaned_percentile[col] = data[col]
                data_cleaned_z_score[col] = data[col]
        
        # Save cleaned datasets
        data_cleaned_iqr.to_csv("c:\\Users\\visualapp\\Music\\Results\\data_cleaned_iqr.csv", index=False)
        data_cleaned_percentile.to_csv("c:\\Users\\visualapp\\Music\\Results\\data_cleaned_percentile.csv", index=False)
        data_cleaned_z_score.to_csv("c:\\Users\\visualapp\\Music\\Results\\data_cleaned_z_score.csv", index=False)
        
        # Print dataset information
        print("\nDataset Information:")
        print(f"Original dataset shape: {data.shape}")
        print(f"IQR cleaned dataset shape: {data_cleaned_iqr.shape}")
        print(f"Percentile cleaned dataset shape: {data_cleaned_percentile.shape}")
        print(f"Z-Score cleaned dataset shape: {data_cleaned_z_score.shape}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
