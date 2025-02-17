import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer

def convert_only_character_columns(df):
    categorical_columns = df.select_dtypes(include=['object']).columns
    numerical_columns = df.select_dtypes(include=['int64', 'float64']).columns

    if len(categorical_columns) == 0:
        return df
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('onehot', 
             OneHotEncoder(
                 sparse_output=False, 
                 handle_unknown='ignore', 
                 max_categories=10 
             ), 
             categorical_columns)
        ],
        remainder='passthrough' )
    
    encoded_array = preprocessor.fit_transform(df)
    
    
    try:
        onehot_columns = preprocessor.named_transformers_['onehot'].get_feature_names_out(categorical_columns)
    except AttributeError:
        onehot_columns = [f"{col}_{cat}" for col in categorical_columns 
                           for cat in preprocessor.named_transformers_['onehot'].categories_[list(categorical_columns).index(col)]]
    
    passthrough_columns = list(df.columns.drop(categorical_columns))
    
    all_columns = list(onehot_columns) + passthrough_columns
    
    if len(all_columns) != encoded_array.shape[1]:
        print(f"Warning: Column mismatch. Expected {len(all_columns)} columns, got {encoded_array.shape[1]} columns.")
        if len(all_columns) > encoded_array.shape[1]:
            all_columns = all_columns[:encoded_array.shape[1]]
        else:
            all_columns.extend([f'extra_col_{i}' for i in range(encoded_array.shape[1] - len(all_columns))])
    
    df_encoded = pd.DataFrame(
        encoded_array, 
        columns=all_columns[:encoded_array.shape[1]], 
        index=df.index
    )
    
    return df_encoded, preprocessor

def main():
    file_path = r"c:\\Users\\visualapp\\Music\\Results\\data_cleaned.csv"
    
    try:
        data = pd.read_csv(file_path)
        
        ml_ready_data, preprocessor = convert_only_character_columns(data)
        
        # Print column types before and after conversion
        print("Columns before conversion:")
        print(data.dtypes)
        print("\nColumns after conversion:")
        print(ml_ready_data.dtypes)
        
        ml_ready_data.to_csv(r"c:\\Users\\visualapp\\Music\\Results\\ml_ready_data1.csv", index=False)
    
        print("\nDataset Information:")
        print(f"Original dataset shape: {data.shape}")
        print(f"ML-ready dataset shape: {ml_ready_data.shape}")        

        categorical_columns = data.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            print(f"\nOne-Hot Encoded Categories for '{col}':")

            try:
                categories = preprocessor.named_transformers_['onehot'].categories_[list(categorical_columns).index(col)]
                for category in categories:
                    print(f"{category}")
            except Exception as cat_error:
                print(f"Could not retrieve categories: {cat_error}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()