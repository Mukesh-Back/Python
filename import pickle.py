import pandas as pd
import pickle

def find_row_by_calcium(df, calcium_value, column='Sodium'):
    matching_row = df[df[column] == calcium_value]
    
    if not matching_row.empty:
        return matching_row
    else:
        df['Calcium_diff'] = (df[column] - calcium_value).abs()
        closest_row = df.loc[df['Calcium_diff'].idxmin()]
        return closest_row

def get_closest_row(file_path, calcium_value, column='Sodium'):
    df = pickle.load(open(file_path, 'rb'))
    result = find_row_by_calcium(df, calcium_value, column)
    
    return result

Req_input = float(input("Enter Calcium value: "))

file_path = 'Germination.pkl' 


result = get_closest_row(file_path, Req_input)

if isinstance(result, pd.DataFrame):
    print(result)  
else:
    print(f"Closest match: {result}")  
