import pickle
import os

def read_pickle_data(file_name):
    if not os.path.exists(file_name):
        return f"Error: File '{file_name}' not found."

    try:
        with open(file_name, 'rb') as file:
            data = pickle.load(file)
    except (pickle.UnpicklingError, EOFError) as e:
        return f"Error reading pickle file: {e}"

    if isinstance(data, dict):
        print("Available keys:", list(data.keys()))
        key = input("Enter a key: ")
        return data.get(key, "Key not found")

    elif isinstance(data, list):
        print(f"List has {len(data)} items")
        try:
            index = int(input("Enter a number: "))
            return data[index] if 0 <= index < len(data) else "Index out of range"
        except ValueError:
            return "Invalid index input"

    else:
        return data

file_name = 'Germination_stage.pkl'
print("Result:", read_pickle_data(file_name))
