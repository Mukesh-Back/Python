import pickle
import os

def access_pickle_data():
    file_name = input("Enter the filename to access data (e.g., Germination_stage.pkl): ")
    
    if not os.path.exists(file_name):
        print(f"Error: File '{file_name}' not found.")
        return
    
    try:

        with open(file_name, "rb") as file:
            data = pickle.load(file)

        print(f"Data loaded successfully from {file_name}.")
        print(f"Data type: {type(data).__name__}")
        
        # Process data based on its type
        if isinstance(data, dict):
            print("Available keys:", list(data.keys()))
            key = input("Enter a key to view its value: ")
            print(f"Value: {data.get(key, 'Key not found')}")
        elif isinstance(data, list):
            print(f"The list has {len(data)} items.")
            index = int(input("Enter an index to view its value: "))
            if 0 <= index < len(data):
                print(f"Value at index {index}: {data[index]}")
            else:
                print("Index out of range.")
        else:
            print("Data:", data)
    except Exception as e:
        print(f"An error occurred while accessing the file: {e}")

access_pickle_data()
