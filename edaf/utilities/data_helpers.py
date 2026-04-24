import json

import numpy as np
import pandas as pd


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # Handle NaN values
        if isinstance(obj, float) and math.isnan(obj):
            return None
        # Convert numpy int64 and float64 to regular Python int and float
        if isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        # Handle Pandas Series by converting to dictionary
        if isinstance(obj, pd.Series):
            return obj.to_dict()
        # Format scientific notation numbers for readability
        if isinstance(obj, float):
            return format(obj, '.6f')
        return super(CustomJSONEncoder, self).default(obj)

def create_json_dump(data_list, file_name):
    """
    Takes a list of data and writes it as a JSON file after handling NaN values,
    avoiding circular references, converting NumPy data types, and handling Pandas Series.
    
    :param data_list: List containing entries
    :param file_name: Name of the JSON file to create
    """
    try:
        with open(file_name, 'w') as json_file:
            json.dump(data_list, json_file, indent=4, cls=CustomJSONEncoder)
    except ValueError as e:
        print(f"Error creating JSON dump: {e}")


def read_json(file_name):
    """
    Reads a JSON file and loads it into a Python dictionary.
    
    :param file_name: Name of the JSON file to read
    :return: A dictionary containing the data from the JSON file
    """
    try:
        with open(file_name, 'r') as json_file:
            data = json.load(json_file)
        return data
    except ValueError as e:
        print(f"Error reading JSON file: {e}")
        return None