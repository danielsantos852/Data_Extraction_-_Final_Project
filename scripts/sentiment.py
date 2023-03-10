# Import libraries
import requests as rq
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Input parameters
filename = 'raw'
api_key = '178ed1ae297a85cb88392b3d3174d30b'


# Main function
def main():
    
    # Load dataframe from parquet file
    data = load_parquet(file=filename)
    
    


# Load Parquet function
def load_parquet(file:str) -> pd.core.frame.DataFrame:

    ''' Takes a filename as input parameter.
        Returns pandas dataframe as output. '''

    # Read parquet file as pyarrow table
    table = pq.read_table(f'../data/{filename}.parquet')

    # Convert pyarrow table to pandas dataframe
    df = table.to_pandas()

    # Return dataframe
    return df