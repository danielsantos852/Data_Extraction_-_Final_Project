# Import libraries
import requests as re
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Input parameters
api_key = '10940c331d2a782a7a7181a67d0c9b9b'
query = 'airsoft'
api_url = f'https://gnews.io/api/v4/search?q={query}&apikey={api_key}'


# Main function
def main() -> None:
    
    ''' Main function. '''
    
    # Get response from API
    #response = get_api_response(url=api_url)
    
    # [DEBUG] Get mock response
    response = get_mock_response()
    
    # [DEBUG] Export response as JSON file
    #export_json(d=response, filename='mock')

    # Generate dataframe from response
    data = generate_dataframe(r=response)

    # [DEBUG] Display dataframe
    #display(data)
    
    # Save dataframe as parquet file
    save_as_parquet(d=data, filename='raw')


# Get API Response function
def get_api_response(url:str) -> dict:
    
    ''' Takes API's URL as input parameter. 
        Returns API's response as output. '''
    
    # Get response from API
    r = re.get(url, params={#'lang':'pt',
                            'country':'br',
                            #'max':10,
                            #'in':'title,description,content',
                            #'nullable':'content',
                            #'from':'2022-01-01T00:00:00Z',      # e.g. 2022-08-21T16:27:09Z
                            #'to':'2023-03-08T00:00:00Z',
                            'sortby':'publishedAt',})            # 'relevance' or 'publishedAt'
    
    # Get response content (as bytes class)
    r = r.content
    
    # Decode response content (bytes to str)
    r = r.decode(encoding='utf-8')
    
    # Convert response content (str to dict)
    r = json.loads(r)
    
    # Return response content
    return r


# Get Mock Response function
def get_mock_response() -> dict:
    
    ''' Returns a mock response for delevoping purposes. '''
    
    # Load response data from mock JSON file
    with open('./data/mock.json', 'r', encoding='utf-8') as file:
        r = json.load(file)
    
    # Return response
    return r


# Export JSON function
def export_json(d:dict, filename:str) -> None:
    
    ''' Takes dictionary and filename as input parameters. 
        Exports dictionary as json file with provided filename. 
        Returns nothing. '''
    
    # Export dictionary as JSON file
    with open(f'./data/{filename}.json', 'w', encoding='utf-8') as file:
        json.dump(obj=d, fp=file, ensure_ascii=False, indent=4)


# Generate Dataframe function
def generate_dataframe(r:dict) -> pd.core.frame.DataFrame:
    
    ''' Takes API's response as input parameter.
        Returns pandas dataframe with response's data as output. '''
    
    # Create empty, main dataframe
    df = pd.DataFrame()

    # For each article (dict) in response['articles'] (list) :
    for article in r['articles']:

        # Remove article['source'] (nested dict) from article
        source = article.pop('source')

        # Add 'source's values as values to article
        article['source_name'] = source['name']
        article['source_url'] = source['url']

        # Create temporary, single-row dataframe from article data
        temp = pd.DataFrame(data=article, index=[0])

        # Concatenate main and temp dataframes
        df = pd.concat(objs=[df, temp],
                axis=0,
                ignore_index=True)
    
    # Return main dataframe
    return df


# Save As Parquet function
def save_as_parquet(d:pd.core.frame.DataFrame, filename:str) -> None:
    
    ''' Takes dataframe as input. 
        Saves dataframe as parquet file.
        Returns nothing. '''
    
    # Create pyarrow table from pandas dataframe
    table = pa.Table.from_pandas(d)
    
    # Create parquet file
    pqwriter = pq.ParquetWriter(f'./data/{filename}.parquet', table.schema)
    
    # Add table to parquet file
    pqwriter.write_table(table)
    
    # Close parquet file
    pqwriter.close()


# If this is the script being run:
if __name__=='__main__':
    
    # Call main function
    main()