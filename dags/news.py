# Import libraries
import requests as re
import json
import time
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Input parameters
queries = [
    {
        'query':'aborto',
        'language':'pt',
        'country':'br',
    },
    {
        'query':'abortion',
        'language':'en',
        'country':'us',
    },
]

# API parameters
api_key = '10940c331d2a782a7a7181a67d0c9b9b'
time_between_retries = 10
max_retries = 3

# Export file parameters
filename_output = '1.raw'


# Main function
def main() -> None:
    
    ''' Main function. '''
    
    # Create empty dataframe
    data = pd.DataFrame()

    # For each specified query:
    for query in queries:

        # Set API URL
        api_url = f"https://gnews.io/api/v4/search?q={query['query']}&apikey={api_key}"

        # Start retry counter
        retries = 0

        # Start infinite loop
        while True:
            
            # Get response from API
            response, response_code = get_api_response(url=api_url,
                                                       lang=query['language'],
                                                       country=query['country'])
            
            # If Error 500:
            if response_code == 500:
                
                # Print error message
                print('ERROR 500: Internal API Server Error. ', end='')
            
            # Else, if Error 503:
            elif response_code == 503:
                
                # Print error message
                print(f'ERROR 503: Service Unavailable. ', end='')

            # Else:
            else:
            
                # Break from the infinite loop
                break                
            
            # Update retry counter
            retries += 1
            
            # If max retries reached:
            if retries > max_retries:
                
                # Exit program with "max retries reached" message
                sys.exit('Maximum amount of retries reached. Stopping program...')
            
            # Print retry message
            print(f'Retrying in {time_between_retries} seconds. (Retry {retries} of {max_retries})')

            # Wait for [time_between_retries] seconds
            time.sleep(time_between_retries)
        
        # [DEBUG] Export response as JSON file
        #export_json(d=response, filename=f"mock_{query['query']}_{query['language']}_{query['country']}")

        # Generate temp dataframe from response
        temp = generate_dataframe(r=response)

        # Concatenate data and temp dataframes
        data = pd.concat(objs=[data, temp],
                         axis=0,
                         ignore_index=True)
    
    # Save dataframe to parquet file
    save_as_parquet(df=data, filename=filename_output)


# Get API Response function
def get_api_response(url:str, lang:str, country:str) -> dict:
    
    ''' Takes API's URL as input parameter. 
        Returns API's response as output. '''
    
    # Get response from API
    r = re.get(url, params={'lang':lang,
                            'country':country,
                            #'max':10,
                            'in':'title,description',            # 'title, description, content'
                            #'nullable':'content',
                            #'from':'2022-01-01T00:00:00Z',      # e.g. 2022-08-21T16:27:09Z
                            #'to':'2023-03-08T00:00:00Z',
                            'sortby':'publishedAt',})            # 'relevance' or 'publishedAt'
    
    # Get response code from API response
    r_code = r.status_code
    
    # Get response content (as bytes class)
    r = r.content
    
    # Decode response content (bytes to str)
    r = r.decode(encoding='utf-8')
    
    # Convert response content (str to dict)
    r = json.loads(r)
    
    # Return response content
    return r, r_code


# Get Mock Response function
def get_mock_response() -> dict:
    
    ''' Returns a mock response (for delevoping purposes). '''
    
    # Load response data from mock JSON file
    with open('/data/mock.json', 'r', encoding='utf-8') as file:
        r = json.load(file)
    
    # Return response
    return r


# Export JSON function
def export_json(d:dict, filename:str) -> None:
    
    ''' Takes dictionary and filename as input parameters. 
        Exports dictionary as json file with provided filename. 
        Returns nothing. '''
    
    # Export dictionary as JSON file
    with open(f'/data/{filename}.json', 'w', encoding='utf-8') as file:
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
def save_as_parquet(df:pd.core.frame.DataFrame, filename:str) -> None:
    
    ''' Takes dataframe as input. 
        Export dataframe as parquet file.
        Returns nothing. '''
    
    # Create pyarrow table from pandas dataframe
    table = pa.Table.from_pandas(df)
    
    # Create parquet file
    pqwriter = pq.ParquetWriter(f'./dags/data/{filename}.parquet', table.schema)
    
    # Add table to parquet file
    pqwriter.write_table(table)
    
    # Close parquet file
    pqwriter.close()


# If this is the script being run:
if __name__=='__main__':
    
    # Call main function
    main()