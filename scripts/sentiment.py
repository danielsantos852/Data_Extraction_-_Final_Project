# Import libraries
import requests as re
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Input parameters
api_key = '178ed1ae297a85cb88392b3d3174d30b'
api_url = 'https://api.meaningcloud.com/sentiment-2.1'
filename_input = '1.raw'
filename_output = '2.sentiment'


# Main function
def main() -> None:

    ''' Main function. '''
    
    # Load raw data to be analyzed
    data = load_parquet(filename=filename_input)
    
    # [DEBUG] Display dataframe
    #display(data)
    
    # Perform sentiment analysis on data
    data = sentiment_analysis(df=data, 
                              url=api_url, 
                              key=api_key)
    
    # [DEBUG] Display updated dataframe
    #display(data)

    # Save dataframe to parquet file
    save_as_parquet(df=data, 
                    filename=filename_output)


# Load Parquet function
def load_parquet(filename:str) -> pd.core.frame.DataFrame:

    ''' Takes a filename as input parameter.
        Returns pandas dataframe as output. '''

    # Read parquet file as pyarrow table
    table = pq.read_table(f'./data/{filename}.parquet')

    # Convert pyarrow table to pandas dataframe
    df = table.to_pandas()

    # Return dataframe
    return df


# Get Sentiment function
def sentiment_analysis(df:pd.core.frame.DataFrame, url:str, key:str) -> pd.core.frame.DataFrame:
    
    ''' Takes news dataframe, API url and API key as input parameters. 
        Returns a dataframe with new column(s) as output. '''
    
    # Create empty score_tag list
    score_tag = []

    # Create empty agreement list
    agreement = []
    
    # Create empty irony list
    irony = []

    # For each row (article) in dataframe:
    for row in range(len(df)):
        
        # Get response from API
        r = get_API_response(txt=df.loc[row]['title'], 
                             url=url, 
                             key=key)

        # Append response['score_tag'] to score_tag list
        score_tag.append(r['score_tag'])

        # Append response['agreement'] to agreement list
        agreement.append(r['agreement'])

        # Append response['irony'] to irony list
        irony.append(r['irony'])

    # Append score_tag list as column to dataframe
    df['score_tag'] = score_tag

    # Append agreement list as column to dataframe
    df['agreement'] = agreement

    # Append irony list as column to dataframe
    df['irony'] = irony

    # Return updated dataframe
    return df


# Get API response function
def get_API_response(txt:str, url:str, key:str):
    
    ''' Takes text, API url and API key as input parameters. 
        Returns sentiment as output. '''
    
    # Get response from API
    r = re.get(url, params={'key':key,
                            'of':'json', # Output format: 'json' or 'xml'
                            'lang':'auto', # Language of text to analyze
                            'txt':txt, # Text to analyze
                            #'url':'', # URL of the document to analyze
                            'model':'general', # Sentiment model to use
                            'uw':'y', # Deal with unknown words
                            })

    # Get response content (as bytes class)
    r = r.content

    # Decode response content (bytes to str)
    r = r.decode(encoding='utf-8')

    # Convert response content (str to dict)
    r = json.loads(r)

    # [DEBUG] Print response as JSON
    # print(json.dumps(r, indent=4))

    # Return response
    return r


# Save As Parquet function
def save_as_parquet(df:pd.core.frame.DataFrame, filename:str) -> None:
    
    ''' Takes dataframe as input. 
        Export dataframe as parquet file. 
        Returns nothing. '''
    
    # Create pyarrow table from pandas dataframe
    table = pa.Table.from_pandas(df)
    
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