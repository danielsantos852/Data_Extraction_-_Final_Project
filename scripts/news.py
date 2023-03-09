# Import libraries
import requests as re
import json
import pandas as pd

# Input parameters
api_key = '10940c331d2a782a7a7181a67d0c9b9b'
query = 'airsoft'

# Make API's URL
api_url = f'https://gnews.io/api/v4/search?q={query}&apikey={api_key}'


# Main function
def main():
    
    # Get search results from API
    # results = get_api_response(url=api_url).json()

    # Get response from mock json file
    results = json.load(open('../mock2.json'))

    # Create empty (main) pandas dataframe
    df = pd.DataFrame()

    # For each result in search:
    for result in results['articles']:

        # Remove nested dictionary from result dictionary
        source = result.pop('source')
        
        # Add nested dictionary's keys as keys to result dictionary
        result['source_name'] = source['name']
        result['source_url'] = source['url']
        
        # Create temporary dafaframe with current result's data
        temp = pd.DataFrame(data=result, index=[0])
                
        # Concatenate temporary and main dataframes
        df = pd.concat(objs=[df, temp],
                axis=0,
                ignore_index=True)


# Get Response function
def get_api_response(url=api_url):
    
    # Get response from API
    response = re.get(api_url, params={'lang':'pt',
                                       'country':'br',
                                       'max':10,
                                       'in':'title,description,content',
                                       'nullable':'content',
                                       #'from':'2022-01-01T00:00:00Z',      # e.g. 2022-08-21T16:27:09Z
                                       #'to':'2023-03-08T00:00:00Z',
                                       'sortby':'publishedAt',})            # 'relevance' or 'publishedAt'
    
    # Return response's content and HTTP code
    return response.content, response


if __name__=='__main__':
    main()