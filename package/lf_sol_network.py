import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_flipside_crypto_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data. HTTP Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def filter_and_save(df, days):
    df['DAY'] = pd.to_datetime(df['DAY'])
    end_date = df['DAY'].max()
    start_date = end_date - timedelta(days=days)
    filtered_df = df[df['DAY'] >= start_date]
    return filtered_df

def count_unique_traders(filtered_df):
    unique_sellers = filtered_df.groupby('SWAP_FROM_SYMBOL')['SWAPPER'].nunique().reset_index()
    unique_sellers.columns = ['TOKEN', 'UNIQUE_SELLERS']
    unique_buyers = filtered_df.groupby('SWAP_TO_SYMBOL')['SWAPPER'].nunique().reset_index()
    unique_buyers.columns = ['TOKEN', 'UNIQUE_BUYERS']
    unique_traders = pd.merge(unique_sellers, unique_buyers, on='TOKEN', how='outer')
    return unique_traders

def lambda_handler(event, context):
    url = 'https://flipsidecrypto.xyz/api/v1/queries/9723aa06-ecb6-4c5d-9503-44f7b19e09de/data/latest'
    data = fetch_flipside_crypto_data(url)
    if not data:
        return {
            'statusCode': 500,
            'body': 'Failed to fetch data'
        }
    df = pd.DataFrame(data)
    filtered_df = filter_and_save(df, 1)
    unique_traders = count_unique_traders(filtered_df)
    csv_output = filtered_df.to_csv(index=False)
    
    return {
        'statusCode': 200,
        'body': csv_output,
        'headers': {
            'Content-Type': 'text/csv',
            'Content-Disposition': 'attachment; filename="filtered_data.csv"'
        }
    }
