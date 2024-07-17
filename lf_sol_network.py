import requests
import pandas as pd
from datetime import datetime, timedelta
import networkx as nx
from pyvis.network import Network
import base64

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

def generate_network_graph(df):
    G = nx.DiGraph()
    for index, row in df.iterrows():
        if pd.notna(row['SWAP_FROM_SYMBOL']) and pd.notna(row['SWAP_TO_SYMBOL']):
            G.add_edge(row['SWAP_FROM_SYMBOL'], row['SWAP_TO_SYMBOL'], weight=row['TOTAL_SWAP_FROM_AMOUNT_USD'])
    
    net = Network(notebook=False)
    net.from_nx(G)
    net.show("swap_network.html")
    
    with open("swap_network.html", "r") as file:
        html_content = file.read()
        
    return base64.b64encode(html_content.encode('utf-8')).decode('utf-8')

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
    
    network_graph_html = generate_network_graph(filtered_df)
    
    return {
        'statusCode': 200,
        'body': {
            'filtered_data': filtered_df.to_dict(orient='records'),
            'unique_traders': unique_traders.to_dict(orient='records'),
            'network_graph_html': network_graph_html
        }
    }
