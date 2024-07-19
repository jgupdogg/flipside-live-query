import requests
import pandas as pd
from datetime import datetime, timedelta

# Update the URL to point to your new query
url = "https://flipsidecrypto.xyz/api/v1/queries/1a10bea8-8307-417b-af3d-57327b93db7d/data/latest"

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

def transform_data_to_cytoscape_format(data):
    nodes = {}
    edges = []
    
    for record in data:
        from_address = record['ORIGIN_FROM_ADDRESS']
        to_address = record['ORIGIN_TO_ADDRESS']
        tx_hash = record['TX_HASH']
        raw_amount = record['RAW_AMOUNT']
        from_label = record['FROM_LABEL'] if 'FROM_LABEL' in record else from_address
        to_label = record['TO_LABEL'] if 'TO_LABEL' in record else to_address
        price = record['PRICE'] if 'PRICE' in record else None
        usd_amount = record['USD_AMOUNT'] if 'USD_AMOUNT' in record else None
        symbol = record['SYMBOL'] if 'SYMBOL' in record else None
        contract_address = record['CONTRACT_ADDRESS'] if 'CONTRACT_ADDRESS' in record else None

        # Add nodes if they don't exist
        if from_address not in nodes:
            nodes[from_address] = {'data': {'id': from_address, 'label': from_label}}
        if to_address not in nodes:
            nodes[to_address] = {'data': {'id': to_address, 'label': to_label}}
        
        # Add edge
        edges.append({
            'data': {
                'id': tx_hash,
                'source': from_address,
                'target': to_address,
                'raw_amount': raw_amount,
                'price': price,
                'usd_amount': usd_amount,
                'symbol': symbol,
                'contract_address': contract_address
            }
        })
    
    # Combine nodes and edges
    elements = list(nodes.values()) + edges
    return elements

def lambda_handler(event, context):
    data = fetch_flipside_crypto_data(url)
    if not data:
        return {
            'statusCode': 500,
            'body': 'Failed to fetch data'
        }
    
    # Transform data
    elements = transform_data_to_cytoscape_format(data)
    
    return {
        'statusCode': 200,
        'body': elements,
        'headers': {
            'Content-Type': 'application/json'
        }
    }
