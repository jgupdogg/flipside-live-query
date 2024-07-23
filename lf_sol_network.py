import requests
import json
import logging

# Update the URL to point to your new query
url = "https://flipsidecrypto.xyz/api/v1/queries/1a10bea8-8307-417b-af3d-57327b93db7d/data/latest"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def fetch_flipside_crypto_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to fetch data. HTTP Status code: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None

def transform_data_to_cytoscape_format(data):
    nodes = {}
    edges = []
    
    for record in data:
        from_address = record['ORIGIN_FROM_ADDRESS']
        to_address = record['ORIGIN_TO_ADDRESS']
        tx_hash = record['TX_HASH']
        raw_amount = record['RAW_AMOUNT']
        from_label = record.get('FROM_LABEL', from_address)
        to_label = record.get('TO_LABEL', to_address)
        price = record.get('PRICE')
        usd_amount = record.get('USD_AMOUNT')
        symbol = record.get('SYMBOL')
        contract_address = record.get('CONTRACT_ADDRESS')

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
    try:
        logger.info("Event: %s", json.dumps(event))
        
        # Fetch data from Flipside Crypto
        data = fetch_flipside_crypto_data(url)
        if data is None:
            return {
                'statusCode': 500,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
                },
                'body': json.dumps({
                    'error': 'Failed to fetch data from Flipside Crypto'
                })
            }
        
        # Transform data to Cytoscape format
        elements = transform_data_to_cytoscape_format(data)
        
        # Return the transformed data
        result = {
            "message": "Processed successfully!",
            "data": elements
        }
        
        logger.info("Result: %s", json.dumps(result))
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization'
            },
            'body': json.dumps(result)
        }
    except Exception as e:
        logger.error("Error: %s", str(e))
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization'
            },
            'body': json.dumps({
                'error': str(e)
            })
        }
