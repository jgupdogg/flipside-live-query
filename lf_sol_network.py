import json
import logging
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

url = "https://flipsidecrypto.xyz/api/v1/queries/1a10bea8-8307-417b-af3d-57327b93db7d/data/latest"

def fetch_data(url):
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
        from_label = record['FROM_LABEL']
        to_label = record['TO_LABEL']
        tx_hash = f"{from_label}_to_{to_label}"
        total_amount = record['TOTAL_AMOUNT']
        symbol = record['SYMBOL']
        usd_amount = record['TOTAL_AMOUNT_USD']
        transaction_count = record['TRANSACTION_COUNT']

        # Update node data or create new if not exists for FROM_LABEL
        if from_label not in nodes:
            nodes[from_label] = {
                'data': {
                    'id': from_label,
                    'label': from_label,
                    'total_transacted': total_amount,
                    'transaction_count': transaction_count
                }
            }
        else:
            nodes[from_label]['data']['total_transacted'] += total_amount
            nodes[from_label]['data']['transaction_count'] += transaction_count

        # Update node data or create new if not exists for TO_LABEL
        if to_label not in nodes:
            nodes[to_label] = {
                'data': {
                    'id': to_label,
                    'label': to_label,
                    'total_transacted': total_amount,
                    'transaction_count': transaction_count
                }
            }
        else:
            nodes[to_label]['data']['total_transacted'] += total_amount
            nodes[to_label]['data']['transaction_count'] += transaction_count

        edges.append({
            'data': {
                'id': tx_hash,
                'source': from_label,
                'target': to_label,
                'total_amount': total_amount,
                'usd_amount': usd_amount,
                'symbol': symbol,
                'transaction_count': transaction_count
            }
        })

    elements = list(nodes.values()) + edges
    return elements
    nodes = {}
    edges = []

    for record in data:
        from_label = record['FROM_LABEL']
        to_label = record['TO_LABEL']
        tx_hash = f"{from_label}_to_{to_label}"
        total_amount = record['TOTAL_AMOUNT']
        symbol = record['SYMBOL']
        usd_amount = record['TOTAL_AMOUNT_USD']
        transaction_count = record['TRANSACTION_COUNT']

        if from_label not in nodes:
            nodes[from_label] = {'data': {'id': from_label, 'label': from_label}}
        if to_label not in nodes:
            nodes[to_label] = {'data': {'id': to_label, 'label': to_label}}

        edges.append({
            'data': {
                'id': tx_hash,
                'source': from_label,
                'target': to_label,
                'total_amount': total_amount,
                'usd_amount': usd_amount,
                'symbol': symbol,
                'transaction_count': transaction_count
            }
        })

    elements = list(nodes.values()) + edges
    return elements

def lambda_handler(event, context):
    try:
        logger.info("Event: %s", json.dumps(event))

        data = fetch_data(url)
        if data is None:
            return {
                'statusCode': 500,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
                },
                'body': json.dumps({
                    'error': 'Failed to fetch data'
                })
            }

        elements = transform_data_to_cytoscape_format(data)

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
