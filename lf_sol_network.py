import json
import logging
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PREDEFINED_COLORS = [
    "#66b3ff", "#ff6666", "#6a0dad", "#ff8c00", "#ffd700",
    "#adff2f", "#20b2aa", "#ff6347", "#8a2be2", "#00ced1"
]

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

def get_color(label_type, label_type_colors):
    if label_type not in label_type_colors:
        color_index = len(label_type_colors) % len(PREDEFINED_COLORS)
        label_type_colors[label_type] = PREDEFINED_COLORS[color_index]
    return label_type_colors[label_type]

def normalize(value, min_value, max_value):
    if max_value == min_value:
        return 1
    return (value - min_value) / (max_value - min_value)

def create_or_update_node(nodes, label, label_type, total_amount, transaction_count, usd_amount, label_type_colors):
    color = get_color(label_type, label_type_colors)
    if label not in nodes:
        nodes[label] = {
            'data': {
                'id': label,
                'label': label,
                'label_type': label_type,
                'total_transacted': total_amount,
                'transaction_count': transaction_count,
                'total_usd_amount': usd_amount,
                'background_color': color,
                'width': 10 + total_amount ** 0.5 * 20,
                'height': 10 + total_amount ** 0.5 * 20,
                'opacity': max(0.1, transaction_count / 100)
            }
        }
    else:
        nodes[label]['data']['total_transacted'] += total_amount
        nodes[label]['data']['transaction_count'] += transaction_count
        nodes[label]['data']['total_usd_amount'] += usd_amount
        nodes[label]['data']['width'] = 10 + nodes[label]['data']['total_transacted'] ** 0.5 * 20
        nodes[label]['data']['height'] = 10 + nodes[label]['data']['total_transacted'] ** 0.5 * 20
        nodes[label]['data']['opacity'] = max(0.1, nodes[label]['data']['transaction_count'] / 100)

def transform_data_to_cytoscape_format(data):
    nodes = {}
    edges = []

    for record in data:
        from_label = record['FROM_LABEL']
        from_label_type = record.get('FROM_LABEL_TYPE', 'Unknown')  # Use 'Unknown' if type is not specified
        to_label = record['TO_LABEL']
        to_label_type = record.get('TO_LABEL_TYPE', 'Unknown')  # Use 'Unknown' if type is not specified
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
                    'label_type': from_label_type,
                    'total_transacted': total_amount,
                    'transaction_count': transaction_count,
                    'total_usd_amount': usd_amount,
                }
            }
        else:
            nodes[from_label]['data']['total_transacted'] += total_amount
            nodes[from_label]['data']['transaction_count'] += transaction_count
            nodes[from_label]['data']['total_usd_amount'] += usd_amount

        # Update node data or create new if not exists for TO_LABEL
        if to_label not in nodes:
            nodes[to_label] = {
                'data': {
                    'id': to_label,
                    'label': to_label,
                    'label_type': to_label_type,
                    'total_transacted': total_amount,
                    'transaction_count': transaction_count,
                    'total_usd_amount': usd_amount,
                }
            }
        else:
            nodes[to_label]['data']['total_transacted'] += total_amount
            nodes[to_label]['data']['transaction_count'] += transaction_count
            nodes[to_label]['data']['total_usd_amount'] += usd_amount

        logger.info(f"Adding edge with usd_amount: {usd_amount}")  # Debug logging
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
