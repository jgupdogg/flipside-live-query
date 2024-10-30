import json
import logging
import pandas as pd
import networkx as nx
import seaborn as sns
from matplotlib.colors import rgb2hex
from fetch_data import fetch_data
from formatting_utils import create_tripartite_graph
from graph_utils import graph_to_cytoscape_json
from logging_utils import configure_logging

logger = configure_logging()

def lambda_handler(event, context):
    try:
        logger.info("Event: %s", json.dumps(event))

        url = 'https://flipsidecrypto.xyz/api/v1/queries/1a10bea8-8307-417b-af3d-57327b93db7d/data/latest'

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
        
        tripartite_graph, label_color_dict, balance_group_df = create_tripartite_graph(data)

        projected_cytoscape_json = graph_to_cytoscape_json(tripartite_graph)

        result = {
            "message": "Processed successfully!",
            "data_projected": projected_cytoscape_json,
            "label_color_dict": label_color_dict,
            "balance_group_stats": balance_group_df.to_dict()
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


# Test the function locally
if __name__ == "__main__":
    url = 'https://flipsidecrypto.xyz/api/v1/queries/1a10bea8-8307-417b-af3d-57327b93db7d/data/latest'
    data = fetch_data(url)
    
    if data is not None:
        tripartite_graph, label_color_dict, balance_group_df = create_tripartite_graph(data)
        if tripartite_graph is not None and label_color_dict is not None:
            projected_cytoscape_json = graph_to_cytoscape_json(tripartite_graph)
            print(json.dumps(projected_cytoscape_json, indent=4))
            print(json.dumps(label_color_dict, indent=4))
            print(balance_group_df)
        else:
            print("Failed to create the graph.")
    else:
        print("Failed to fetch data.")
