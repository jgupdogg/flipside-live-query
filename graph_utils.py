import networkx as nx
import seaborn as sns 
from matplotlib.colors import rgb2hex


    

def add_or_update_node(G, node_id, partite, label, label_subtype, label_type, balance_category, latest_balance, amount_usd, amount_eth, transaction_count, color=None):
    if balance_category == 'unknown' or balance_category is None:
        print(f"Skipping node {node_id} with balance category {balance_category}")
        return False

    if not G.has_node(node_id):
        print(f"Adding node {node_id} with label {label} and partite {partite}")
        G.add_node(node_id, partite=partite, id=node_id, label=label, label_subtype=label_subtype, label_type=label_type, balance_category=balance_category, total_usd=0, count=0, amount_eth=0, address_count=0, interacted_count=0, latest_balance=0, color=color, transaction_count=0)
        if partite == 'customer':
            G.nodes[node_id]['balance_group'] = balance_category
    
    # Update node attributes
    G.nodes[node_id]['total_usd'] += amount_usd
    G.nodes[node_id]['address_count'] += 1
    G.nodes[node_id]['transaction_count'] += transaction_count
    G.nodes[node_id]['latest_balance'] += latest_balance
    G.nodes[node_id]['amount_eth'] += amount_eth
    G.nodes[node_id]['label'] = label
    return True

def graph_to_cytoscape_json(G):
    data = nx.cytoscape_data(G)


    return data

def generate_color_dict(final_labels):
    palette = sns.color_palette("hsv", len(final_labels))
    color_map = {label: rgb2hex(color) for label, color in zip(final_labels, palette)}
    return color_map
