import pandas as pd
import networkx as nx
import seaborn as sns
from matplotlib.colors import rgb2hex
from graph_utils import add_or_update_node

def format_balance(balance, unit='eth'):
    if unit == 'eth':
        balance /= 10**18
        return f"{balance:,.2f}"
    elif unit == 'usd':
        if balance >= 1e6:
            balance /= 1e6
            return f"${balance:,.0f}M"
        return f"${balance:,.0f}"

def label_node(node):
    partite = node.get('partite')
    label = node.get('label')
    label_subtype = node.get('label_subtype')
    balance_category = node.get('balance_category')

    if partite == 'customer':
        return balance_category
    elif '3rd party' in partite:
        return f"{label_subtype}"
    elif partite == 'unknown':
        return balance_category
    elif partite == 'coinbase':
        return label_subtype
    else:
        return 'unknown'

def classify_partite(id):
    if '3rd party' in id:
        return id
    elif 'coinbase' in id:
        return 'coinbase'
    elif 'customer' in id:
        return 'customer'
    else:
        return 'unknown'

def generate_node_id(row, label_col, subtype_col, type_col, balance_category_col):
    label = row[label_col]
    subtype = row[subtype_col]
    balance_category = row[balance_category_col]
    label_type = row[type_col]

    if label_col == 'FROM_LABEL':
        # Sender is customer
        if (
            row['TO_LABEL'] == 'coinbase' and
            row['TO_LABEL_SUBTYPE'] == 'hot_wallet' and
            (subtype == 'hot_wallet' or subtype == 'unknown') and
            label != 'coinbase'
        ):
            return f"customer {balance_category}", 'customer'
    elif label_col == 'TO_LABEL':
        # Receiver is customer
        if (
            row['FROM_LABEL'] == 'coinbase' and
            row['FROM_LABEL_SUBTYPE'] == 'hot_wallet' and
            (subtype == 'hot_wallet' or subtype == 'unknown') and
            label != 'coinbase'
        ):
            return f"customer {balance_category}", 'customer'

    # Receiver is coinbase
    if label_col == 'TO_LABEL' and label == 'coinbase':
        return f"coinbase - {subtype}", 'coinbase'
    # Sender is coinbase
    if label_col == 'FROM_LABEL' and label == 'coinbase':
        return f"coinbase - {subtype}", 'coinbase'

    # Receiver is 3rd party
    if label_col == 'TO_LABEL' and label != 'unknown':
        return f"3rd party {label_type}", '3rd party'

    # Default case
    return f"unknown {balance_category}", 'unknown'

def create_tripartite_graph(data):
    df = pd.DataFrame(data)

    # Verify required columns are present
    required_columns = [
        'FROM_LABEL', 'TO_LABEL', 'FROM_LABEL_SUBTYPE', 'TO_LABEL_SUBTYPE',
        'FROM_LABEL_TYPE', 'TO_LABEL_TYPE', 'FROM_BALANCE_CATEGORY',
        'TO_BALANCE_CATEGORY', 'TRANSACTION_COUNT', 'TOTAL_AMOUNT',
        'TOTAL_AMOUNT_USD', 'TOTAL_FROM_BALANCE', 'TOTAL_TO_BALANCE'
    ]
    
    for col in required_columns:
        if col not in df.columns:
            return None, None

    # Create node IDs based on the new format
    df['FROM_ID'], df['FROM_FINAL_LABEL'] = zip(*df.apply(lambda row: generate_node_id(row, 'FROM_LABEL', 'FROM_LABEL_SUBTYPE', 'FROM_LABEL_TYPE', 'FROM_BALANCE_CATEGORY'), axis=1))
    df['TO_ID'], df['TO_FINAL_LABEL'] = zip(*df.apply(lambda row: generate_node_id(row, 'TO_LABEL', 'TO_LABEL_SUBTYPE', 'TO_LABEL_TYPE', 'TO_BALANCE_CATEGORY'), axis=1))

    B = nx.DiGraph()
    final_labels = set()

    # Initialize data structures to hold the amounts sent and received
    balance_group_stats = {}

    for index, row in df.iterrows():
        from_id = row['FROM_ID']
        to_id = row['TO_ID']
        amount_usd = row['TOTAL_AMOUNT_USD']
        amount_eth = row['TOTAL_AMOUNT']
        transaction_count = row['TRANSACTION_COUNT']

        from_label = row['FROM_LABEL']
        from_label_subtype = row['FROM_LABEL_SUBTYPE']
        from_label_type = row['FROM_LABEL_TYPE']
        from_balance_category = row['FROM_BALANCE_CATEGORY']
        from_latest_balance = row['TOTAL_FROM_BALANCE']

        to_label = row['TO_LABEL']
        to_label_subtype = row['TO_LABEL_SUBTYPE']
        to_label_type = row['TO_LABEL_TYPE']
        to_balance_category = row['TO_BALANCE_CATEGORY']
        to_latest_balance = row['TOTAL_TO_BALANCE']

        from_partite = classify_partite(from_id)
        to_partite = classify_partite(to_id)

        if from_balance_category == 'unknown' or to_balance_category == 'unknown':
            print(f"Skipping node with unknown balance category: {from_id} or {to_id}")
            continue

        # Add or update nodes
        add_or_update_node(B, from_id, from_partite, from_label, from_label_subtype, from_label_type, from_balance_category, from_latest_balance, amount_usd, amount_eth, transaction_count)
        add_or_update_node(B, to_id, to_partite, to_label, to_label_subtype, to_label_type, to_balance_category, to_latest_balance, amount_usd, amount_eth, transaction_count)

        # Collect final labels
        final_labels.add(from_partite)
        final_labels.add(to_partite)

        # Update the balance group stats
        if 'customer' in from_partite and to_partite == 'coinbase':
            if from_balance_category not in balance_group_stats:
                balance_group_stats[from_balance_category] = {'sent': 0, 'received': 0}
            balance_group_stats[from_balance_category]['sent'] += amount_usd

        if from_partite == 'coinbase' and 'customer' in to_partite:
            if to_balance_category not in balance_group_stats:
                balance_group_stats[to_balance_category] = {'sent': 0, 'received': 0}
            balance_group_stats[to_balance_category]['received'] += amount_usd

        if from_id != to_id:
            if B.has_edge(from_id, to_id):
                B.edges[from_id, to_id]['amount_usd'] += amount_usd
                B.edges[from_id, to_id]['amount_eth'] += amount_eth
                B.edges[from_id, to_id]['count'] += transaction_count
            else:
                B.add_edge(from_id, to_id, amount_usd=amount_usd, amount_eth=amount_eth, count=transaction_count)
            print(f"Added edge from {from_id} to {to_id}")
    
    # Generate color palette based on final labels
    palette = sns.color_palette("hsv", len(final_labels))
    color_map = {label: rgb2hex(color) for label, color in zip(final_labels, palette)}

    # Update nodes with colors
    for node_id, node_data in B.nodes(data=True):
        print(f"Node data: {node_data}")
        partite = node_data.get('partite', 'unknown')
        B.nodes[node_id]['color'] = color_map.get(partite) 

    # Calculate network centrality measures and add to node metadata
    degree_centrality = nx.degree_centrality(B)
    closeness_centrality = nx.closeness_centrality(B)

    for node_id in B.nodes:
        B.nodes[node_id]['degree_centrality'] = degree_centrality[node_id]
        B.nodes[node_id]['closeness_centrality'] = closeness_centrality[node_id]
        B.nodes[node_id]['label'] = label_node(B.nodes[node_id])

    label_color_dict = {label: color_map[label] for label in final_labels}

    # Create a DataFrame for balance group stats
    balance_group_df = pd.DataFrame.from_dict(balance_group_stats, orient='index', columns=['sent', 'received'])
    balance_group_df['net'] = balance_group_df['received'] - balance_group_df['sent']

    return B, label_color_dict, balance_group_df
