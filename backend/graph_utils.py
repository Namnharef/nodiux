# backend/graph_utils.py
import networkx as nx
import pandas as pd
import io
from pyvis.network import Network
from itertools import combinations
import community.community_louvain as community_louvain
from collections import Counter
from itertools import combinations
import ast
import sqlite3
import json

def normalize_handle(handle):
    return handle.lower().replace('.bsky.social','')

#def mentions_build_edges_weighted_G(df):
#    mention_edges = []
#    for idx, row in df.iterrows():
#        source = row['author_handle']
#        for target in row['mentions']:
#            target = normalize_handle(target)
#            if source != target:
#                mention_edges.append({'source': source, 'target': target})
#
#    edges_df = pd.DataFrame(mention_edges)
#    edges_weighted = edges_df.groupby(['source', 'target']).size().reset_index(name='weight')
#
#    G = nx.DiGraph()
#    for _, row in edges_weighted.iterrows():
#        G.add_edge(row['source'], row['target'], weight=row['weight'])  
#        
#    return edges_weighted, G

def mentions_build_edges_weighted_G(df):
    mention_edges = []
    for idx, row in df.iterrows():
        source = row.get('author_handle')
        mentions = row.get('mentions', [])
        if not isinstance(mentions, list):
            continue
        for target in mentions:
            target = normalize_handle(target)
            if source and target and source != target:
                mention_edges.append({'source': source, 'target': target})

    edges_df = pd.DataFrame(mention_edges)
    if edges_df.empty:
        return pd.DataFrame(columns=["source", "target", "weight"]), nx.DiGraph()

    edges_weighted = edges_df.groupby(['source', 'target']).size().reset_index(name='weight')

    G = nx.DiGraph()
    for _, row in edges_weighted.iterrows():
        G.add_edge(row['source'], row['target'], weight=row['weight'])  

    return edges_weighted, G


def hashtags_build_hashtag_weighted_G_hash(df):
    hashtag_edges = []
    for idx, row in df.iterrows():
        hashtags = list(set([h.lower() for h in row['hashtags']]))
        for h1, h2 in combinations(hashtags, 2):
            hashtag_edges.append((h1, h2))
    hashtag_df = pd.DataFrame(hashtag_edges, columns=['hashtag1', 'hashtag2'])
    hashtag_weighted = hashtag_df.groupby(['hashtag1', 'hashtag2']).size().reset_index(name='weight')

    G_hash = nx.Graph()
    for _, row in hashtag_weighted.iterrows():
        G_hash.add_edge(row['hashtag1'], row['hashtag2'], weight=row['weight'])
    
    return hashtag_weighted, G_hash


def build_networks(df):
    if 'mentions' in df.columns:
        df['mentions'] = df['mentions'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    else:
        df['mentions'] = [[] for _ in range(len(df))]  # lista vuota per ogni riga, se serve
    
    if 'hashtags' in df.columns:
        df['hashtags'] = df['hashtags'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
    else:
        df['hashtags'] = [[] for _ in range(len(df))]
        
    # === Mentions Network ===
    edges_weighted, G = mentions_build_edges_weighted_G(df)

    partition = community_louvain.best_partition(G.to_undirected()) if len(G) > 0 else {}

    net = Network(height='700px', width='100%', bgcolor='white', font_color='black', directed=True)
    net.from_nx(G)

    for node in net.nodes:
        node_id = node['id']
        indegree = G.in_degree(node_id)
        node['size'] = 10 + indegree * 5
        node['color'] = f"hsl({(partition.get(node_id,0)*67)%360}, 80%, 60%)"
        node['title'] = f"{node_id} In-degree: {indegree}"

    for edge in net.edges:
        edge['width'] = edge.get('weight', 1)
        edge['title'] = f"Weight: {edge['width']}"

    net.force_atlas_2based(gravity=-50, central_gravity=0.005, spring_length=200, spring_strength=0.01)
    
    mentions_html = net.generate_html()

    # === Hashtag Network ===
    hashtag_weighted, G_hash = hashtags_build_hashtag_weighted_G_hash(df)

    top_nodes = sorted(G_hash.degree, key=lambda x: x[1], reverse=True)[:50]
    G_hash_top = G_hash.subgraph([n for n, _ in top_nodes]).copy()
    partition = community_louvain.best_partition(G_hash_top) if len(G_hash_top) > 0 else {}

    net_h = Network(height='700px', width='100%', bgcolor='white', font_color='black', directed=False)
    net_h.from_nx(G_hash_top)

    for node in net_h.nodes:
        node_id = node['id']
        degree = G_hash_top.degree(node_id)
        node['size'] = 10 + degree * 2
        node['color'] = f"hsl({(partition.get(node_id,0)*67)%360}, 80%, 60%)"
        node['title'] = f"{node_id} Degree: {degree}"

    for edge in net_h.edges:
        edge['width'] = edge.get('weight', 1)
        edge['title'] = f"Weight: {edge['width']}"

    net_h.force_atlas_2based(gravity=-50, central_gravity=0.005, spring_length=200, spring_strength=0.01)
    
    hashtags_html = net_h.generate_html()
    hashtags_html = hashtags_html.replace("mynetwork", "mynetwork_hashtag")

    return mentions_html, hashtags_html

def build_mentions_csv_gexf(df):
    # === Mentions Network ===
    edges_weighted, G = mentions_build_edges_weighted_G(df)  

    # Downloads for Mentions Network
    csv_edges = edges_weighted.to_csv(index=False).encode('utf-8')
    gexf_data = io.BytesIO()
    nx.write_gexf(G, gexf_data)
    gexf_data.seek(0)

    return csv_edges, gexf_data 

def build_hashtag_csv_gexf(df): 
    # === Hashtag Network ===
    hashtag_weighted, G_hash = hashtags_build_hashtag_weighted_G_hash(df)

    top_nodes = sorted(G_hash.degree, key=lambda x: x[1], reverse=True)[:50]
    G_hash_top = G_hash.subgraph([n for n, _ in top_nodes]).copy()
    
    # Downloads for Hashtags Network
    csv_edges_h = hashtag_weighted.to_csv(index=False).encode('utf-8')
    gexf_data_h = io.BytesIO()
    nx.write_gexf(G_hash_top, gexf_data_h)
    gexf_data_h.seek(0)

    return csv_edges_h, gexf_data_h 

def build_kpis(df):
    import numpy as np

    total_posts = len(df)
    
    if 'author_handle' in df.columns:
        unique_users = df['author_handle'].nunique()
    else:
        unique_users = 0        
        
    total_mentions = df['mentions'].apply(len).sum() if 'mentions' in df.columns else 0
    total_hashtags = df['hashtags'].apply(len).sum() if 'hashtags' in df.columns else 0

    kpis = {
        "posts": total_posts,
        "users": unique_users,
        "mentions": total_mentions,
        "hashtags": total_hashtags
    }

    if 'author_handle' in df.columns:
        top_active = df['author_handle'].value_counts().head(10).to_dict()
    else:
        top_active = {} 
        
    if 'mentions' in df.columns:
        all_mentions = pd.Series(
            [m.lower() for mentions in df['mentions'] for m in mentions]
        ).value_counts().head(10)
    else:
        all_mentions = pd.Series(dtype=str)
        
    if 'hashtags' in df.columns:
        all_hashtags = pd.Series(
            [h.lower() for hlist in df['hashtags'] for h in hlist]
        ).value_counts().head(10)
    else:
        all_hashtags = pd.Series(dtype=str)

    if 'emojis' in df.columns:
        all_emojis = pd.Series(
            [e for emlist in df['emojis'] for e in emlist]
        ).value_counts().head(10)
    else:
        all_emojis = pd.Series(dtype=str)

    top10 = {
        "active_users": top_active,
        "mentions": all_mentions.to_dict(),
        "hashtags": all_hashtags.to_dict(),
        "emojis": all_emojis.to_dict()
    }

    # --- Analisi temporale dinamica ---
    if 'created_at' in df.columns:
        min_time = df['created_at'].min()
        max_time = df['created_at'].max()
    else:
        min_time = None
        max_time = None
    
    if min_time and max_time:
        total_seconds = (max_time - min_time).total_seconds()
    else:
        total_seconds = 0

    # Definisci il numero di bucket (almeno 10)
    buckets = 10

    # Se range > 10 giorni → raggruppa per mese, altrimenti usa intervalli più piccoli
    if min_time is None or max_time is None or min_time == max_time:
        # Nessun dato valido, crea output vuoto o default
        activity = []
    else:
        if total_seconds > 10 * 86400:
            df['period'] = df['created_at'].dt.to_period('M').dt.to_timestamp()
            grouped = df.groupby('period').size().reset_index(name='post_count')
            grouped['label'] = grouped['period'].dt.strftime('%Y-%m')
        else:
            bins = pd.date_range(start=min_time, end=max_time, periods=buckets+1)
            df['period'] = pd.cut(df['created_at'], bins=bins, right=False)
            grouped = df.groupby('period').size().reset_index(name='post_count')
            grouped['label'] = grouped['period'].apply(lambda x: x.left.strftime('%Y-%m-%d %H:%M'))

        grouped['rolling'] = grouped['post_count'].rolling(3, min_periods=1).mean()
        activity = grouped[['label', 'post_count', 'rolling']].rename(columns={'label': 'time'}).to_dict(orient='records')

    return kpis, top10, activity, df
