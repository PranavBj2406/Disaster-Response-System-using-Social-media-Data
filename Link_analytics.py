from pymongo import MongoClient
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np

def fetch_disaster_data(days_back=7):
    """Fetch recent disaster data from MongoDB"""
    print("Connecting to MongoDB...")
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['disaster_response']
        
        # Get data from both batch and streaming collections
        print("Fetching batch data...")
        batch_data = list(db.disaster_alerts.find())
        print(f"Found {len(batch_data)} batch records")
        
        print("Fetching streaming data...")
        streaming_data = list(db.realtime_alerts.find())
        print(f"Found {len(streaming_data)} streaming records")
        
        # Combine the data
        all_data = batch_data + streaming_data
        print(f"Total combined records: {len(all_data)}")
        
        if not all_data:
            print("No data found in either collection!")
            return pd.DataFrame()
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(all_data)
        print(f"DataFrame created with shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Filter for recent data
        if 'timestamp' in df.columns:
            print("Filtering by timestamp...")
            # Convert timestamp strings to datetime objects if needed
            if isinstance(df['timestamp'].iloc[0], str):
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Filter for recent data
            cutoff_date = datetime.now() - timedelta(days=days_back)
            print(f"Filtering for data after: {cutoff_date}")
            
            original_count = len(df)
            df = df[df['timestamp'] >= cutoff_date]
            print(f"After date filtering: {len(df)} records (removed {original_count - len(df)})")
        else:
            print("No 'timestamp' column found - using all data")
        
        return df
        
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return pd.DataFrame()

def build_location_graph(df):
    """Build a graph connecting locations based on disaster relationships"""
    G = nx.Graph()
    
    # Add nodes for each location
    locations = df['location'].unique()
    for loc in locations:
        G.add_node(loc)
    
    # Connect locations that have the same disaster type within a short time frame
    disaster_types = df['disaster_type'].unique()
    
    for disaster in disaster_types:
        disaster_df = df[df['disaster_type'] == disaster]
        
        # Get all locations for this disaster type
        affected_locations = disaster_df['location'].unique()
        
        # Connect all locations affected by the same disaster type
        if len(affected_locations) > 1:
            for i in range(len(affected_locations)):
                for j in range(i+1, len(affected_locations)):
                    loc1 = affected_locations[i]
                    loc2 = affected_locations[j]
                    
                    # Check if edge already exists
                    if G.has_edge(loc1, loc2):
                        # Increase weight for existing edge
                        G[loc1][loc2]['weight'] += 1
                        G[loc1][loc2]['disasters'].add(disaster)
                    else:
                        # Create new edge
                        G.add_edge(loc1, loc2, weight=1, disasters={disaster})
    
    return G

def analyze_disaster_spread(G, df):
    """Analyze the spread of disasters between locations"""
    # Calculate network metrics
    centrality = nx.degree_centrality(G)
    betweenness = nx.betweenness_centrality(G)
    
    # Find communities using Louvain method
    communities = nx.community.louvain_communities(G)
    
    # Calculate average alert level by location
    loc_alert_levels = df.groupby('location')['alert_level'].mean().to_dict()
    
    # Prepare results
    results = {
        'high_risk_locations': [],
        'disaster_clusters': [],
        'potential_spread_paths': []
    }
    
    # Identify high-risk locations (high centrality and high alert level)
    for loc, cent in centrality.items():
        if loc in loc_alert_levels:
            alert = loc_alert_levels[loc]
            risk_score = cent * alert
            
            if risk_score > 0.5:  # Threshold can be adjusted
                results['high_risk_locations'].append({
                    'location': loc,
                    'centrality': cent,
                    'alert_level': alert,
                    'risk_score': risk_score
                })
    
    # Sort by risk score
    results['high_risk_locations'].sort(key=lambda x: x['risk_score'], reverse=True)
    
    # Identify disaster clusters from communities
    for i, community in enumerate(communities):
        community_list = list(community)
        if len(community_list) > 1:
            cluster = {
                'id': i,
                'locations': community_list,
                'size': len(community_list),
                'avg_alert_level': np.mean([loc_alert_levels.get(loc, 0) for loc in community_list])
            }
            results['disaster_clusters'].append(cluster)
    
    # Sort clusters by size and alert level
    results['disaster_clusters'].sort(key=lambda x: (x['size'], x['avg_alert_level']), reverse=True)
    
    # Identify potential spread paths (high betweenness edges)
    for u, v, data in G.edges(data=True):
        edge_betweenness = betweenness[u] + betweenness[v]
        if edge_betweenness > 0.1:  # Threshold can be adjusted
            results['potential_spread_paths'].append({
                'from': u,
                'to': v,
                'betweenness': edge_betweenness,
                'shared_disasters': list(data.get('disasters', set())),
                'connection_strength': data.get('weight', 1)
            })
    
    # Sort by betweenness
    results['potential_spread_paths'].sort(key=lambda x: x['betweenness'], reverse=True)
    
    return results

def visualize_disaster_network(G, output_file='disaster_network.png'):
    """Visualize the disaster network"""
    plt.figure(figsize=(12, 10))
    
    # Prepare node colors based on disaster types
    node_colors = []
    
    # Get positions using force-directed layout
    pos = nx.spring_layout(G, k=0.3, iterations=50)
    
    # Draw the graph
    edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
    max_weight = max(edge_weights) if edge_weights else 1
    normalized_weights = [2 + 3 * (w / max_weight) for w in edge_weights]
    
    nx.draw_networkx_edges(G, pos, width=normalized_weights, alpha=0.5)
    nx.draw_networkx_nodes(G, pos, node_size=500, alpha=0.8)
    nx.draw_networkx_labels(G, pos, font_size=10)
    
    plt.title("Disaster Connection Network by Location")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    return output_file

def generate_link_analysis_report():
    """Generate a comprehensive link analysis report"""
    print("Starting link analysis report generation...")
    
    # Fetch data
    df = fetch_disaster_data()
    
    if df.empty:
        print("ERROR: No disaster data available for analysis.")
        return "No disaster data available for analysis."
    
    print(f"Processing {len(df)} records...")
    
    # Check required columns
    required_columns = ['location', 'disaster_type', 'alert_level']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"ERROR: Missing required columns: {missing_columns}")
        print(f"Available columns: {df.columns.tolist()}")
        return f"Missing required columns: {missing_columns}"
    
    print("Required columns found. Continuing with analysis...")
    
    # Build the graph
    G = build_location_graph(df)
    print(f"Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    
    # Run analysis
    analysis_results = analyze_disaster_spread(G, df)
    
    # Visualize
    viz_file = visualize_disaster_network(G)
    
    # Save results to MongoDB
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['disaster_response']
        
        report = {
            'timestamp': datetime.now(),
            'locations_analyzed': len(df['location'].unique()),
            'disaster_types_analyzed': list(df['disaster_type'].unique()),
            'high_risk_locations': analysis_results['high_risk_locations'][:5],  # Top 5
            'disaster_clusters': analysis_results['disaster_clusters'],
            'potential_spread_paths': analysis_results['potential_spread_paths'][:10],  # Top 10
            'visualization_path': viz_file
        }
        
        db.link_analysis_reports.insert_one(report)
        print("Report saved to MongoDB successfully")
        
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")
        report = {
            'timestamp': datetime.now(),
            'locations_analyzed': len(df['location'].unique()),
            'disaster_types_analyzed': list(df['disaster_type'].unique()),
            'high_risk_locations': analysis_results['high_risk_locations'][:5],
            'disaster_clusters': analysis_results['disaster_clusters'],
            'potential_spread_paths': analysis_results['potential_spread_paths'][:10],
            'visualization_path': viz_file
        }
    
    print(f"Link analysis report generated with {len(analysis_results['high_risk_locations'])} high-risk locations identified.")
    return report

if __name__ == "__main__":
    print("="*50)
    print("DISASTER LINK ANALYSIS - DEBUG MODE")
    print("="*50)
    
    report = generate_link_analysis_report()
    
    if isinstance(report, str):
        print(f"Analysis failed: {report}")
    else:
        # Print a summary
        print("\n" + "="*50)
        print("DISASTER LINK ANALYSIS SUMMARY:")
        print("="*50)
        print(f"Analysis completed at: {report['timestamp']}")
        print(f"Analyzed {report['locations_analyzed']} locations and {len(report['disaster_types_analyzed'])} disaster types")
        
        if report['high_risk_locations']:
            print("\nTop High-Risk Locations:")
            for loc in report['high_risk_locations']:
                print(f"- {loc['location']}: Risk Score {loc['risk_score']:.2f}, Alert Level {loc['alert_level']:.1f}")
        else:
            print("\nNo high-risk locations identified")
        
        print(f"\nIdentified {len(report['disaster_clusters'])} disaster clusters")
        print(f"Found {len(report['potential_spread_paths'])} potential spread paths between locations")
        print(f"\nNetwork visualization saved to: {report['visualization_path']}")
    
    print("\n" + "="*50)
    print("ANALYSIS COMPLETE")
    print("="*50)