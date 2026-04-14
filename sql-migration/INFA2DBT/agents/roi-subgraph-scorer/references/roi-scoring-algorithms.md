# ROI Scoring Algorithms

## Build DAG (Step 1)

```python
import networkx as nx

def build_dbt_dag(project_path):
    G = nx.DiGraph()
    
    # Parse manifest.json for model relationships
    with open(f"{project_path}/target/manifest.json") as f:
        manifest = json.load(f)
    
    for node_id, node in manifest['nodes'].items():
        if node['resource_type'] == 'model':
            G.add_node(node['name'], **node)
            for dep in node.get('depends_on', {}).get('nodes', []):
                if dep.startswith('model.'):
                    dep_name = dep.split('.')[-1]
                    G.add_edge(dep_name, node['name'])
    
    return G
```

## Calculate Centrality (Step 4)

```python
def calculate_centrality_scores(dag):
    scores = {}
    pagerank = nx.pagerank(dag)
    betweenness = nx.betweenness_centrality(dag)
    out_degree = dict(dag.out_degree())
    
    for node in dag.nodes():
        scores[node] = {
            'pagerank': pagerank.get(node, 0),
            'betweenness': betweenness.get(node, 0),
            'out_degree': out_degree.get(node, 0)
        }
    
    return scores
```

## Calculate Composite ROI Score (Step 5)

```python
def calculate_roi_score(model_name, query_metrics, credit_metrics, centrality_scores, weights):
    w = weights or {
        'query_frequency': 0.30,
        'bytes_scanned': 0.25,
        'credit_consumption': 0.25,
        'centrality': 0.20
    }
    
    qm = query_metrics.get(model_name, {})
    cm = credit_metrics.get(model_name, {})
    cs = centrality_scores.get(model_name, {})
    
    score = (
        w['query_frequency'] * normalize(qm.get('query_count', 0), all_query_counts) +
        w['bytes_scanned'] * normalize(qm.get('total_gb_scanned', 0), all_scan_volumes) +
        w['credit_consumption'] * normalize(cm.get('total_credits', 0), all_credits) +
        w['centrality'] * normalize(cs.get('pagerank', 0) + cs.get('out_degree', 0), all_centrality)
    )
    
    return score  # 0.0 to 1.0
```

## Assign Tier (Step 6)

```python
def assign_tier(roi_score, model_name, dag):
    if dag.out_degree(model_name) >= 10:
        return 'tier_1'  # High-fanout nodes always Tier 1
    if roi_score >= 0.70:
        return 'tier_1'
    elif roi_score >= 0.40:
        return 'tier_2'
    else:
        return 'tier_3'
```

## Generate Subgraph Recommendations (Steps 7-8)

```python
def identify_optimization_subgraph(model_name, dag, tier_assignments):
    upstream = list(nx.ancestors(dag, model_name))
    downstream = list(nx.descendants(dag, model_name))
    
    optimization_group = [model_name]
    for u in upstream:
        if tier_assignments[u] in ['tier_1', 'tier_2']:
            optimization_group.append(u)
    
    return {
        'anchor_model': model_name,
        'optimization_group': optimization_group,
        'impact_radius': len(downstream),
        'recommendation': generate_recommendation(model_name, dag)
    }

def generate_recommendation(model_name, metrics, dag):
    recs = []
    if metrics['total_gb_scanned'] > 100:
        recs.append({'type': 'clustering', 'priority': 'high',
                     'description': 'Add cluster_by based on common filter columns',
                     'estimated_impact': 'Reduce scan volume by 40-60%'})
    if metrics['query_count'] > 1000 and dag.nodes[model_name].get('materialized') == 'table':
        recs.append({'type': 'incremental', 'priority': 'medium',
                     'description': 'Convert to incremental materialization',
                     'estimated_impact': 'Reduce refresh time and cost'})
    if dag.out_degree(model_name) >= 5:
        upstream_tables = [u for u in dag.predecessors(model_name) 
                         if dag.nodes[u].get('materialized') == 'table']
        if upstream_tables:
            recs.append({'type': 'materialization_review', 'priority': 'medium',
                         'description': f'Review upstream materializations: {upstream_tables}'})
    return recs
```
