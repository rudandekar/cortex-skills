# Target Consolidation Analysis

Identifies multiple models writing to the same target table — a key optimization opportunity.

## Detection Query

```sql
SELECT 
    TARGET_TABLE,
    TARGET_SCHEMA,
    COUNT(DISTINCT MODEL_NAME) AS model_count,
    ARRAY_AGG(DISTINCT MODEL_NAME) AS models,
    ARRAY_AGG(DISTINCT SOURCE_WORKFLOW) AS source_workflows,
    SUM(CTE_COUNT) AS total_ctes,
    AVG(COLUMN_COUNT) AS avg_columns
FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY
GROUP BY TARGET_TABLE, TARGET_SCHEMA
HAVING COUNT(DISTINCT MODEL_NAME) > 1
ORDER BY model_count DESC;
```

## Analysis Code

```python
def analyze_consolidation_candidates(conn, dag):
    """Identify models targeting same table and assess consolidation potential."""
    cursor = conn.cursor()
    cursor.execute(CONSOLIDATION_QUERY)
    
    candidates = []
    for row in cursor.fetchall():
        target_table = row[0]
        models = row[3]
        workflows = row[4]
        
        similarity_scores = []
        for i, m1 in enumerate(models):
            for m2 in models[i+1:]:
                sim = compare_model_sql(conn, m1, m2)
                similarity_scores.append(sim)
        
        avg_similarity = sum(similarity_scores) / len(similarity_scores) if similarity_scores else 0
        
        candidates.append({
            'target_table': f"{row[1]}.{target_table}",
            'model_count': row[2],
            'models': models,
            'source_workflows': workflows,
            'sql_similarity': avg_similarity,
            'consolidation_priority': 'HIGH' if avg_similarity > 0.7 else 'MEDIUM' if avg_similarity > 0.4 else 'LOW',
            'recommendation': generate_consolidation_recommendation(models, avg_similarity, workflows)
        })
    
    return sorted(candidates, key=lambda x: (-x['model_count'], -x['sql_similarity']))


def compare_model_sql(conn, model1: str, model2: str) -> float:
    """Compare SQL content similarity between two models (0.0 to 1.0) using Jaccard similarity of CTE structure."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SQL_CONTENT FROM INFA2DBT_DB.PIPELINE.MODEL_REGISTRY 
        WHERE MODEL_NAME IN (%s, %s)
    """, (model1, model2))
    
    results = cursor.fetchall()
    if len(results) != 2:
        return 0.0
    
    sql1, sql2 = results[0][0], results[1][0]
    ctes1 = set(re.findall(r'(\w+)\s+AS\s*\(', sql1, re.IGNORECASE))
    ctes2 = set(re.findall(r'(\w+)\s+AS\s*\(', sql2, re.IGNORECASE))
    
    if not ctes1 and not ctes2:
        return 0.0
    
    intersection = len(ctes1 & ctes2)
    union = len(ctes1 | ctes2)
    return intersection / union if union > 0 else 0.0


def generate_consolidation_recommendation(models, similarity, workflows):
    if similarity > 0.7:
        return f"MERGE: {similarity:.0%} overlap. Likely duplicate logic from {', '.join(workflows)}."
    elif similarity > 0.4:
        return f"REVIEW: {similarity:.0%} overlap. May have shared transformations."
    else:
        return f"KEEP SEPARATE: Low similarity ({similarity:.0%})."
```
