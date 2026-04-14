# RAG Corpus Search

Agent 2 queries the corpus before generating DBT code:

```python
def search_corpus(transformation_type: str, limit: int = 3) -> list:
    """Search for relevant conversion examples."""
    query = f"""
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH',
            '{{"query": "{transformation_type} conversion pattern",
              "columns": ["TRANSFORMATION_TYPE", "INFA_PATTERN", "DBT_PATTERN"],
              "filter": {{"@eq": {{"TRANSFORMATION_TYPE": "{transformation_type}"}}}},
              "limit": {limit}}}'
        )
    )['results']
    """
    return execute_sql(query)
```

## Adding Corpus Examples

```sql
INSERT INTO INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS 
(EXAMPLE_ID, TRANSFORMATION_TYPE, INFA_PATTERN, DBT_PATTERN, DESCRIPTION, TAGS, COMPLEXITY, USE_CASE)
SELECT 
    'EXP001', 
    'Expression',
    '<TRANSFORMATION TYPE="Expression"><TABLEATTRIBUTE EXPRESSION="qty * price"/></TRANSFORMATION>',
    'SELECT qty * price AS total FROM source_cte',
    'Convert arithmetic Expression to SQL calculation',
    ARRAY_CONSTRUCT('calculation', 'arithmetic'),
    'LOW',
    'Simple calculations';
```
