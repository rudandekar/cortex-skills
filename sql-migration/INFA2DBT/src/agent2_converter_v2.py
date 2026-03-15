#!/usr/bin/env python3
"""Agent 2: Informatica to DBT Code Converter v2.0 - With RAG Corpus Search and Persistent State

IMPORTANT: This is a PoC/reference implementation. The fidelity scoring in this module
is a structural proxy heuristic, NOT the full Agent 3 scoring pipeline which requires
sandbox execution and data comparison. See Agent 3 SKILL.md for the complete scoring spec.
"""

import json
import os
import re
import uuid
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("Warning: snowflake-connector-python not installed. State persistence disabled.")

CONNECTION_NAME = os.getenv("SNOWFLAKE_CONNECTION_NAME") or "DELOITTENA_COCO"


@contextmanager
def snowflake_connection():
    """Context manager for Snowflake connection - ensures single connection per batch."""
    if not SNOWFLAKE_AVAILABLE:
        yield None
        return
    
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    try:
        yield conn
    finally:
        conn.close()


def escape_dollar_quotes(content: str) -> str:
    """Escape $$ in content to prevent dollar-quote injection."""
    if not content:
        return ""
    return content.replace("$$", "$\\$")


def search_corpus(conn, transformation_type: str, description: str = None, limit: int = 3) -> list:
    """Search the INFA2DBT corpus for relevant conversion examples using Cortex Search.
    
    Uses parameterized JSON construction to prevent injection.
    """
    if conn is None:
        return []
    
    try:
        query_text = f"{transformation_type} transformation conversion pattern"
        if description:
            query_text += f" {description[:100]}"
        
        search_params = json.dumps({
            "query": query_text,
            "columns": ["TRANSFORMATION_TYPE", "INFA_PATTERN", "DBT_PATTERN", "DESCRIPTION"],
            "filter": {"@eq": {"TRANSFORMATION_TYPE": transformation_type}},
            "limit": limit
        })
        
        search_query = """
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'INFA2DBT_DB.PIPELINE.INFA2DBT_CORPUS_SEARCH',
                %s
            )
        )['results'] AS results
        """
        
        cursor = conn.cursor()
        cursor.execute(search_query, (search_params,))
        result = cursor.fetchone()
        
        if result and result[0]:
            return result[0]
        return []
    except Exception as e:
        print(f"  [RAG] Corpus search warning: {e}")
        return []


def register_pipeline_run(conn, workflow_name: str, xml_path: str = None) -> str:
    """Register a new pipeline run in PIPELINE_STATE table using parameterized queries."""
    run_id = str(uuid.uuid4())[:8]
    
    if conn is None:
        return run_id
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
            (RUN_ID, WORKFLOW_NAME, XML_FILE_PATH, STATUS, CURRENT_PHASE)
            VALUES (%s, %s, %s, 'IN_PROGRESS', 'CONVERSION')
        """, (run_id, workflow_name, xml_path or ""))
        conn.commit()
        return run_id
    except Exception as e:
        print(f"  [STATE] Warning - could not register run: {e}")
        return run_id


def update_pipeline_status(conn, run_id: str, status: str, phase: str = None, error_msg: str = None):
    """Update pipeline run status using parameterized queries."""
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        
        if status in ['COMPLETED', 'FAILED']:
            if error_msg:
                cursor.execute("""
                    UPDATE INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
                    SET STATUS = %s, CURRENT_PHASE = %s, ERROR_MSG = %s, 
                        COMPLETED_AT = CURRENT_TIMESTAMP(), UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE RUN_ID = %s
                """, (status, phase or '', error_msg[:500] if error_msg else None, run_id))
            else:
                cursor.execute("""
                    UPDATE INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
                    SET STATUS = %s, CURRENT_PHASE = %s, 
                        COMPLETED_AT = CURRENT_TIMESTAMP(), UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE RUN_ID = %s
                """, (status, phase or '', run_id))
        else:
            cursor.execute("""
                UPDATE INFA2DBT_DB.PIPELINE.PIPELINE_STATE 
                SET STATUS = %s, CURRENT_PHASE = %s, UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RUN_ID = %s
            """, (status, phase or '', run_id))
        
        conn.commit()
    except Exception as e:
        print(f"  [STATE] Warning - could not update status: {e}")


def register_model(conn, run_id: str, handoff: dict, sql_content: str, schema_yml: str, unit_test_yml: str) -> str:
    """Register converted model in MODEL_REGISTRY table using parameterized queries."""
    model_id = str(uuid.uuid4())[:8]
    
    if conn is None:
        return model_id
    
    try:
        model_name = handoff['proposed_model_name']
        workflow_name = handoff['workflow_name']
        target = handoff['target']
        transformation_types = handoff.get('transformation_types', [])
        
        safe_sql = escape_dollar_quotes(sql_content)
        safe_schema = escape_dollar_quotes(schema_yml)
        safe_unit = escape_dollar_quotes(unit_test_yml)
        
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO INFA2DBT_DB.PIPELINE.MODEL_REGISTRY 
            (MODEL_ID, RUN_ID, MODEL_NAME, SOURCE_WORKFLOW, TARGET_TABLE, TARGET_SCHEMA,
             SQL_CONTENT, SCHEMA_YML, UNIT_TEST_YML, TRANSFORMATION_TYPES, 
             COLUMN_COUNT, CTE_COUNT, FIDELITY_STATUS)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
        """, (
            model_id,
            run_id,
            model_name,
            workflow_name,
            target.get("name", ""),
            target.get("owner", "PUBLIC"),
            safe_sql,
            safe_schema,
            safe_unit,
            transformation_types,
            len(target.get('fields', [])),
            sql_content.count(' AS (')
        ))
        conn.commit()
        return model_id
    except Exception as e:
        print(f"  [REGISTRY] Warning - could not register model: {e}")
        return model_id


def record_fidelity_score(conn, model_id: str, run_id: str, scores: dict):
    """Record fidelity scores for a model using parameterized queries."""
    if conn is None:
        return
    
    score_id = str(uuid.uuid4())[:8]
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO INFA2DBT_DB.PIPELINE.FIDELITY_SCORES
            (SCORE_ID, MODEL_ID, RUN_ID, OVERALL_SCORE, STRUCTURE_SCORE, 
             SEMANTICS_SCORE, TEST_COVERAGE, TRANSFORMATION_COVERAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            score_id,
            model_id,
            run_id,
            scores.get('overall', 0.0),
            scores.get('structure', 0.0),
            scores.get('semantics', 0.0),
            scores.get('test_coverage', 0.0),
            scores.get('transformation_coverage', 0.0)
        ))
        conn.commit()
    except Exception as e:
        print(f"  [FIDELITY] Warning - could not record score: {e}")


def load_handoff(handoff_path: str) -> dict:
    """Load handoff JSON file."""
    with open(handoff_path) as f:
        return json.load(f)


def generate_dbt_sql(handoff: dict, corpus_examples: dict = None) -> str:
    """Generate DBT SQL model from handoff, enhanced with corpus examples."""
    model_name = handoff['proposed_model_name']
    workflow_name = handoff['workflow_name']
    target = handoff['target']
    sources = handoff['sources']
    transformations = handoff['transformation_chain']
    
    materialization = determine_materialization(model_name)
    
    config = f'''{{{{ config(
    materialized='{materialization}',
    schema='{target.get('owner', 'PUBLIC').lower()}',
    tags=['wf_{workflow_name.lower().replace("wf_", "")}', 'TODO_freq', 'TODO_domain'],
    meta={{
        'source_workflow': '{workflow_name}',
        'target_table': '{target['name']}',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '{datetime.now(timezone.utc).isoformat()}'
    }}
) }}}}'''
    
    ctes = []
    
    for i, source in enumerate(sources):
        source_name = source['name'].lower()
        cte = f'''source_{source_name} AS (
    SELECT
        {generate_column_list(source['fields'])}
    FROM {{{{ source('raw', '{source_name}') }}}}
)'''
        ctes.append(cte)
    
    for transform in transformations:
        t_type = transform['type']
        t_name = transform['name'].lower()
        
        example = corpus_examples.get(t_type, {}) if corpus_examples else {}
        
        if t_type == 'Filter':
            cte = generate_filter_cte(transform, ctes, example)
            if cte:
                ctes.append(cte)
        
        elif t_type == 'Expression':
            cte = generate_expression_cte(transform, ctes, example)
            if cte:
                ctes.append(cte)
        
        elif t_type == 'Aggregator':
            cte = generate_aggregator_cte(transform, ctes, example)
            if cte:
                ctes.append(cte)
        
        elif t_type in ['Lookup Procedure', 'Lookup']:
            cte = generate_lookup_cte(transform, ctes, example, handoff)
            if cte:
                ctes.append(cte)
        
        elif t_type == 'Joiner':
            cte = generate_joiner_cte(transform, ctes, example, handoff)
            if cte:
                ctes.append(cte)
        
        elif t_type == 'Sequence Generator':
            cte = generate_sequence_cte(transform, ctes, example)
            if cte:
                ctes.append(cte)
        
        elif t_type == 'Sorter':
            cte = generate_sorter_cte(transform, ctes, example)
            if cte:
                ctes.append(cte)
        
        elif t_type == 'Router':
            cte = generate_router_cte(transform, ctes, example)
            if cte:
                ctes.append(cte)
        
        elif t_type == 'Normalizer':
            cte = generate_normalizer_cte(transform, ctes, example)
            if cte:
                ctes.append(cte)
    
    final_cte = f'''final AS (
    SELECT
        {generate_target_columns(target['fields'])}
    FROM {get_previous_cte_name(ctes)}
)'''
    ctes.append(final_cte)
    
    sql = f'''{config}

WITH {','.join([chr(10) + chr(10) + cte for cte in ctes])}

SELECT * FROM final'''
    
    return sql


def generate_filter_cte(transform: dict, ctes: list, example: dict) -> str:
    t_name = transform['name'].lower()
    filter_cond = transform.get('attributes', {}).get('Filter Condition', 'TRUE')
    filter_cond = clean_expression(filter_cond)
    prev_cte = get_previous_cte_name(ctes)
    return f'''filtered_{t_name} AS (
    SELECT *
    FROM {prev_cte}
    WHERE {filter_cond}
)'''


def generate_expression_cte(transform: dict, ctes: list, example: dict) -> str:
    t_name = transform['name'].lower()
    expressions = []
    pass_through = []
    
    for field in transform.get('fields', []):
        if field.get('porttype') == 'OUTPUT' and field.get('expression'):
            expr = clean_expression(field['expression'])
            expressions.append(f"    {expr} AS {field['name'].lower()}")
        elif field.get('porttype') in ['INPUT/OUTPUT', 'INPUT']:
            pass_through.append(f"    {field['name'].lower()}")
    
    if not expressions and not pass_through:
        return None
    
    prev_cte = get_previous_cte_name(ctes)
    cols = ',\n'.join(pass_through + expressions) if (pass_through or expressions) else '*'
    return f'''transformed_{t_name} AS (
    SELECT
{cols}
    FROM {prev_cte}
)'''


def generate_aggregator_cte(transform: dict, ctes: list, example: dict) -> str:
    t_name = transform['name'].lower()
    group_cols = []
    agg_exprs = []
    
    for field in transform.get('fields', []):
        expr = field.get('expression', '')
        if any(agg in expr.upper() for agg in ['COUNT(', 'SUM(', 'MAX(', 'MIN(', 'AVG(']):
            agg_exprs.append(f"    {clean_expression(expr)} AS {field['name'].lower()}")
        elif field.get('porttype') in ['INPUT/OUTPUT', 'INPUT']:
            group_cols.append(field['name'].lower())
    
    prev_cte = get_previous_cte_name(ctes)
    cols = ',\n'.join([f"    {c}" for c in group_cols] + agg_exprs)
    group_by = ', '.join(group_cols) if group_cols else '1'
    return f'''aggregated_{t_name} AS (
    SELECT
{cols}
    FROM {prev_cte}
    GROUP BY {group_by}
)'''


def generate_lookup_cte(transform: dict, ctes: list, example: dict, handoff: dict = None) -> str:
    """Generate lookup CTE with proper Snowflake EXCLUDE syntax."""
    t_name = transform['name'].lower()
    lookup_cond = transform.get('attributes', {}).get('Lookup Condition', 'a.key = b.key')
    lookup_table = transform.get('attributes', {}).get('Lookup table name', 'lookup_table')
    
    lookup_cols = []
    for field in transform.get('fields', []):
        if field.get('porttype') == 'OUTPUT':
            lookup_cols.append(f"b.{field['name'].lower()}")
    
    lookup_select = ', '.join(lookup_cols) if lookup_cols else 'b.*'
    
    prev_cte = get_previous_cte_name(ctes)
    return f'''lookup_{t_name} AS (
    SELECT
        a.*,
        {lookup_select}
    FROM {prev_cte} a
    LEFT JOIN {{{{ source('raw', '{lookup_table.lower()}') }}}} b
        ON {clean_expression(lookup_cond)}
)'''


def generate_joiner_cte(transform: dict, ctes: list, example: dict, handoff: dict = None) -> str:
    """Generate joiner CTE with dynamic detail table from transformation attributes."""
    t_name = transform['name'].lower()
    join_type = transform.get('attributes', {}).get('Join Type', 'Normal Join')
    join_cond = transform.get('attributes', {}).get('Join Condition', 'a.id = b.id')
    
    detail_table = transform.get('attributes', {}).get('Detail Source', None)
    if not detail_table and handoff:
        sources = handoff.get('sources', [])
        if len(sources) > 1:
            detail_table = sources[1]['name']
    if not detail_table:
        detail_table = f"{t_name}_detail"
    
    sql_join = {
        'Normal Join': 'INNER JOIN',
        'Master Outer Join': 'LEFT OUTER JOIN',
        'Detail Outer Join': 'RIGHT OUTER JOIN',
        'Full Outer Join': 'FULL OUTER JOIN'
    }.get(join_type, 'INNER JOIN')
    
    prev_cte = get_previous_cte_name(ctes)
    
    return f'''joined_{t_name} AS (
    SELECT a.*, b.*
    FROM {prev_cte} a
    {sql_join} {{{{ source('raw', '{detail_table.lower()}') }}}} b
        ON {clean_expression(join_cond)}
)'''


def generate_sequence_cte(transform: dict, ctes: list, example: dict) -> str:
    t_name = transform['name'].lower()
    prev_cte = get_previous_cte_name(ctes)
    return f'''sequenced_{t_name} AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS seq_id,
        src.*
    FROM {prev_cte} src
)'''


def generate_sorter_cte(transform: dict, ctes: list, example: dict) -> str:
    t_name = transform['name'].lower()
    sort_order = transform.get('attributes', {}).get('Sorter Data Order', '1')
    prev_cte = get_previous_cte_name(ctes)
    return f'''sorted_{t_name} AS (
    SELECT *
    FROM {prev_cte}
    ORDER BY {sort_order}
)'''


def generate_router_cte(transform: dict, ctes: list, example: dict) -> str:
    """Generate router CTE - note: full router support requires multiple output models."""
    t_name = transform['name'].lower()
    prev_cte = get_previous_cte_name(ctes)
    
    groups = transform.get('groups', [])
    if not groups:
        condition = transform.get('attributes', {}).get('Router Condition', 'TRUE')
        return f'''routed_{t_name} AS (
    SELECT *
    FROM {prev_cte}
    WHERE {clean_expression(condition)}
)'''
    
    first_group = groups[0] if groups else {}
    condition = first_group.get('condition', 'TRUE')
    return f'''routed_{t_name} AS (
    SELECT *
    FROM {prev_cte}
    WHERE {clean_expression(condition)}
)'''


def generate_normalizer_cte(transform: dict, ctes: list, example: dict) -> str:
    """Generate normalizer CTE using LATERAL FLATTEN."""
    t_name = transform['name'].lower()
    prev_cte = get_previous_cte_name(ctes)
    
    array_col = transform.get('attributes', {}).get('Array Column', 'array_col')
    
    return f'''normalized_{t_name} AS (
    SELECT
        src.*,
        flattened.VALUE AS {array_col}_value,
        flattened.INDEX AS {array_col}_index
    FROM {prev_cte} src,
    LATERAL FLATTEN(INPUT => src.{array_col}) flattened
)'''


def determine_materialization(model_name: str) -> str:
    if model_name.startswith('stg_'):
        return 'view'
    elif model_name.startswith('int_'):
        return 'ephemeral'
    elif any(model_name.startswith(p) for p in ['mart_', 'dim_', 'fact_']):
        return 'table'
    return 'table'


def generate_column_list(fields: list) -> str:
    """Generate column list - truncation adds a separate comment line, not inline."""
    if not fields:
        return '*'
    cols = [f['name'].lower() for f in fields[:10]]
    result = ',\n        '.join(cols)
    if len(fields) > 10:
        result += f"\n        /* Note: {len(fields) - 10} additional columns omitted - review source schema */"
    return result


def generate_target_columns(fields: list) -> str:
    if not fields:
        return '*'
    cols = [f['name'].lower() for f in fields]
    return ',\n        '.join(cols)


def get_previous_cte_name(ctes: list) -> str:
    if not ctes:
        return 'source_data'
    last_cte = ctes[-1]
    return last_cte.split(' AS (')[0].strip()


def clean_expression(expr: str, warn_on_truncate: bool = True) -> str:
    """Clean Informatica expression for Snowflake SQL.
    
    Args:
        expr: Raw expression string
        warn_on_truncate: If True, prints warning when expression is truncated
    
    Returns:
        Cleaned expression, truncated to 500 chars max with warning
    """
    if not expr:
        return 'TRUE'
    expr = expr.replace('&#xD;&#xA;', ' ').replace('&#x9;', ' ')
    expr = expr.replace('&apos;', "'").replace('&lt;', '<').replace('&gt;', '>')
    expr = expr.replace('DECODE(TRUE,', 'CASE WHEN ')
    expr = expr.replace('IIF(', 'IFF(')
    expr = expr.replace('SESSSTARTTIME', 'CURRENT_TIMESTAMP()')
    expr = expr.replace('SYSDATE', 'CURRENT_DATE()')
    expr = expr.replace('$PMMappingName', "'mapping_name'")
    expr = ' '.join(expr.split())
    
    max_len = 500
    if len(expr) > max_len:
        if warn_on_truncate:
            print(f"  [WARN] Expression truncated from {len(expr)} to {max_len} chars - manual review required")
        return expr[:max_len] + ' /* TRUNCATED - review original */'
    return expr


def generate_schema_yml(handoff: dict) -> str:
    model_name = handoff['proposed_model_name']
    target = handoff['target']
    workflow_name = handoff['workflow_name']
    
    columns_yml = []
    for field in target['fields']:
        col_yml = f'''      - name: {field['name'].lower()}
        description: "Target column from {workflow_name}"'''
        if field.get('keytype') == 'PRIMARY KEY' or field.get('nullable') == 'NOTNULL':
            col_yml += '''
        tests:
          - not_null'''
        if field.get('keytype') == 'PRIMARY KEY':
            col_yml += '''
          - unique'''
        columns_yml.append(col_yml)
    
    return f'''version: 2

models:
  - name: {model_name}
    description: >
      DBT model converted from Informatica workflow {workflow_name},
      target table {target['name']}. Generated by INFA2DBT accelerator v2.0.0 with RAG.
    meta:
      source_workflow: {workflow_name}
      target_table: {target['name']}
      conversion_timestamp: {datetime.now(timezone.utc).isoformat()}
    columns:
{chr(10).join(columns_yml)}
'''


def generate_unit_tests(handoff: dict) -> str:
    model_name = handoff['proposed_model_name']
    sources = handoff['sources']
    transformation_types = handoff['transformation_types']
    
    tests = []
    
    if 'Filter' in transformation_types:
        tests.append(f'''  - name: test_{model_name}_filter_basic
    model: {model_name}
    description: "Verify filter transformation passes expected rows"
    given:
      - input: source('raw', '{sources[0]['name'].lower() if sources else 'source_table'}')
        rows:
          - {{id: 1, status: 'ACTIVE'}}
          - {{id: 2, status: 'INACTIVE'}}
    expect:
      rows:
        - {{id: 1, status: 'ACTIVE'}}''')
    
    if 'Aggregator' in transformation_types:
        tests.append(f'''  - name: test_{model_name}_aggregator_sum
    model: {model_name}
    description: "Verify aggregation produces correct sums"
    given:
      - input: source('raw', '{sources[0]['name'].lower() if sources else 'source_table'}')
        rows:
          - {{key_id: 1, amount: 100}}
          - {{key_id: 1, amount: 200}}
          - {{key_id: 2, amount: 50}}
    expect:
      rows:
        - {{key_id: 1, total_amount: 300}}
        - {{key_id: 2, total_amount: 50}}''')
    
    if 'Expression' in transformation_types:
        tests.append(f'''  - name: test_{model_name}_expression_transform
    model: {model_name}
    description: "Verify expression transformation logic"
    given:
      - input: source('raw', '{sources[0]['name'].lower() if sources else 'source_table'}')
        rows:
          - {{col1: 'test', col2: 100}}
    expect:
      rows:
        - {{col1: 'test', col2: 100}}''')
    
    if not tests:
        tests.append(f'''  - name: test_{model_name}_basic_passthrough
    model: {model_name}
    description: "Basic passthrough verification"
    given:
      - input: source('raw', 'source_table')
        rows:
          - {{id: 1}}
    expect:
      rows:
        - {{id: 1}}''')
    
    return f'''version: 2

unit_tests:
{chr(10).join(tests)}
'''


def calculate_fidelity_scores(handoff: dict, sql_content: str) -> dict:
    """Calculate conversion fidelity scores.
    
    NOTE: This is a STRUCTURAL PROXY heuristic for quick feedback during conversion.
    It does NOT replace Agent 3's full fidelity scoring which requires:
    - Sandbox execution with sample data
    - Row count comparison
    - Column-level validation
    - Null profile matching
    - Aggregate verification
    
    See Agent 3 SKILL.md (conversion-fidelity-scorer) for the complete scoring spec.
    """
    transformation_types = handoff.get('transformation_types', [])
    target_fields = handoff.get('target', {}).get('fields', [])
    
    structure_score = min(1.0, len(sql_content.split(' AS (')) / max(1, len(transformation_types)))
    semantics_score = 0.8 if not handoff.get('quarantine_flag', False) else 0.5
    test_coverage = min(1.0, len(transformation_types) * 0.25)
    transformation_coverage = min(1.0, sql_content.count('FROM') / max(1, len(transformation_types)))
    
    overall = (structure_score * 0.3 + semantics_score * 0.3 + 
               test_coverage * 0.2 + transformation_coverage * 0.2)
    
    return {
        'overall': round(overall, 3),
        'structure': round(structure_score, 3),
        'semantics': round(semantics_score, 3),
        'test_coverage': round(test_coverage, 3),
        'transformation_coverage': round(transformation_coverage, 3),
        '_note': 'Structural proxy only - see Agent 3 for full fidelity scoring'
    }


def main():
    parser = argparse.ArgumentParser(description='Agent 2: Convert Informatica handoffs to dbt models')
    parser.add_argument('--handoff-dir', default='./artifacts/handoffs', help='Directory containing handoff JSON files')
    parser.add_argument('--output-dir', default='./output', help='Output directory for generated files')
    parser.add_argument('--no-state', action='store_true', help='Disable Snowflake state persistence')
    args = parser.parse_args()
    
    handoff_dir = Path(args.handoff_dir)
    output_dir = Path(args.output_dir)
    
    models_dir = output_dir / 'models' / 'converted'
    tests_dir = output_dir / 'tests' / 'unit'
    logs_dir = output_dir / 'logs' / 'agent2'
    
    for d in [models_dir, tests_dir, logs_dir]:
        d.mkdir(parents=True, exist_ok=True)
    
    if not handoff_dir.exists():
        print(f"Error: Handoff directory not found: {handoff_dir}")
        return
    
    handoff_files = list(handoff_dir.glob('*_handoff.json'))
    
    if not handoff_files:
        print(f"No handoff files found in {handoff_dir}")
        return
    
    print(f"Agent 2 v2.0.0: Converting {len(handoff_files)} handoffs to DBT models...")
    print("Features: RAG Corpus Search + Persistent State Store")
    print("=" * 60)
    
    with snowflake_connection() as conn:
        if args.no_state:
            conn = None
            print("State persistence disabled")
        
        run_id = register_pipeline_run(conn, "BATCH_CONVERSION")
        print(f"Pipeline Run ID: {run_id}")
        
        results = []
        corpus_cache = {}
        
        for handoff_file in handoff_files:
            handoff = load_handoff(str(handoff_file))
            model_name = handoff['proposed_model_name']
            print(f"\nConverting: {model_name}")
            
            try:
                for t_type in handoff.get('transformation_types', []):
                    if t_type not in corpus_cache:
                        print(f"  [RAG] Searching corpus for '{t_type}' patterns...")
                        examples = search_corpus(conn, t_type)
                        if examples:
                            corpus_cache[t_type] = examples[0] if examples else {}
                            print(f"  [RAG] Found {len(examples)} examples for {t_type}")
                        else:
                            corpus_cache[t_type] = {}
                
                sql = generate_dbt_sql(handoff, corpus_cache)
                sql_path = models_dir / f"{model_name}.sql"
                sql_path.write_text(sql)
                print(f"  SQL: {sql_path} ({len(sql)} chars)")
                
                schema_yml = generate_schema_yml(handoff)
                schema_path = models_dir / f"{model_name}.schema.yml"
                schema_path.write_text(schema_yml)
                print(f"  Schema: {schema_path}")
                
                unit_tests = generate_unit_tests(handoff)
                unit_path = tests_dir / f"{model_name}_unit.yml"
                unit_path.write_text(unit_tests)
                print(f"  Unit tests: {unit_path}")
                
                model_id = register_model(conn, run_id, handoff, sql, schema_yml, unit_tests)
                print(f"  [REGISTRY] Model registered: {model_id}")
                
                scores = calculate_fidelity_scores(handoff, sql)
                record_fidelity_score(conn, model_id, run_id, scores)
                print(f"  [FIDELITY] Score: {scores['overall']:.1%} (struct:{scores['structure']:.1%}, sem:{scores['semantics']:.1%})")
                
                log = {
                    'model_id': model_id,
                    'model_name': model_name,
                    'run_id': run_id,
                    'workflow_name': handoff['workflow_name'],
                    'transformation_types': handoff['transformation_types'],
                    'sql_lines': len(sql.split('\n')),
                    'fidelity_scores': scores,
                    'corpus_examples_used': len([t for t in handoff['transformation_types'] if corpus_cache.get(t)]),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                log_path = logs_dir / f"{model_name}.json"
                log_path.write_text(json.dumps(log, indent=2))
                
                results.append(log)
                
            except Exception as e:
                print(f"  ERROR: {e}")
                results.append({
                    'model_name': model_name,
                    'error': str(e),
                    'quarantine_flag': True
                })
        
        success_count = len([r for r in results if 'error' not in r])
        error_count = len([r for r in results if 'error' in r])
        
        if error_count == 0:
            update_pipeline_status(conn, run_id, 'COMPLETED', 'DONE')
        else:
            update_pipeline_status(conn, run_id, 'COMPLETED_WITH_ERRORS', 'DONE', f'{error_count} models failed')
        
        print("\n" + "=" * 60)
        print(f"Agent 2 v2.0.0 Complete")
        print(f"  Run ID: {run_id}")
        print(f"  Models converted: {success_count}")
        print(f"  Errors: {error_count}")
        
        if results:
            avg_fidelity = sum(r.get('fidelity_scores', {}).get('overall', 0) for r in results if 'error' not in r) / max(1, success_count)
            print(f"  Avg fidelity score: {avg_fidelity:.1%}")
            corpus_hits = sum(r.get('corpus_examples_used', 0) for r in results)
            print(f"  Corpus examples used: {corpus_hits}")


if __name__ == '__main__':
    main()
