#!/usr/bin/env python3
"""
INFA2DBT Agent 2 Automation — Batch dbt Model Generator v1.0

Reads handoff JSONs from Agent 1 and programmatically generates:
  - {model}.sql          — dbt model with CTE structure
  - {model}.schema.yml   — column documentation and tests
  - {model}_unit.yml     — unit test fixtures

Tier classification:
  Tier 1: Single Source Qualifier (with or without SQL Override), 0-1 transforms
  Tier 2: 2-3 standard transforms (Expression, Filter, Aggregator, Lookup, etc.)
  Tier 3: Complex chains (4+ transforms), escalate types, quarantined

Usage:
  python3 agent2_batch.py --handoff-dir /path/to/artifacts --output-dir /path/to/dbt_output
  python3 agent2_batch.py --handoff-dir /path/to/artifacts --output-dir /path/to/dbt_output --tier 1
"""

import json
import os
import re
import csv
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict


TERADATA_TO_SNOWFLAKE = {
    'USER': 'CURRENT_USER()',
    'CURRENT_TIMESTAMP(0)': 'CURRENT_TIMESTAMP()',
    'CURRENT_TIMESTAMP (0)': 'CURRENT_TIMESTAMP()',
    'CURRENT_DATE': 'CURRENT_DATE()',
    'SYSDATE': 'CURRENT_TIMESTAMP()',
    'TRIM': 'TRIM',
    'CAST': 'CAST',
    'COALESCE': 'COALESCE',
    'NVL': 'COALESCE',
    'DECODE': 'DECODE',
}

ESCALATE_TRANSFORMS = {'Stored Procedure', 'Custom Transformation', 'External Procedure'}


def classify_tier(handoff: Dict) -> int:
    t_types = handoff.get('transformation_types', [])
    chain = handoff.get('transformation_chain', [])
    coverage = handoff.get('corpus_coverage_status', 'ok')

    if coverage == 'missing':
        return 3

    for t in t_types:
        if t in ESCALATE_TRANSFORMS:
            return 3

    if len(chain) <= 1 and all(t in ('Source Qualifier',) for t in t_types):
        return 1

    if len(chain) <= 3 and all(t not in ESCALATE_TRANSFORMS for t in t_types):
        return 2

    return 3


def convert_param_refs(sql: str, variables: Dict) -> str:
    for var_name in sorted(variables.keys(), key=len, reverse=True):
        clean_name = var_name.lstrip('$').lower()
        pattern = re.escape(var_name)
        sql = re.sub(pattern, "{{ var('" + clean_name + "') }}", sql)
    return sql


def convert_teradata_update_to_merge(post_sql: str, variables: Dict, model_ref: str = '{{ this }}') -> str:
    post_sql = post_sql.strip().rstrip(';').strip()
    if not post_sql:
        return ''

    converted = convert_param_refs(post_sql, variables)

    converted = re.sub(r'\bUSER\b(?!\s*\()', 'CURRENT_USER()', converted)
    converted = re.sub(r'CURRENT_TIMESTAMP\s*\(\s*0\s*\)', 'CURRENT_TIMESTAMP()', converted)

    update_from_match = re.match(
        r'UPDATE\s+(\w+)\s+FROM\s+(.+?)\s+SET\s+(.+?)\s+WHERE\s+(.+)',
        converted, re.IGNORECASE | re.DOTALL
    )

    if update_from_match:
        alias = update_from_match.group(1).strip()
        from_clause = update_from_match.group(2).strip()
        set_clause = update_from_match.group(3).strip()
        where_clause = update_from_match.group(4).strip()

        tables = [t.strip() for t in from_clause.split(',')]
        target_table = None
        using_table = None

        for tbl in tables:
            parts = tbl.split()
            tbl_name = parts[0]
            tbl_alias = parts[1] if len(parts) > 1 else parts[0].split('.')[-1]
            if tbl_alias.upper() == alias.upper():
                target_table = tbl
            else:
                using_table = tbl

        if target_table and using_table:
            using_parts = using_table.split()
            using_alias = using_parts[1] if len(using_parts) > 1 else using_parts[0].split('.')[-1]

            set_items = []
            for item in re.split(r',(?![^(]*\))', set_clause):
                item = item.strip()
                if '=' in item:
                    col, val = item.split('=', 1)
                    col = col.strip()
                    val = val.strip()
                    if '.' in col:
                        col = col.split('.')[-1]
                    set_items.append(f"        {alias}.{col} = {val}")

            join_conds = []
            other_conds = []
            for cond in re.split(r'\bAND\b', where_clause, flags=re.IGNORECASE):
                cond = cond.strip()
                if alias.upper() in cond.upper() and using_alias.upper() in cond.upper() and '=' in cond:
                    join_conds.append(cond)
                else:
                    other_conds.append(cond)

            on_clause = ' AND '.join(join_conds)
            if other_conds:
                on_clause += ' AND ' + ' AND '.join(other_conds)

            merge_sql = f"""MERGE INTO {model_ref} AS {alias}
    USING {using_table}
        ON {on_clause}
    WHEN MATCHED THEN UPDATE SET
{chr(10).join(set_items)}"""
            return merge_sql

    return f"-- TODO: Manual conversion needed for Post SQL\n-- Original (Teradata):\n-- {converted}"


def clean_sql_override(sql_override: str) -> str:
    if not sql_override:
        return ''
    cleaned = sql_override.replace('\r\n', '\n').replace('\r', '\n')
    cleaned = re.sub(r'\n\s*\n+', '\n', cleaned)
    return cleaned.strip()


def infer_primary_key(handoff: Dict) -> Optional[str]:
    field_lineage = handoff.get('field_lineage', [])
    target_table = handoff.get('target_table', '')

    for fl in field_lineage:
        fname = fl.get('target_field', '').upper()
        if 'BK_' in fname or fname.endswith('_KEY') or fname.endswith('_ID') or fname.endswith('_PK'):
            return fl['target_field']

    chain = handoff.get('transformation_chain', [])
    for transform in chain:
        for port in transform.get('ports', []):
            ptype = port.get('porttype', '').upper()
            if 'PRIMARY' in ptype or 'KEY' in ptype:
                return port['name']

    if field_lineage:
        return field_lineage[0]['target_field']
    return None


def get_session_post_sql(handoff: Dict) -> str:
    for sti_name, attrs in handoff.get('session_overrides', {}).items():
        post_sql = attrs.get('Post SQL', '')
        if post_sql:
            return post_sql
    return ''


def detect_post_sql_from_components(handoff_dir: str, handoff_filename: str) -> str:
    parent_dir = os.path.dirname(os.path.dirname(handoff_dir))
    log_path = os.path.join(parent_dir, 'logs', 'agent1_run.json')
    return ''


def generate_model_sql(handoff: Dict) -> str:
    model_name = handoff.get('_output_model_name', handoff['proposed_model_name'])
    workflow_name = handoff['workflow_name']
    target_table = handoff['target_table']
    variables = handoff.get('variables', {})
    chain = handoff.get('transformation_chain', [])
    field_lineage = handoff.get('field_lineage', [])
    dbt_config = handoff.get('dbt_config', {})
    source_tables = handoff.get('source_tables', [])

    materialization = dbt_config.get('materialization', 'view')
    schema = dbt_config.get('schema', 'PUBLIC')
    tags = dbt_config.get('tags', ['infa2dbt'])
    pre_hooks = dbt_config.get('pre_hook', [])
    post_hooks = dbt_config.get('post_hook', [])

    has_update_strategy = any(t.get('type') == 'Update Strategy' for t in chain)

    post_sql = get_session_post_sql(handoff)
    has_post_sql_merge = bool(post_sql and re.search(r'UPDATE|MERGE|DML_TYPE', post_sql, re.IGNORECASE))

    if has_update_strategy or has_post_sql_merge:
        materialization = 'incremental'

    primary_key = infer_primary_key(handoff)

    sq_transform = next((t for t in chain if t['type'] == 'Source Qualifier'), None)
    sql_override = ''
    if sq_transform:
        sql_override = sq_transform.get('attributes', {}).get('Sql Query', '')

    target_columns = []
    for fl in field_lineage:
        target_columns.append(fl['target_field'])

    if not target_columns and sq_transform:
        for port in sq_transform.get('ports', []):
            pname = port.get('name', '')
            if pname not in ('ACTION_CODE', 'DML_TYPE', 'ROWID'):
                target_columns.append(pname)

    config_parts = []
    config_parts.append(f"    materialized='{materialization}'")

    if materialization == 'incremental':
        if primary_key:
            config_parts.append(f"    unique_key='{primary_key}'")
        else:
            config_parts.append("    unique_key='TODO_define_unique_key'")
        config_parts.append("    incremental_strategy='merge'")
        config_parts.append("    on_schema_change='sync_all_columns'")

    config_parts.append(f"    schema=var('{schema.lower()}', '{schema}')")

    tag_list = ', '.join(f"'{t}'" for t in ['wf_' + workflow_name.lower()] + tags)
    config_parts.append(f"    tags=[{tag_list}]")

    meta_dict = (
        f"    meta={{\n"
        f"        'source_workflow': '{workflow_name}',\n"
        f"        'target_table': '{target_table}',\n"
        f"        'generated_by': 'INFA2DBT_accelerator_v2.2.0',\n"
        f"        'generation_timestamp': '{datetime.now(timezone.utc).isoformat()}'\n"
        f"    }}"
    )
    config_parts.append(meta_dict)

    if pre_hooks:
        for ph in pre_hooks:
            config_parts.append(f"    pre_hook=\"{ph}\"")

    merge_hook = ''
    if has_post_sql_merge and post_sql:
        merge_hook = convert_teradata_update_to_merge(post_sql, variables)
        if merge_hook and not merge_hook.startswith('-- TODO'):
            escaped = merge_hook.replace('"', '\\"').replace('\n', '\\n')
            config_parts.append(f'    post_hook="{escaped}"')

    config_block = "{{ config(\n" + ",\n".join(config_parts) + "\n) }}"

    ctes = []

    if sql_override:
        cleaned_sql = clean_sql_override(sql_override)
        cleaned_sql = convert_param_refs(cleaned_sql, variables)
        source_name = source_tables[0].split('.')[-1].lower() if source_tables else 'source'
        ctes.append(f"source_{source_name} AS (\n    {cleaned_sql}\n)")
    elif source_tables:
        for src in source_tables:
            src_name = src.split('.')[-1].lower()
            src_ref = convert_param_refs(src, variables)
            ctes.append(f"source_{src_name} AS (\n    SELECT *\n    FROM {src_ref}\n)")

    prev_cte = None
    if ctes:
        first_src = source_tables[0].split('.')[-1].lower() if source_tables else 'source'
        prev_cte = f"source_{first_src}"

    for transform in chain:
        t_type = transform.get('type', '')
        t_name = transform.get('name', '').lower()

        if t_type == 'Source Qualifier':
            continue

        elif t_type == 'Expression':
            expr_cols = []
            for port in transform.get('ports', []):
                expr = port.get('expression', '')
                ptype = port.get('porttype', '').upper()
                if expr and 'OUTPUT' in ptype:
                    converted_expr = convert_param_refs(expr, variables)
                    expr_cols.append(f"    {converted_expr} AS {port['name']}")
                elif 'OUTPUT' in ptype:
                    expr_cols.append(f"    {port['name']}")

            if expr_cols and prev_cte:
                cte_sql = f"expr_{t_name} AS (\n    SELECT\n        {prev_cte}.*,\n" + ",\n".join(f"        {c.strip()}" for c in expr_cols) + f"\n    FROM {prev_cte}\n)"
                ctes.append(cte_sql)
                prev_cte = f"expr_{t_name}"

        elif t_type == 'Filter':
            filter_cond = transform.get('attributes', {}).get('Filter Condition', 'TRUE')
            filter_cond = convert_param_refs(filter_cond, variables)
            if prev_cte:
                ctes.append(f"filtered_{t_name} AS (\n    SELECT *\n    FROM {prev_cte}\n    WHERE {filter_cond}\n)")
                prev_cte = f"filtered_{t_name}"

        elif t_type == 'Aggregator':
            group_ports = []
            agg_ports = []
            for port in transform.get('ports', []):
                ptype = port.get('porttype', '').upper()
                expr = port.get('expression', '')
                if 'GROUP BY' in ptype.upper() or (not expr and 'INPUT' in ptype):
                    group_ports.append(port['name'])
                elif expr:
                    agg_ports.append(f"    {convert_param_refs(expr, variables)} AS {port['name']}")

            if prev_cte:
                select_cols = ',\n        '.join(group_ports + [a.strip() for a in agg_ports])
                group_by = ', '.join(group_ports)
                ctes.append(f"agg_{t_name} AS (\n    SELECT\n        {select_cols}\n    FROM {prev_cte}\n    GROUP BY {group_by}\n)")
                prev_cte = f"agg_{t_name}"

        elif t_type in ('Lookup Procedure', 'Lookup'):
            lookup_sql = transform.get('attributes', {}).get('Lookup Sql Override', '')
            lookup_table = transform.get('attributes', {}).get('Lookup table name', '')
            lookup_cond = transform.get('attributes', {}).get('Lookup condition', '')

            return_ports = [p for p in transform.get('ports', []) if 'RETURN' in p.get('porttype', '').upper() or 'OUTPUT' in p.get('porttype', '').upper()]

            if lookup_table and prev_cte:
                lookup_ref = convert_param_refs(lookup_table, variables)
                cond = convert_param_refs(lookup_cond, variables) if lookup_cond else 'TRUE'
                return_cols = ', '.join(f"lkp.{p['name']}" for p in return_ports) if return_ports else 'lkp.*'
                ctes.append(
                    f"lkp_{t_name} AS (\n"
                    f"    SELECT\n        {prev_cte}.*,\n        {return_cols}\n"
                    f"    FROM {prev_cte}\n"
                    f"    LEFT JOIN {lookup_ref} lkp\n"
                    f"        ON {cond}\n)"
                )
                prev_cte = f"lkp_{t_name}"

        elif t_type == 'Joiner':
            join_type = transform.get('attributes', {}).get('Join Type', 'Normal Join')
            join_cond = transform.get('attributes', {}).get('Join Condition', '')
            join_cond = convert_param_refs(join_cond, variables)

            sql_join = 'INNER JOIN' if 'Normal' in join_type else 'LEFT JOIN' if 'Master' in join_type else 'FULL OUTER JOIN'

            if prev_cte:
                ctes.append(
                    f"join_{t_name} AS (\n"
                    f"    SELECT *\n"
                    f"    FROM {prev_cte}\n"
                    f"    {sql_join} -- TODO: specify right-side source\n"
                    f"        ON {join_cond or 'TODO: define join condition'}\n)"
                )
                prev_cte = f"join_{t_name}"

        elif t_type == 'Router':
            for group in transform.get('groups', []):
                g_name = group.get('name', 'default').lower().replace(' ', '_')
                g_expr = group.get('expression', 'TRUE')
                g_expr = convert_param_refs(g_expr, variables)
                if g_name != 'default' and prev_cte:
                    ctes.append(f"route_{g_name} AS (\n    SELECT *\n    FROM {prev_cte}\n    WHERE {g_expr}\n)")

        elif t_type == 'Rank':
            rank_port = None
            group_ports = []
            for port in transform.get('ports', []):
                ptype = port.get('porttype', '').upper()
                if 'RANK' in port.get('name', '').upper() or port.get('expression', ''):
                    rank_port = port['name']
                elif 'GROUP BY' in ptype:
                    group_ports.append(port['name'])

            if prev_cte:
                partition = ', '.join(group_ports) if group_ports else 'NULL'
                order = rank_port or 'NULL'
                ctes.append(
                    f"rank_{t_name} AS (\n"
                    f"    SELECT *,\n"
                    f"        ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) AS rnk\n"
                    f"    FROM {prev_cte}\n)"
                )
                prev_cte = f"rank_{t_name}"

        elif t_type == 'Sorter':
            sort_ports = []
            for port in transform.get('ports', []):
                if port.get('expression', ''):
                    direction = 'DESC' if 'DESC' in port['expression'].upper() else 'ASC'
                    sort_ports.append(f"{port['name']} {direction}")
                else:
                    sort_ports.append(f"{port['name']} ASC")

            if prev_cte and sort_ports:
                order_clause = ', '.join(sort_ports[:5])
                ctes.append(f"sorted_{t_name} AS (\n    SELECT *\n    FROM {prev_cte}\n    ORDER BY {order_clause}\n)")
                prev_cte = f"sorted_{t_name}"

        elif t_type == 'Normalizer':
            if prev_cte:
                ctes.append(
                    f"norm_{t_name} AS (\n"
                    f"    SELECT *\n"
                    f"    FROM {prev_cte}\n"
                    f"    -- TODO: Add LATERAL FLATTEN for normalization\n)"
                )
                prev_cte = f"norm_{t_name}"

        elif t_type == 'Update Strategy':
            pass

        elif t_type == 'Sequence':
            if prev_cte:
                ctes.append(
                    f"seq_{t_name} AS (\n"
                    f"    SELECT *,\n"
                    f"        ROW_NUMBER() OVER (ORDER BY 1) AS generated_seq\n"
                    f"    FROM {prev_cte}\n)"
                )
                prev_cte = f"seq_{t_name}"

        elif t_type == 'Union':
            if prev_cte:
                ctes.append(
                    f"union_{t_name} AS (\n"
                    f"    SELECT * FROM {prev_cte}\n"
                    f"    UNION ALL\n"
                    f"    SELECT * FROM -- TODO: specify second input\n)"
                )
                prev_cte = f"union_{t_name}"

        elif t_type == 'Transaction Control':
            pass

        else:
            if prev_cte:
                ctes.append(
                    f"-- TODO: Unsupported transformation type: {t_type}\n"
                    f"-- Transformation: {transform.get('name', 'unknown')}\n"
                    f"unsupported_{t_name} AS (\n    SELECT * FROM {prev_cte}\n)"
                )
                prev_cte = f"unsupported_{t_name}"

    if target_columns and prev_cte:
        col_list = ',\n        '.join(target_columns)
        ctes.append(f"final AS (\n    SELECT\n        {col_list}\n    FROM {prev_cte}\n)")
    elif prev_cte:
        ctes.append(f"final AS (\n    SELECT *\n    FROM {prev_cte}\n)")

    lines = [config_block, '']

    if merge_hook and merge_hook.startswith('-- TODO'):
        lines.append(merge_hook)
        lines.append('')

    if ctes:
        lines.append('WITH ' + ctes[0] + ',')
        for cte in ctes[1:-1]:
            lines.append('')
            lines.append(cte + ',')
        if len(ctes) > 1:
            lines.append('')
            lines.append(ctes[-1])
    else:
        lines.append(f"-- WARNING: No transformation chain found for {target_table}")
        lines.append(f"SELECT 'TODO' AS placeholder")

    lines.append('')
    lines.append('SELECT * FROM final')

    return '\n'.join(lines)


def generate_schema_yml(handoff: Dict) -> str:
    model_name = handoff.get('_output_model_name', handoff['proposed_model_name'])
    workflow_name = handoff['workflow_name']
    target_table = handoff['target_table']
    field_lineage = handoff.get('field_lineage', [])

    primary_key = infer_primary_key(handoff)

    lines = [
        'models:',
        f'  - name: {model_name}',
        f'    description: >',
        f'      dbt model converted from Informatica workflow {workflow_name},',
        f'      target table {target_table}.',
        f'      Generated by INFA2DBT accelerator v2.2.0.',
        f'    meta:',
        f'      source_workflow: {workflow_name}',
        f'      target_table: {target_table}',
        f"      conversion_timestamp: '{datetime.now(timezone.utc).isoformat()}'",
        f'    columns:',
    ]

    for fl in field_lineage:
        col_name = fl['target_field']
        datatype = fl.get('datatype', 'string')
        source_path = fl.get('source_path', [])
        source_desc = ''
        if source_path:
            src = source_path[-1]
            source_desc = f"Source: {src.get('instance', 'unknown')}.{src.get('field', col_name)}"

        lines.append(f'      - name: {col_name}')
        lines.append(f'        description: "{source_desc}"')
        if datatype:
            lines.append(f'        data_type: {datatype.upper()}')

        is_pk = (col_name == primary_key)
        is_not_null = is_pk or any(
            kw in col_name.upper()
            for kw in ('_FLG', '_USER', '_DTM', '_DT', '_DATE', '_KEY', '_ID')
        )

        if is_pk or is_not_null:
            lines.append(f'        tests:')
            lines.append(f'          - not_null')
            if is_pk:
                lines.append(f'          - unique')

    return '\n'.join(lines)


def generate_unit_tests(handoff: Dict) -> str:
    model_name = handoff['proposed_model_name']
    t_types = handoff.get('transformation_types', [])
    field_lineage = handoff.get('field_lineage', [])
    chain = handoff.get('transformation_chain', [])
    source_tables = handoff.get('source_tables', [])

    tests = []

    sample_row = {}
    for fl in field_lineage[:8]:
        col = fl['target_field']
        dt = fl.get('datatype', 'string').lower()
        if 'timestamp' in dt or 'date' in dt:
            sample_row[col] = '2026-01-01 00:00:00'
        elif 'int' in dt or 'number' in dt:
            sample_row[col] = 1
        else:
            sample_row[col] = f'test_{col.lower()[:10]}'

    if 'Source Qualifier' in t_types:
        sq = next((t for t in chain if t['type'] == 'Source Qualifier'), None)
        sql_override = sq.get('attributes', {}).get('Sql Query', '') if sq else ''

        if "DML_TYPE" in sql_override.upper():
            row_i = dict(sample_row)
            row_i['DML_TYPE'] = 'I'
            row_u = dict(sample_row)
            row_u['DML_TYPE'] = 'U'

            input_rows_str = '          - {' + ', '.join(f'{k}: {_yaml_val(v)}' for k, v in row_i.items()) + '}'
            input_rows_str += '\n          - {' + ', '.join(f'{k}: {_yaml_val(v)}' for k, v in row_u.items()) + '}'

            expect_row = {k: v for k, v in row_i.items() if k != 'DML_TYPE'}
            expect_str = '        - {' + ', '.join(f'{k}: {_yaml_val(v)}' for k, v in expect_row.items()) + '}'

            tests.append(
                f"  - name: test_{model_name}_filter_dml_type\n"
                f"    description: \"Only DML_TYPE='I' rows should pass through.\"\n"
                f"    model: {model_name}\n"
                f"    given:\n"
                f"      - input: source('raw', '{source_tables[0].split('.')[-1] if source_tables else 'source'}')\n"
                f"        format: dict\n"
                f"        rows:\n"
                f"{input_rows_str}\n"
                f"    expect:\n"
                f"      rows:\n"
                f"{expect_str}"
            )
        else:
            input_str = '          - {' + ', '.join(f'{k}: {_yaml_val(v)}' for k, v in sample_row.items()) + '}'
            expect_str = '        - {' + ', '.join(f'{k}: {_yaml_val(v)}' for k, v in sample_row.items()) + '}'
            tests.append(
                f"  - name: test_{model_name}_passthrough\n"
                f"    description: \"All columns pass through correctly.\"\n"
                f"    model: {model_name}\n"
                f"    given:\n"
                f"      - input: source('raw', '{source_tables[0].split('.')[-1] if source_tables else 'source'}')\n"
                f"        format: dict\n"
                f"        rows:\n"
                f"{input_str}\n"
                f"    expect:\n"
                f"      rows:\n"
                f"{expect_str}"
            )

    if 'Filter' in t_types:
        filter_t = next((t for t in chain if t['type'] == 'Filter'), None)
        if filter_t:
            tests.append(
                f"  - name: test_{model_name}_filter\n"
                f"    description: \"Filter transformation applies correctly.\"\n"
                f"    model: {model_name}"
            )

    if 'Aggregator' in t_types:
        tests.append(
            f"  - name: test_{model_name}_aggregation\n"
            f"    description: \"Aggregation produces correct grouped results.\"\n"
            f"    model: {model_name}"
        )

    if not tests:
        input_str = '          - {' + ', '.join(f'{k}: {_yaml_val(v)}' for k, v in sample_row.items()) + '}'
        expect_str = '        - {' + ', '.join(f'{k}: {_yaml_val(v)}' for k, v in sample_row.items()) + '}'
        tests.append(
            f"  - name: test_{model_name}_basic\n"
            f"    description: \"Basic data passthrough test.\"\n"
            f"    model: {model_name}\n"
            f"    given:\n"
            f"      - input: source('raw', '{source_tables[0].split('.')[-1] if source_tables else 'source'}')\n"
            f"        format: dict\n"
            f"        rows:\n"
            f"{input_str}\n"
            f"    expect:\n"
            f"      rows:\n"
            f"{expect_str}"
        )

    return 'unit_tests:\n' + '\n\n'.join(tests)


def _yaml_val(v):
    if isinstance(v, str):
        return f"'{v}'"
    return str(v)


def generate_log(handoff: Dict, tier: int, model_sql_lines: int, unit_test_count: int,
                 quarantine: bool, quarantine_reason: str = '') -> Dict:
    return {
        'model_name': handoff['proposed_model_name'],
        'source_workflow': handoff['workflow_name'],
        'target_table': handoff['target_table'],
        'tier': tier,
        'transformation_types_converted': handoff.get('transformation_types', []),
        'materialization': handoff.get('dbt_config', {}).get('materialization', 'view'),
        'lines_of_sql': model_sql_lines,
        'unit_tests_generated': unit_test_count > 0,
        'unit_test_count': unit_test_count,
        'quarantine_flag': quarantine,
        'quarantine_reason': quarantine_reason,
        'corpus_coverage_status': handoff.get('corpus_coverage_status', 'unknown'),
        'database_type': handoff.get('database_type', 'unknown'),
        'variable_count': len(handoff.get('variables', {})),
        'agent2_version': '2.0.0',
        'generation_timestamp': datetime.now(timezone.utc).isoformat(),
    }


def process_handoff(handoff_path: str, output_dir: str, tier_filter: Optional[int] = None,
                    file_prefix: Optional[str] = None) -> Dict:
    with open(handoff_path) as f:
        handoff = json.load(f)

    tier = classify_tier(handoff)
    original_model_name = handoff['proposed_model_name']
    output_name = file_prefix if file_prefix else original_model_name

    result = {
        'model_name': output_name,
        'original_model_name': original_model_name,
        'tier': tier,
        'status': 'skipped',
        'quarantine': False,
        'quarantine_reason': '',
    }

    if tier_filter is not None and tier != tier_filter:
        return result

    if tier == 3:
        result['status'] = 'quarantined'
        result['quarantine'] = True
        reason_parts = []
        for t in handoff.get('transformation_types', []):
            if t in ESCALATE_TRANSFORMS:
                reason_parts.append(f"Escalate transform: {t}")
        if handoff.get('corpus_coverage_status') == 'missing':
            reason_parts.append("Missing corpus coverage")
        if len(handoff.get('transformation_chain', [])) > 3:
            reason_parts.append(f"Complex chain: {len(handoff['transformation_chain'])} transforms")
        result['quarantine_reason'] = '; '.join(reason_parts) if reason_parts else 'Complex chain'

        log = generate_log(handoff, tier, 0, 0, True, result['quarantine_reason'])
        log_dir = os.path.join(output_dir, 'logs', 'agent2')
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, f"{output_name}.json"), 'w') as f:
            json.dump(log, f, indent=2)

        return result

    handoff['_output_model_name'] = output_name
    model_sql = generate_model_sql(handoff)
    schema_yml = generate_schema_yml(handoff)
    unit_yml = generate_unit_tests(handoff)

    models_dir = os.path.join(output_dir, 'models', 'converted')
    tests_dir = os.path.join(output_dir, 'tests', 'unit')
    log_dir = os.path.join(output_dir, 'logs', 'agent2')
    os.makedirs(models_dir, exist_ok=True)
    os.makedirs(tests_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    with open(os.path.join(models_dir, f"{output_name}.sql"), 'w') as f:
        f.write(model_sql)

    with open(os.path.join(models_dir, f"{output_name}.schema.yml"), 'w') as f:
        f.write(schema_yml)

    with open(os.path.join(tests_dir, f"{output_name}_unit.yml"), 'w') as f:
        f.write(unit_yml)

    sql_lines = len(model_sql.split('\n'))
    test_count = unit_yml.count('- name: test_')

    log = generate_log(handoff, tier, sql_lines, test_count, False)
    log['output_model_name'] = output_name
    log['original_model_name'] = original_model_name
    with open(os.path.join(log_dir, f"{output_name}.json"), 'w') as f:
        json.dump(log, f, indent=2)

    result['status'] = 'converted'
    result['sql_lines'] = sql_lines
    result['test_count'] = test_count
    return result


def find_all_handoffs(handoff_dir: str) -> List[str]:
    handoff_paths = []
    for root, dirs, files in os.walk(handoff_dir):
        for f in files:
            if f.endswith('_handoff.json'):
                handoff_paths.append(os.path.join(root, f))
    return sorted(handoff_paths)


def find_all_handoffs_with_sequence(handoff_dir: str) -> List[Dict]:
    workflow_dirs = []
    for entry in os.listdir(handoff_dir):
        full = os.path.join(handoff_dir, entry)
        if os.path.isdir(full) and entry != 'artifacts':
            csv_path = os.path.join(full, 'workflow_model_mapping.csv')
            if os.path.exists(csv_path):
                workflow_dirs.append((entry, full, csv_path))

    sequenced = []
    for wf_dir_name, wf_dir_path, csv_path in sorted(workflow_dirs):
        with open(csv_path, newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        wf_name = rows[0]['workflow_name'].lower() if rows else wf_dir_name.lower()
        handoff_dir_path = os.path.join(wf_dir_path, 'handoffs')
        if not os.path.isdir(handoff_dir_path):
            continue

        existing_handoffs = {}
        for hf in os.listdir(handoff_dir_path):
            if hf.endswith('_handoff.json'):
                existing_handoffs[hf] = os.path.join(handoff_dir_path, hf)

        for seq_idx, row in enumerate(rows):
            proposed = row.get('proposed_model_name', '')
            handoff_filename = f"{proposed}_handoff.json"
            if handoff_filename in existing_handoffs:
                seq_num = f"{seq_idx + 1:02d}"
                file_prefix = f"{wf_name}_{seq_num}_{proposed}"
                sequenced.append({
                    'handoff_path': existing_handoffs[handoff_filename],
                    'file_prefix': file_prefix,
                    'workflow_name': wf_name,
                    'sequence': seq_idx + 1,
                    'original_model_name': proposed,
                })
                del existing_handoffs[handoff_filename]

        for hf_name, hf_path in sorted(existing_handoffs.items()):
            seq_idx += 1
            proposed = hf_name.replace('_handoff.json', '')
            seq_num = f"{seq_idx + 1:02d}"
            file_prefix = f"{wf_name}_{seq_num}_{proposed}"
            sequenced.append({
                'handoff_path': hf_path,
                'file_prefix': file_prefix,
                'workflow_name': wf_name,
                'sequence': seq_idx + 1,
                'original_model_name': proposed,
            })

    return sequenced


def main():
    parser = argparse.ArgumentParser(description='INFA2DBT Agent 2 Batch Converter')
    parser.add_argument('--handoff-dir', required=True, help='Root directory containing Agent 1 artifacts')
    parser.add_argument('--output-dir', required=True, help='Output directory for dbt models')
    parser.add_argument('--tier', type=int, choices=[1, 2, 3], help='Only process specific tier')
    args = parser.parse_args()

    print(f"Scanning for handoff JSONs in {args.handoff_dir}...")
    sequenced_handoffs = find_all_handoffs_with_sequence(args.handoff_dir)
    print(f"Found {len(sequenced_handoffs)} handoff files (sequenced by workflow)")

    stats = defaultdict(int)
    tier_counts = defaultdict(int)
    quarantine_list = []
    converted_list = []

    for i, item in enumerate(sequenced_handoffs):
        if i % 500 == 0 and i > 0:
            print(f"  Progress: {i}/{len(sequenced_handoffs)} handoffs processed...")

        try:
            result = process_handoff(item['handoff_path'], args.output_dir, args.tier,
                                     file_prefix=item['file_prefix'])
            stats[result['status']] += 1
            tier_counts[result['tier']] += 1

            if result['quarantine']:
                quarantine_list.append({
                    'model_name': result['model_name'],
                    'tier': result['tier'],
                    'reason': result.get('quarantine_reason', ''),
                    'handoff_path': item['handoff_path'],
                })
            elif result['status'] == 'converted':
                converted_list.append({
                    'model_name': result['model_name'],
                    'tier': result['tier'],
                    'sql_lines': result.get('sql_lines', 0),
                    'test_count': result.get('test_count', 0),
                })
        except Exception as e:
            stats['error'] += 1
            quarantine_list.append({
                'model_name': item.get('file_prefix', os.path.basename(item['handoff_path'])),
                'tier': -1,
                'reason': f"Processing error: {str(e)[:200]}",
                'handoff_path': item['handoff_path'],
            })

    quarantine_path = os.path.join(args.output_dir, 'quarantine_list.csv')
    if quarantine_list:
        with open(quarantine_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['model_name', 'tier', 'reason', 'handoff_path'])
            writer.writeheader()
            writer.writerows(quarantine_list)

    summary_path = os.path.join(args.output_dir, 'conversion_summary.json')
    summary = {
        'total_handoffs': len(sequenced_handoffs),
        'converted': stats.get('converted', 0),
        'quarantined': stats.get('quarantined', 0),
        'skipped': stats.get('skipped', 0),
        'errors': stats.get('error', 0),
        'tier_distribution': dict(tier_counts),
        'quarantine_list_path': quarantine_path if quarantine_list else None,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    }
    os.makedirs(args.output_dir, exist_ok=True)
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*60}")
    print("AGENT 2 BATCH CONVERSION SUMMARY")
    print(f"{'='*60}")
    print(f"Total handoffs found:  {len(sequenced_handoffs)}")
    print(f"Tier 1 (simple):       {tier_counts.get(1, 0)}")
    print(f"Tier 2 (standard):     {tier_counts.get(2, 0)}")
    print(f"Tier 3 (complex):      {tier_counts.get(3, 0)}")
    print(f"Converted:             {stats.get('converted', 0)}")
    print(f"Quarantined:           {stats.get('quarantined', 0)}")
    print(f"Skipped:               {stats.get('skipped', 0)}")
    print(f"Errors:                {stats.get('error', 0)}")
    if quarantine_list:
        print(f"Quarantine list:       {quarantine_path}")
    print(f"Summary:               {summary_path}")


if __name__ == '__main__':
    main()
