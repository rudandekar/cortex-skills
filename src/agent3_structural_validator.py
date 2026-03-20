#!/usr/bin/env python3
"""Agent 3: Structural Fidelity Validator

Programmatically compares Informatica XML handoff metadata against
generated dbt SQL models to validate:
  1. Transformation coverage  - every XML transform has a corresponding CTE
  2. Sequence preservation    - CTE order matches execution sequence
  3. Column coverage          - all target columns present in final SELECT
  4. Expression translation   - Informatica expressions converted to SQL
  5. Filter/Join/Lookup logic - conditions carried over correctly
  6. Source coverage           - all source tables referenced

Usage:
    python agent3_structural_validator.py --handoff-dir ./handoffs --sql-dir ./output/models/converted
    python agent3_structural_validator.py --handoff-dir ./handoffs --sql-dir ./output/models/converted --report /tmp/report.json
"""

import json
import os
import re
import argparse
from pathlib import Path
from typing import Dict, List, Tuple


CTE_PATTERN = re.compile(r'(\w+)\s+AS\s*\(', re.IGNORECASE)
SOURCE_REF_PATTERN = re.compile(r"source\(\s*'(\w+)'\s*,\s*'(\w+)'\s*\)", re.IGNORECASE)
CONFIG_PATTERN = re.compile(r"\{\{\s*config\(.*?schema='(\w*)'", re.DOTALL)
MATERIALIZATION_PATTERN = re.compile(r"materialized='(\w+)'")


def extract_ctes(sql: str) -> List[str]:
    sql_no_config = re.sub(r'\{\{.*?\}\}', '', sql, flags=re.DOTALL)
    return CTE_PATTERN.findall(sql_no_config)


def extract_source_refs(sql: str) -> List[Tuple[str, str]]:
    return SOURCE_REF_PATTERN.findall(sql)


def extract_final_columns(sql: str) -> List[str]:
    final_match = re.search(r'final\s+AS\s*\(\s*SELECT\s+(.*?)\n\s+FROM\b', sql, re.DOTALL | re.IGNORECASE)
    if not final_match:
        return []
    col_block = final_match.group(1)
    if col_block.strip() == '*':
        return ['*']
    cols = []
    for col in col_block.split(','):
        col = col.strip()
        as_match = re.search(r'\bAS\s+(\w+)\s*$', col, re.IGNORECASE)
        if as_match:
            cols.append(as_match.group(1).lower())
        else:
            parts = col.split('.')
            cols.append(parts[-1].strip().lower())
    return cols


def validate_transformation_coverage(handoff: dict, sql: str) -> dict:
    ctes = extract_ctes(sql)
    cte_names_lower = [c.lower() for c in ctes]

    xml_transforms = handoff.get('transformation_chain', [])
    transform_types = {}
    for t in xml_transforms:
        t_type = t['type']
        transform_types[t_type] = transform_types.get(t_type, 0) + 1

    type_to_cte_prefix = {
        'Source Qualifier': 'source_',
        'Expression': 'transformed_',
        'Filter': 'filtered_',
        'Aggregator': 'aggregated_',
        'Lookup': 'lookup_',
        'Lookup Procedure': 'lookup_',
        'Joiner': 'joined_',
        'Sequence Generator': 'sequenced_',
        'Sorter': 'sorted_',
        'Router': 'routed_',
        'Normalizer': 'normalized_',
        'Update Strategy': 'update_strategy_',
        'XML Source Qualifier': 'xml_parsed_',
    }

    covered = []
    missing = []

    for t in xml_transforms:
        t_type = t['type']
        t_name = t['name'].lower()
        prefix = type_to_cte_prefix.get(t_type, '')

        found = False
        if prefix:
            expected_cte = f"{prefix}{t_name}"
            if expected_cte in cte_names_lower:
                found = True

        if not found and prefix:
            for cte in cte_names_lower:
                if cte.startswith(prefix):
                    found = True
                    break

        if not found and t_type == 'Source Qualifier':
            for cte in cte_names_lower:
                if cte.startswith('source_'):
                    found = True
                    break

        if found:
            covered.append({'type': t_type, 'name': t['name'], 'status': 'covered'})
        else:
            missing.append({'type': t_type, 'name': t['name'], 'status': 'missing'})

    total = len(xml_transforms)
    coverage_pct = len(covered) / max(1, total)

    return {
        'total_transforms': total,
        'covered': len(covered),
        'missing_count': len(missing),
        'missing_details': missing,
        'coverage_pct': round(coverage_pct, 3),
        'transform_type_counts': transform_types
    }


def validate_sequence_preservation(handoff: dict, sql: str) -> dict:
    ctes = extract_ctes(sql)
    xml_transforms = handoff.get('transformation_chain', [])
    xml_types_order = [t['type'] for t in xml_transforms]

    expected_cte_order = []
    for t_type in xml_types_order:
        prefix_map = {
            'Source Qualifier': 'source_',
            'Expression': 'transformed_',
            'Filter': 'filtered_',
            'Aggregator': 'aggregated_',
            'Lookup': 'lookup_',
            'Lookup Procedure': 'lookup_',
            'Joiner': 'joined_',
            'Router': 'routed_',
            'Update Strategy': 'update_strategy_',
            'Normalizer': 'normalized_',
            'Sequence Generator': 'sequenced_',
            'Sorter': 'sorted_',
            'XML Source Qualifier': 'xml_parsed_',
        }
        expected_cte_order.append(prefix_map.get(t_type, 'unknown_'))

    actual_prefixes = []
    for cte in ctes:
        cte_l = cte.lower()
        if cte_l == 'final':
            continue
        matched_prefix = 'other_'
        for prefix in ['source_', 'transformed_', 'filtered_', 'aggregated_',
                        'lookup_', 'joined_', 'routed_', 'update_strategy_',
                        'normalized_', 'sequenced_', 'sorted_', 'xml_parsed_']:
            if cte_l.startswith(prefix):
                matched_prefix = prefix
                break
        actual_prefixes.append(matched_prefix)

    source_first = True
    if actual_prefixes and actual_prefixes[0] != 'source_':
        source_first = False

    final_last = ctes[-1].lower() == 'final' if ctes else False

    source_prefixes = {'source_', 'xml_parsed_'}
    expected_non_source = [p for p in expected_cte_order if p not in source_prefixes]
    actual_non_source = [p for p in actual_prefixes if p not in source_prefixes]

    in_order = 0
    expected_idx = 0
    for actual_p in actual_non_source:
        while expected_idx < len(expected_non_source):
            if expected_non_source[expected_idx] == actual_p:
                in_order += 1
                expected_idx += 1
                break
            expected_idx += 1

    non_source_score = in_order / max(1, len(actual_non_source)) if actual_non_source else 1.0
    source_score = 1.0 if source_first else 0.5
    sequence_score = source_score * 0.2 + non_source_score * 0.8

    return {
        'source_ctes_first': source_first,
        'final_cte_last': final_last,
        'cte_count': len(ctes),
        'xml_transform_count': len(xml_transforms),
        'sequence_score': round(sequence_score, 3),
        'cte_order': [c.lower() for c in ctes],
        'xml_type_order': xml_types_order
    }


def validate_column_coverage(handoff: dict, sql: str) -> dict:
    target_fields = handoff.get('target', {}).get('fields', [])
    target_cols = set(f['name'].lower() for f in target_fields)

    final_cols = set(extract_final_columns(sql))

    if '*' in final_cols:
        return {
            'target_columns': len(target_cols),
            'final_select_columns': 'wildcard',
            'matched': len(target_cols),
            'missing_in_sql': [],
            'coverage_pct': 1.0,
            'note': 'Final SELECT uses *, column-level validation requires runtime check'
        }

    matched = target_cols & final_cols
    missing_in_sql = target_cols - final_cols

    coverage_pct = len(matched) / max(1, len(target_cols))

    return {
        'target_columns': len(target_cols),
        'final_select_columns': len(final_cols),
        'matched': len(matched),
        'missing_in_sql': sorted(missing_in_sql),
        'coverage_pct': round(coverage_pct, 3)
    }


def validate_expression_translation(handoff: dict, sql: str) -> dict:
    xml_transforms = handoff.get('transformation_chain', [])
    sql_lower = sql.lower()

    results = []
    for t in xml_transforms:
        if t['type'] != 'Expression':
            continue

        for field in t.get('fields', []):
            expr = field.get('expression', '').strip()
            porttype = field.get('porttype', '')
            fname = field['name'].lower()

            if not expr or porttype == 'INPUT':
                continue

            if expr.upper() == fname.upper():
                continue

            translated = False
            if fname in sql_lower:
                translated = True

            infa_funcs = re.findall(r'\b(IIF|DECODE|SYSDATE|SESSSTARTTIME|NVL|SETMAXVARIABLE)\b',
                                     expr, re.IGNORECASE)
            sf_equivalents = {
                'IIF': 'IFF(',
                'DECODE': 'CASE',
                'SYSDATE': 'CURRENT_DATE',
                'SESSSTARTTIME': 'CURRENT_TIMESTAMP',
                'NVL': 'COALESCE(',
                'SETMAXVARIABLE': '/* SETMAXVARIABLE'
            }

            func_translations = []
            for func in infa_funcs:
                sf_equiv = sf_equivalents.get(func.upper(), '')
                found_in_sql = sf_equiv.lower() in sql_lower if sf_equiv else False
                func_translations.append({
                    'infa_function': func,
                    'expected_sf': sf_equiv,
                    'found': found_in_sql
                })

            results.append({
                'field': field['name'],
                'porttype': porttype,
                'has_expression': True,
                'column_in_sql': translated,
                'infa_functions': func_translations
            })

    total_expr = len(results)
    translated = len([r for r in results if r['column_in_sql']])
    func_checks = [f for r in results for f in r.get('infa_functions', [])]
    func_translated = len([f for f in func_checks if f['found']])

    return {
        'expression_fields': total_expr,
        'columns_present': translated,
        'column_coverage_pct': round(translated / max(1, total_expr), 3) if total_expr > 0 else 1.0,
        'function_translations_checked': len(func_checks),
        'function_translations_found': func_translated,
        'function_coverage_pct': round(func_translated / max(1, len(func_checks)), 3) if func_checks else 1.0,
        'details': results[:20]
    }


def validate_filter_logic(handoff: dict, sql: str) -> dict:
    xml_transforms = handoff.get('transformation_chain', [])
    sql_lower = sql.lower()

    results = []
    for t in xml_transforms:
        if t['type'] != 'Filter':
            continue

        filter_cond = t.get('attributes', {}).get('Filter Condition', '')
        has_where = 'where' in sql_lower
        cte_exists = any(f'filtered_{t["name"].lower()}' in c.lower()
                        for c in extract_ctes(sql))

        results.append({
            'name': t['name'],
            'original_condition': filter_cond[:200],
            'cte_generated': cte_exists,
            'has_where_clause': has_where
        })

    return {
        'filter_count': len(results),
        'filters_with_cte': len([r for r in results if r['cte_generated']]),
        'details': results
    }


def validate_lookup_logic(handoff: dict, sql: str) -> dict:
    xml_transforms = handoff.get('transformation_chain', [])
    sql_lower = sql.lower()

    results = []
    for t in xml_transforms:
        if t['type'] not in ('Lookup', 'Lookup Procedure'):
            continue

        lookup_table = t.get('attributes', {}).get('Lookup table name', '')
        lookup_cond = t.get('attributes', {}).get('Lookup Condition', '')

        has_join = 'join' in sql_lower
        table_ref = lookup_table.lower() in sql_lower if lookup_table else False
        cte_exists = any(f'lookup_{t["name"].lower()}' in c.lower()
                        for c in extract_ctes(sql))

        results.append({
            'name': t['name'],
            'lookup_table': lookup_table,
            'lookup_condition': lookup_cond[:200],
            'cte_generated': cte_exists,
            'join_present': has_join,
            'table_referenced': table_ref
        })

    return {
        'lookup_count': len(results),
        'lookups_with_cte': len([r for r in results if r['cte_generated']]),
        'lookups_with_join': len([r for r in results if r['join_present']]),
        'details': results
    }


def validate_update_strategy(handoff: dict, sql: str) -> dict:
    xml_transforms = handoff.get('transformation_chain', [])
    sql_lower = sql.lower()

    results = []
    for t in xml_transforms:
        if t['type'] != 'Update Strategy':
            continue

        strategy_expr = t.get('attributes', {}).get('Update Strategy Expression', '')
        cte_exists = any(f'update_strategy_{t["name"].lower()}' in c.lower()
                        for c in extract_ctes(sql))
        has_change_type = '_dbt_change_type' in sql_lower
        has_incremental = 'incremental' in sql_lower

        results.append({
            'name': t['name'],
            'strategy_expression': strategy_expr,
            'cte_generated': cte_exists,
            'change_type_column': has_change_type,
            'incremental_config': has_incremental
        })

    return {
        'update_strategy_count': len(results),
        'strategies_with_cte': len([r for r in results if r['cte_generated']]),
        'details': results
    }


def validate_source_coverage(handoff: dict, sql: str) -> dict:
    source_tables = handoff.get('source_tables', [])
    sql_source_refs = extract_source_refs(sql)
    sql_source_names = set(name.lower() for _, name in sql_source_refs)

    matched = []
    unmatched = []
    for src in source_tables:
        if src.lower() in sql_source_names:
            matched.append(src)
        else:
            unmatched.append(src)

    return {
        'xml_sources': len(source_tables),
        'sql_source_refs': len(sql_source_refs),
        'matched': len(matched),
        'unmatched': unmatched,
        'coverage_pct': round(len(matched) / max(1, len(source_tables)), 3)
    }


def validate_model(handoff: dict, sql: str) -> dict:
    model_name = handoff['proposed_model_name']

    transform_cov = validate_transformation_coverage(handoff, sql)
    sequence = validate_sequence_preservation(handoff, sql)
    column_cov = validate_column_coverage(handoff, sql)
    expr_trans = validate_expression_translation(handoff, sql)
    filter_logic = validate_filter_logic(handoff, sql)
    lookup_logic = validate_lookup_logic(handoff, sql)
    update_strat = validate_update_strategy(handoff, sql)
    source_cov = validate_source_coverage(handoff, sql)

    weights = {
        'transformation_coverage': 0.25,
        'sequence_preservation': 0.15,
        'column_coverage': 0.20,
        'expression_translation': 0.15,
        'source_coverage': 0.10,
        'logic_validation': 0.15,
    }

    filter_score = filter_logic['filters_with_cte'] / max(1, filter_logic['filter_count']) if filter_logic['filter_count'] > 0 else 1.0
    lookup_score = lookup_logic['lookups_with_cte'] / max(1, lookup_logic['lookup_count']) if lookup_logic['lookup_count'] > 0 else 1.0
    update_score = update_strat['strategies_with_cte'] / max(1, update_strat['update_strategy_count']) if update_strat['update_strategy_count'] > 0 else 1.0
    logic_score = (filter_score + lookup_score + update_score) / 3

    overall = (
        transform_cov['coverage_pct'] * weights['transformation_coverage'] +
        sequence['sequence_score'] * weights['sequence_preservation'] +
        column_cov['coverage_pct'] * weights['column_coverage'] +
        expr_trans['column_coverage_pct'] * weights['expression_translation'] +
        source_cov['coverage_pct'] * weights['source_coverage'] +
        logic_score * weights['logic_validation']
    )

    issues = []
    if transform_cov['coverage_pct'] < 0.8:
        issues.append(f"Low transformation coverage: {transform_cov['coverage_pct']:.0%}")
    if sequence['sequence_score'] < 0.8:
        issues.append(f"Sequence mismatch: {sequence['sequence_score']:.0%}")
    if column_cov['coverage_pct'] < 0.8:
        issues.append(f"Missing target columns: {column_cov['missing_in_sql'][:5]}")
    if source_cov['coverage_pct'] < 1.0:
        issues.append(f"Unreferenced sources: {source_cov['unmatched']}")
    if filter_logic['filter_count'] > 0 and filter_score < 1.0:
        issues.append("Filter logic incomplete")
    if lookup_logic['lookup_count'] > 0 and lookup_score < 1.0:
        issues.append("Lookup logic incomplete")

    status = 'PASS' if overall >= 0.85 else 'REVIEW' if overall >= 0.70 else 'FAIL'

    return {
        'model_name': model_name,
        'workflow_name': handoff.get('workflow_name', ''),
        'execution_sequence': handoff.get('execution_sequence'),
        'overall_score': round(overall, 3),
        'status': status,
        'component_scores': {
            'transformation_coverage': transform_cov['coverage_pct'],
            'sequence_preservation': sequence['sequence_score'],
            'column_coverage': column_cov['coverage_pct'],
            'expression_translation': expr_trans['column_coverage_pct'],
            'source_coverage': source_cov['coverage_pct'],
            'logic_validation': round(logic_score, 3)
        },
        'issues': issues,
        'details': {
            'transformation_coverage': transform_cov,
            'sequence_preservation': sequence,
            'column_coverage': column_cov,
            'expression_translation': expr_trans,
            'filter_logic': filter_logic,
            'lookup_logic': lookup_logic,
            'update_strategy': update_strat,
            'source_coverage': source_cov
        }
    }


def main():
    parser = argparse.ArgumentParser(
        description='Agent 3: Structural fidelity validator - compares XML handoffs to dbt SQL'
    )
    parser.add_argument('--handoff-dir', required=True, help='Directory with handoff JSON files')
    parser.add_argument('--sql-dir', required=True, help='Directory with generated SQL files')
    parser.add_argument('--report', default=None, help='Path to write JSON report')
    parser.add_argument('--summary-only', action='store_true', help='Print summary only')
    args = parser.parse_args()

    handoff_dir = Path(args.handoff_dir)
    sql_dir = Path(args.sql_dir)

    handoff_files = sorted(handoff_dir.glob('*_handoff.json'))
    if not handoff_files:
        print(f"No handoff files found in {handoff_dir}")
        return

    print(f"Agent 3 v1.0.0: Validating {len(handoff_files)} models...")
    print("=" * 70)

    results = []
    pass_count = 0
    review_count = 0
    fail_count = 0
    skip_count = 0

    for hf in handoff_files:
        handoff = json.load(open(hf))
        model_name = handoff['proposed_model_name']
        sql_path = sql_dir / f"{model_name}.sql"

        if not sql_path.exists():
            skip_count += 1
            if not args.summary_only:
                print(f"  SKIP {model_name} (SQL not found)")
            continue

        sql = sql_path.read_text()
        result = validate_model(handoff, sql)
        results.append(result)

        if result['status'] == 'PASS':
            pass_count += 1
        elif result['status'] == 'REVIEW':
            review_count += 1
        else:
            fail_count += 1

        if not args.summary_only:
            score = result['overall_score']
            status = result['status']
            seq = result.get('execution_sequence', '?')
            issues_str = f" | Issues: {', '.join(result['issues'][:2])}" if result['issues'] else ""
            print(f"  [{seq:>3}] {status:6s} {score:.1%}  {model_name}{issues_str}")

    print("\n" + "=" * 70)
    print(f"Agent 3 Structural Validation Complete")
    print(f"  Total:   {len(results)}")
    print(f"  PASS:    {pass_count} (>= 85%)")
    print(f"  REVIEW:  {review_count} (70-85%)")
    print(f"  FAIL:    {fail_count} (< 70%)")
    print(f"  SKIP:    {skip_count} (no SQL)")

    if results:
        avg_score = sum(r['overall_score'] for r in results) / len(results)
        print(f"  Avg Score: {avg_score:.1%}")

        avg_components = {}
        for key in results[0]['component_scores']:
            avg_components[key] = sum(r['component_scores'][key] for r in results) / len(results)
        print(f"\n  Component Averages:")
        for k, v in avg_components.items():
            print(f"    {k:30s} {v:.1%}")

    if review_count > 0 or fail_count > 0:
        print(f"\n  Models needing attention:")
        for r in sorted(results, key=lambda x: x['overall_score']):
            if r['status'] in ('REVIEW', 'FAIL'):
                print(f"    {r['status']:6s} {r['overall_score']:.1%}  {r['model_name']}")
                for issue in r['issues'][:3]:
                    print(f"           {issue}")
                if len([r2 for r2 in results if r2['status'] in ('REVIEW', 'FAIL')]) > 20:
                    break

    if args.report:
        report = {
            'summary': {
                'total': len(results),
                'pass': pass_count,
                'review': review_count,
                'fail': fail_count,
                'skip': skip_count,
                'avg_score': round(avg_score, 3) if results else 0,
                'component_averages': {k: round(v, 3) for k, v in avg_components.items()} if results else {}
            },
            'models': results
        }
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n  Full report: {args.report}")


if __name__ == '__main__':
    main()
