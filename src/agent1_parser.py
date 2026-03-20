#!/usr/bin/env python3
"""Agent 1: Informatica XML Parser - Extracts transformation chains and generates handoff JSONs

Parses Informatica PowerCenter workflow XML exports and decomposes them into
per-target-table handoff objects for downstream dbt model generation.

Preserves execution sequence from WORKFLOW task ordering (topological sort)
and uses {workflow_name}_{sequence}_{target_table_name} naming convention.

Usage:
    python agent1_parser.py --xml-dir /path/to/xmls --output-dir ./artifacts/handoffs
    python agent1_parser.py --xml-file /path/to/workflow.xml --output-dir ./artifacts/handoffs
"""

import xml.etree.ElementTree as ET
import json
import os
import argparse
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Optional


def topological_sort_sessions(workflow_elem) -> List[str]:
    in_degree = defaultdict(int)
    graph = defaultdict(list)
    all_tasks = set()

    for link in workflow_elem.findall('.//WORKFLOWLINK'):
        from_task = link.get('FROMTASK', '')
        to_task = link.get('TOTASK', '')
        if from_task and to_task:
            graph[from_task].append(to_task)
            in_degree[to_task] += 1
            all_tasks.add(from_task)
            all_tasks.add(to_task)

    for task in all_tasks:
        if task not in in_degree:
            in_degree[task] = 0

    queue = deque(sorted(t for t in all_tasks if in_degree[t] == 0))
    ordered = []

    while queue:
        batch = sorted(queue)
        queue.clear()
        for task in batch:
            if task != 'Start':
                ordered.append(task)
            for neighbor in sorted(graph.get(task, [])):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

    remaining = [t for t in all_tasks if t not in ordered and t != 'Start']
    ordered.extend(sorted(remaining))

    return ordered


def parse_informatica_xml(xml_path: str) -> list:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    if root.tag != 'POWERMART':
        raise ValueError(f"Expected POWERMART root element, got {root.tag}")

    workflows = []
    xml_basename = Path(xml_path).stem.lower().replace(' ', '_').replace('-', '_')

    for folder in root.findall('.//FOLDER'):
        folder_name = folder.get('NAME', 'UNKNOWN')

        sources = []
        for source in folder.findall('.//SOURCE'):
            source_info = {
                'name': source.get('NAME'),
                'database_type': source.get('DATABASETYPE'),
                'owner': source.get('OWNERNAME', ''),
                'fields': []
            }
            for field in source.findall('.//SOURCEFIELD'):
                source_info['fields'].append({
                    'name': field.get('NAME'),
                    'datatype': field.get('DATATYPE'),
                    'precision': field.get('PRECISION'),
                    'scale': field.get('SCALE'),
                    'nullable': field.get('NULLABLE', 'NULL')
                })
            sources.append(source_info)

        targets_by_name = {}
        for target in folder.findall('.//TARGET'):
            target_info = {
                'name': target.get('NAME'),
                'database_type': target.get('DATABASETYPE'),
                'owner': target.get('OWNERNAME', ''),
                'fields': []
            }
            for field in target.findall('.//TARGETFIELD'):
                target_info['fields'].append({
                    'name': field.get('NAME'),
                    'datatype': field.get('DATATYPE'),
                    'precision': field.get('PRECISION'),
                    'scale': field.get('SCALE'),
                    'nullable': field.get('NULLABLE', 'NULL'),
                    'keytype': field.get('KEYTYPE', 'NOT A KEY')
                })
            targets_by_name[target.get('NAME')] = target_info

        mappings_by_name = {}
        for mapping in folder.findall('.//MAPPING'):
            mapping_name = mapping.get('NAME')
            mapping_desc = mapping.get('DESCRIPTION', '')

            transformations = []
            transformation_types = set()

            for transform in mapping.findall('.//TRANSFORMATION'):
                t_name = transform.get('NAME')
                t_type = transform.get('TYPE')
                transformation_types.add(t_type)

                t_info = {
                    'name': t_name,
                    'type': t_type,
                    'description': transform.get('DESCRIPTION', ''),
                    'fields': [],
                    'attributes': {}
                }

                for field in transform.findall('.//TRANSFORMFIELD'):
                    field_info = {
                        'name': field.get('NAME'),
                        'datatype': field.get('DATATYPE'),
                        'porttype': field.get('PORTTYPE'),
                        'expression': field.get('EXPRESSION', '')
                    }
                    t_info['fields'].append(field_info)

                for attr in transform.findall('.//TABLEATTRIBUTE'):
                    t_info['attributes'][attr.get('NAME')] = attr.get('VALUE', '')

                for group in transform.findall('.//GROUP'):
                    if 'groups' not in t_info:
                        t_info['groups'] = []
                    t_info['groups'].append({
                        'name': group.get('NAME'),
                        'expression': group.get('EXPRESSION', '')
                    })

                transformations.append(t_info)

            instance_to_target = {}
            for inst in mapping.findall('.//INSTANCE'):
                if inst.get('TYPE') == 'TARGET':
                    inst_name = inst.get('NAME')
                    trans_name = inst.get('TRANSFORMATION_NAME')
                    if inst_name and trans_name:
                        instance_to_target[inst_name] = trans_name

            connectors = []
            mapping_target_names = set()
            for conn in mapping.findall('.//CONNECTOR'):
                connectors.append({
                    'from_field': conn.get('FROMFIELD'),
                    'from_instance': conn.get('FROMINSTANCE'),
                    'to_field': conn.get('TOFIELD'),
                    'to_instance': conn.get('TOINSTANCE')
                })
                if conn.get('TOINSTANCETYPE') == 'Target Definition':
                    to_inst = conn.get('TOINSTANCE')
                    resolved = instance_to_target.get(to_inst, to_inst)
                    mapping_target_names.add(resolved)

            if not mapping_target_names:
                mapping_target_names = set(targets_by_name.keys())

            mappings_by_name[mapping_name] = {
                'name': mapping_name,
                'description': mapping_desc,
                'transformations': transformations,
                'transformation_types': transformation_types,
                'connectors': connectors,
                'target_names': mapping_target_names
            }

        workflow_name = None
        session_to_mapping = {}
        session_order = []

        for wf in folder.findall('.//WORKFLOW'):
            workflow_name = wf.get('NAME', '')

            for session in wf.findall('.//SESSION'):
                sess_name = session.get('NAME')
                mapping_name = session.get('MAPPINGNAME')
                if sess_name and mapping_name:
                    session_to_mapping[sess_name] = mapping_name

            session_order = topological_sort_sessions(wf)

        if not workflow_name:
            workflow_name = xml_basename

        wf_name_clean = workflow_name.lower().replace(' ', '_').replace('-', '_')

        global_seq = 0
        seen_mappings = set()

        for session_name in session_order:
            mapping_name = session_to_mapping.get(session_name)
            if not mapping_name or mapping_name not in mappings_by_name:
                continue
            if mapping_name in seen_mappings:
                continue
            seen_mappings.add(mapping_name)

            mapping = mappings_by_name[mapping_name]

            for target_name in sorted(mapping['target_names']):
                if target_name not in targets_by_name:
                    continue
                target = targets_by_name[target_name]
                global_seq += 1
                target_clean = target_name.lower().replace(' ', '_').replace('-', '_')
                proposed_model_name = f"{wf_name_clean}_{global_seq:02d}_{target_clean}"

                workflow_info = {
                    'workflow_name': workflow_name,
                    'mapping_name': mapping_name,
                    'mapping_description': mapping['description'],
                    'folder_name': folder_name,
                    'target_table': target_name,
                    'target_schema': target.get('owner', 'PUBLIC'),
                    'proposed_model_name': proposed_model_name,
                    'execution_sequence': global_seq,
                    'session_name': session_name,
                    'source_tables': [s['name'] for s in sources],
                    'sources': sources,
                    'target': target,
                    'transformation_chain': mapping['transformations'],
                    'transformation_types': list(mapping['transformation_types']),
                    'connectors': mapping['connectors'],
                    'field_lineage': build_field_lineage(mapping['transformations'], mapping['connectors'], target),
                    'corpus_coverage_status': assess_corpus_coverage(mapping['transformation_types'])
                }
                workflows.append(workflow_info)

        unlinked_mappings = set(mappings_by_name.keys()) - seen_mappings
        for mapping_name in sorted(unlinked_mappings):
            mapping = mappings_by_name[mapping_name]
            for target_name in sorted(mapping['target_names']):
                if target_name not in targets_by_name:
                    continue
                target = targets_by_name[target_name]
                global_seq += 1
                target_clean = target_name.lower().replace(' ', '_').replace('-', '_')
                proposed_model_name = f"{wf_name_clean}_{global_seq:02d}_{target_clean}"

                workflow_info = {
                    'workflow_name': workflow_name,
                    'mapping_name': mapping_name,
                    'mapping_description': mapping['description'],
                    'folder_name': folder_name,
                    'target_table': target_name,
                    'target_schema': target.get('owner', 'PUBLIC'),
                    'proposed_model_name': proposed_model_name,
                    'execution_sequence': global_seq,
                    'session_name': None,
                    'source_tables': [s['name'] for s in sources],
                    'sources': sources,
                    'target': target,
                    'transformation_chain': mapping['transformations'],
                    'transformation_types': list(mapping['transformation_types']),
                    'connectors': mapping['connectors'],
                    'field_lineage': build_field_lineage(mapping['transformations'], mapping['connectors'], target),
                    'corpus_coverage_status': assess_corpus_coverage(mapping['transformation_types'])
                }
                workflows.append(workflow_info)

    return workflows


def build_field_lineage(transformations: list, connectors: list, target: dict) -> list:
    lineage = []
    for field in target['fields']:
        field_lineage = {
            'target_column': field['name'],
            'source_path': [],
            'transformations_applied': []
        }
        for conn in connectors:
            if conn['to_field'] == field['name']:
                field_lineage['source_path'].append({
                    'from_field': conn['from_field'],
                    'from_instance': conn['from_instance']
                })
        lineage.append(field_lineage)
    return lineage


def assess_corpus_coverage(transformation_types: set, corpus_csv_path: str = None) -> str:
    if corpus_csv_path and os.path.exists(corpus_csv_path):
        import csv
        corpus_counts = {}
        with open(corpus_csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                t_type = row.get('transform_type', row.get('TRANSFORMATION_TYPE', ''))
                count = int(row.get('example_count', row.get('EXAMPLE_COUNT', 0)))
                corpus_counts[t_type] = count

        for t in transformation_types:
            if t not in corpus_counts:
                return 'missing'
            if corpus_counts[t] < 3:
                return 'thin'
        return 'ok'

    well_covered = {'Source Qualifier', 'Expression', 'Filter', 'Aggregator', 'Lookup', 'Lookup Procedure'}
    thin_covered = {'Router', 'Joiner', 'Rank', 'Update Strategy', 'Sequence Generator', 'Sorter', 'Union'}
    escalate = {'Stored Procedure', 'Custom Transformation', 'Java Transformation', 'External Procedure'}

    for t in transformation_types:
        if t in escalate:
            return 'escalate'
    for t in transformation_types:
        if t not in well_covered and t not in thin_covered:
            return 'missing'
    for t in transformation_types:
        if t in thin_covered:
            return 'thin'
    return 'ok'


def write_handoff(workflow: dict, output_dir: str) -> str:
    filename = f"{workflow['proposed_model_name']}_handoff.json"
    filepath = os.path.join(output_dir, filename)

    handoff = {
        **workflow,
        'generated_timestamp': datetime.now(timezone.utc).isoformat(),
        'agent_version': 'INFA2DBT_v2.3.0',
        'quarantine_flag': workflow['corpus_coverage_status'] in ('missing', 'escalate')
    }

    with open(filepath, 'w') as f:
        json.dump(handoff, f, indent=2)

    return filepath


def main():
    parser = argparse.ArgumentParser(
        description='Agent 1: Parse Informatica PowerCenter XML and generate handoff files'
    )
    parser.add_argument('--xml-dir', help='Directory containing XML files to parse')
    parser.add_argument('--xml-file', help='Single XML file to parse')
    parser.add_argument('--output-dir', default='./artifacts/handoffs',
                        help='Output directory for handoff JSON files')
    parser.add_argument('--corpus-csv', help='Path to corpus_coverage.csv for coverage assessment')
    args = parser.parse_args()

    if not args.xml_dir and not args.xml_file:
        parser.error("Either --xml-dir or --xml-file must be provided")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    xml_files = []
    if args.xml_file:
        xml_files = [args.xml_file]
    elif args.xml_dir:
        xml_dir = Path(args.xml_dir)
        if not xml_dir.exists():
            print(f"Error: XML directory not found: {xml_dir}")
            return
        xml_files = [str(f) for f in xml_dir.glob('*.xml')]

    if not xml_files:
        print("No XML files found to process")
        return

    print(f"Agent 1 v2.3.0: Parsing {len(xml_files)} XML files...")
    print("Features: Execution sequence preservation + workflow-prefixed naming")
    print("=" * 60)

    all_workflows = []
    for xml_path in xml_files:
        xml_file = os.path.basename(xml_path)
        print(f"\nParsing: {xml_file}")

        try:
            workflows = parse_informatica_xml(xml_path)
            print(f"  Found {len(workflows)} target tables")

            for wf in workflows:
                if args.corpus_csv:
                    wf['corpus_coverage_status'] = assess_corpus_coverage(
                        set(wf['transformation_types']), args.corpus_csv
                    )

                print(f"    - [{wf['execution_sequence']:02d}] {wf['proposed_model_name']}")
                print(f"      Sources: {', '.join(wf['source_tables'])}")
                print(f"      Transformations: {', '.join(wf['transformation_types'])}")
                print(f"      Coverage: {wf['corpus_coverage_status']}")

                handoff_path = write_handoff(wf, str(output_dir))
                print(f"      Handoff: {handoff_path}")
                all_workflows.append(wf)
        except Exception as e:
            print(f"  ERROR: {e}")

    print("\n" + "=" * 60)
    print(f"Agent 1 v2.3.0 Complete: Generated {len(all_workflows)} handoff files")

    summary = {
        'total_workflows': len(all_workflows),
        'total_targets': len(all_workflows),
        'transformation_types_found': list(set(t for wf in all_workflows for t in wf['transformation_types'])),
        'coverage_summary': {
            'ok': len([w for w in all_workflows if w['corpus_coverage_status'] == 'ok']),
            'thin': len([w for w in all_workflows if w['corpus_coverage_status'] == 'thin']),
            'missing': len([w for w in all_workflows if w['corpus_coverage_status'] == 'missing']),
            'escalate': len([w for w in all_workflows if w['corpus_coverage_status'] == 'escalate'])
        },
        'agent_version': 'INFA2DBT_v2.3.0',
        'generated_timestamp': datetime.now(timezone.utc).isoformat()
    }

    summary_path = output_dir / '_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"\nTransformation types: {', '.join(summary['transformation_types_found'])}")
    cs = summary['coverage_summary']
    print(f"Coverage: OK={cs['ok']}, Thin={cs['thin']}, Missing={cs['missing']}, Escalate={cs['escalate']}")
    print(f"Summary written to: {summary_path}")


if __name__ == '__main__':
    main()
