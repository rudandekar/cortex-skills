#!/usr/bin/env python3
"""
Informatica PowerCenter XML Parser

Parses PowerCenter workflow XML exports and decomposes them into per-target-table
handoff objects for downstream dbt model generation.

Usage:
    python parse_pc_xml.py <workflow_xml_path> [--output-dir artifacts/] [--corpus-csv docs/corpus_coverage.csv]
"""

import xml.etree.ElementTree as ET
import json
import csv
import os
import re
import argparse
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional, Any


class PowerCenterParser:
    """Parses Informatica PowerCenter XML exports."""
    
    STANDARD_TRANSFORMS = {
        'Source Qualifier', 'Expression', 'Lookup', 'Aggregator', 'Router',
        'Joiner', 'Filter', 'Normalizer', 'Rank', 'Update Strategy',
        'Sequence Generator', 'Sorter', 'Union', 'Transaction Control'
    }
    
    ESCALATE_TRANSFORMS = {'Stored Procedure', 'Custom Transformation', 'External Procedure'}
    
    FREQ_PATTERNS = {
        r'_INCR': 'incr',
        r'_FULL': 'full',
        r'_DAILY': 'daily',
        r'_WEEKLY': 'weekly',
        r'_MONTHLY': 'monthly',
        r'_HOURLY': 'hourly',
    }

    def __init__(self, xml_path: str, output_dir: str = 'artifacts/', corpus_csv: str = None):
        self.xml_path = xml_path
        self.output_dir = output_dir
        self.corpus_csv = corpus_csv
        self.corpus_coverage = {}
        
        self.sources: List[Dict] = []
        self.targets: List[Dict] = []
        self.transformations: List[Dict] = []
        self.connectors: List[Dict] = []
        self.sessions: List[Dict] = []
        self.workflows: List[Dict] = []
        self.mappings: List[Dict] = []
        self.mapplets: List[Dict] = []
        
        self.transform_types_found: Set[str] = set()
        self.coverage_status: Dict[str, str] = {}
        self.dependency_graph: Dict[str, List[str]] = defaultdict(list)
        
    def parse(self) -> Dict[str, Any]:
        """Main parsing entry point."""
        self._validate_xml()
        self._load_corpus_coverage()
        self._extract_all_nodes()
        self._check_corpus_coverage()
        self._build_dependency_graph()
        
        targets_data = self._enumerate_targets()
        handoffs = self._generate_handoffs(targets_data)
        execution_order = self._compute_execution_order()
        
        return {
            'workflows': self.workflows,
            'targets': targets_data,
            'handoffs': handoffs,
            'transform_types': list(self.transform_types_found),
            'coverage_status': self.coverage_status,
            'execution_order': execution_order,
            'summary': self._generate_summary()
        }
    
    def _validate_xml(self):
        """Step 1: Validate XML structure."""
        try:
            self.tree = ET.parse(self.xml_path)
            self.root = self.tree.getroot()
        except ET.ParseError as e:
            raise ValueError(f"Malformed XML: {e}")
        
        if self.root.tag != 'POWERMART':
            raise ValueError(f"Expected POWERMART root element, got {self.root.tag}")
        
        repos = list(self.root.iter('REPOSITORY'))
        if not repos:
            raise ValueError("No REPOSITORY element found")
        
        folders = list(self.root.iter('FOLDER'))
        if not folders:
            raise ValueError("No FOLDER element found")
        
        print(f"✓ Valid PowerCenter XML: {len(repos)} repository(s), {len(folders)} folder(s)")
    
    def _load_corpus_coverage(self):
        """Load corpus coverage CSV if provided."""
        if not self.corpus_csv or not os.path.exists(self.corpus_csv):
            print("⚠ No corpus coverage CSV provided - skipping coverage check")
            return
        
        with open(self.corpus_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                t_type = row.get('transform_type', row.get('TRANSFORMATION_TYPE', ''))
                count = int(row.get('example_count', row.get('EXAMPLE_COUNT', 0)))
                self.corpus_coverage[t_type] = count
        
        print(f"✓ Loaded corpus coverage: {len(self.corpus_coverage)} transformation types")
    
    def _extract_all_nodes(self):
        """Step 2: Extract all node types."""
        for source in self.root.iter('SOURCE'):
            self.sources.append({
                'name': source.get('NAME'),
                'dbdname': source.get('DBDNAME'),
                'databasetype': source.get('DATABASETYPE'),
                'ownername': source.get('OWNERNAME'),
                'fields': [f.attrib for f in source.findall('.//SOURCEFIELD')]
            })
        
        for target in self.root.iter('TARGET'):
            self.targets.append({
                'name': target.get('NAME'),
                'dbdname': target.get('DBDNAME'),
                'databasetype': target.get('DATABASETYPE'),
                'ownername': target.get('OWNERNAME'),
                'tablename': target.get('TABLENAME', target.get('NAME')),
                'fields': [f.attrib for f in target.findall('.//TARGETFIELD')]
            })
        
        for transform in self.root.iter('TRANSFORMATION'):
            t_type = transform.get('TYPE')
            self.transform_types_found.add(t_type)
            self.transformations.append({
                'name': transform.get('NAME'),
                'type': t_type,
                'description': transform.get('DESCRIPTION', ''),
                'fields': [f.attrib for f in transform.findall('.//TRANSFORMFIELD')],
                'attributes': {a.get('NAME'): a.get('VALUE') for a in transform.findall('.//TABLEATTRIBUTE')},
                'ports': self._extract_ports(transform)
            })
        
        for connector in self.root.iter('CONNECTOR'):
            self.connectors.append({
                'frominstance': connector.get('FROMINSTANCE'),
                'fromfield': connector.get('FROMFIELD'),
                'toinstance': connector.get('TOINSTANCE'),
                'tofield': connector.get('TOFIELD'),
                'frominstancetype': connector.get('FROMINSTANCETYPE'),
                'toinstancetype': connector.get('TOINSTANCETYPE')
            })
        
        for session in self.root.iter('SESSION'):
            self.sessions.append({
                'name': session.get('NAME'),
                'mappingname': session.get('MAPPINGNAME'),
                'isvalid': session.get('ISVALID'),
                'connections': [c.attrib for c in session.findall('.//CONNECTIONREFERENCE')],
                'extensions': [e.attrib for e in session.findall('.//SESSIONEXTENSION')]
            })
        
        for workflow in self.root.iter('WORKFLOW'):
            tasks = []
            for task in workflow.findall('.//TASK'):
                tasks.append({
                    'name': task.get('NAME'),
                    'type': task.get('TYPE'),
                    'reusable': task.get('REUSABLE')
                })
            
            links = []
            for link in workflow.findall('.//WORKFLOWLINK'):
                links.append({
                    'fromtask': link.get('FROMTASK'),
                    'totask': link.get('TOTASK'),
                    'condition': link.get('CONDITION')
                })
            
            self.workflows.append({
                'name': workflow.get('NAME'),
                'isvalid': workflow.get('ISVALID'),
                'tasks': tasks,
                'links': links
            })
        
        for mapping in self.root.iter('MAPPING'):
            self.mappings.append({
                'name': mapping.get('NAME'),
                'isvalid': mapping.get('ISVALID'),
                'description': mapping.get('DESCRIPTION', '')
            })
        
        for mapplet in self.root.iter('MAPPLET'):
            self.mapplets.append({
                'name': mapplet.get('NAME'),
                'isvalid': mapplet.get('ISVALID'),
                'description': mapplet.get('DESCRIPTION', '')
            })
        
        print(f"✓ Extracted: {len(self.sources)} sources, {len(self.targets)} targets, "
              f"{len(self.transformations)} transformations, {len(self.connectors)} connectors")
    
    def _extract_ports(self, transform) -> List[Dict]:
        """Extract input/output ports from transformation."""
        ports = []
        for field in transform.findall('.//TRANSFORMFIELD'):
            ports.append({
                'name': field.get('NAME'),
                'datatype': field.get('DATATYPE'),
                'porttype': field.get('PORTTYPE'),
                'expression': field.get('EXPRESSION', ''),
                'defaultvalue': field.get('DEFAULTVALUE', '')
            })
        return ports
    
    def _check_corpus_coverage(self):
        """Step 3: Check transformation type corpus coverage."""
        for t_type in self.transform_types_found:
            if t_type in self.ESCALATE_TRANSFORMS:
                self.coverage_status[t_type] = 'escalate'
            elif t_type not in self.corpus_coverage:
                self.coverage_status[t_type] = 'missing'
            elif self.corpus_coverage[t_type] < 3:
                self.coverage_status[t_type] = 'thin'
            else:
                self.coverage_status[t_type] = 'ok'
        
        gaps = [t for t, s in self.coverage_status.items() if s in ('missing', 'thin', 'escalate')]
        if gaps:
            print(f"⚠ Corpus coverage gaps: {gaps}")
    
    def _build_dependency_graph(self):
        """Build mapping dependency graph for execution ordering."""
        for workflow in self.workflows:
            for link in workflow.get('links', []):
                from_task = link['fromtask']
                to_task = link['totask']
                self.dependency_graph[to_task].append(from_task)
    
    def _enumerate_targets(self) -> List[Dict]:
        """Step 4: Enumerate target tables with transformation chains."""
        targets_data = []
        
        for target in self.targets:
            chain = self._trace_chain(target['name'])
            sources = self._find_sources_for_target(target['name'])
            field_lineage = self._build_field_lineage(target)
            
            targets_data.append({
                'target_instance_name': target['name'],
                'target_table_name': target.get('tablename', target['name']),
                'target_schema_name': target.get('dbdname') or target.get('ownername') or 'PUBLIC',
                'database_type': target.get('databasetype', 'Snowflake'),
                'fields': target['fields'],
                'transformation_chain': chain,
                'source_tables': sources,
                'field_lineage': field_lineage,
                'transform_types_in_chain': list(set(t['type'] for t in chain))
            })
        
        return targets_data
    
    def _trace_chain(self, target_name: str) -> List[Dict]:
        """Step 5: Trace transformation chain backwards from target."""
        chain = []
        visited = set()
        queue = [target_name]
        
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            
            incoming = [c for c in self.connectors if c['toinstance'] == current]
            for conn in incoming:
                from_instance = conn['frominstance']
                queue.append(from_instance)
                
                transform = next((t for t in self.transformations if t['name'] == from_instance), None)
                if transform and transform not in chain:
                    chain.append(transform)
        
        chain.reverse()
        return chain
    
    def _find_sources_for_target(self, target_name: str) -> List[str]:
        """Find all source tables that feed into a target."""
        sources = []
        chain = self._trace_chain(target_name)
        
        for transform in chain:
            if transform['type'] == 'Source Qualifier':
                sql_override = transform['attributes'].get('Sql Query', '')
                if sql_override:
                    tables = re.findall(r'FROM\s+([^\s,]+)', sql_override, re.IGNORECASE)
                    sources.extend(tables)
                else:
                    for conn in self.connectors:
                        if conn['toinstance'] == transform['name']:
                            source = next((s for s in self.sources if s['name'] == conn['frominstance']), None)
                            if source:
                                schema = source.get('ownername') or source.get('dbdname') or ''
                                table = source['name']
                                sources.append(f"{schema}.{table}" if schema else table)
        
        return list(set(sources))
    
    def _build_field_lineage(self, target: Dict) -> List[Dict]:
        """Step 6: Build field-level lineage."""
        lineage = []
        for field in target['fields']:
            field_name = field.get('NAME')
            path = self._trace_field_lineage(field_name, target['name'])
            lineage.append({
                'target_field': field_name,
                'datatype': field.get('DATATYPE'),
                'precision': field.get('PRECISION'),
                'scale': field.get('SCALE'),
                'source_path': path
            })
        return lineage
    
    def _trace_field_lineage(self, field_name: str, instance_name: str) -> List[Dict]:
        """Trace a single field's lineage back to source."""
        path = []
        current_field = field_name
        current_instance = instance_name
        visited = set()
        
        while True:
            key = f"{current_instance}.{current_field}"
            if key in visited:
                break
            visited.add(key)
            
            conn = next((c for c in self.connectors 
                        if c['toinstance'] == current_instance and c['tofield'] == current_field), None)
            if not conn:
                break
            
            path.append({
                'instance': conn['frominstance'],
                'field': conn['fromfield']
            })
            
            current_instance = conn['frominstance']
            current_field = conn['fromfield']
        
        return path
    
    def _infer_freq_from_name(self, name: str) -> str:
        """Infer frequency hint from workflow/mapping name."""
        if not name:
            return 'unknown'
        name_upper = name.upper()
        for pattern, freq in self.FREQ_PATTERNS.items():
            if re.search(pattern, name_upper):
                return freq
        return 'unknown'
    
    def _propose_model_name(self, target_table: str, workflow_name: str = '') -> str:
        """Step 7: Generate proposed dbt model name."""
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', target_table.lower())
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')
        
        if any(x in clean_name for x in ('stg', 'staging', 'raw')):
            prefix = 'stg_'
        elif any(x in clean_name for x in ('int', 'intermediate', 'tmp')):
            prefix = 'int_'
        elif any(x in clean_name for x in ('dim', 'fact', 'mart', 'agg')):
            prefix = 'mart_'
        else:
            prefix = 'mart_'
        
        if not clean_name.startswith(prefix.rstrip('_')):
            return f"{prefix}{clean_name}"
        return clean_name
    
    def _compute_execution_order(self) -> List[str]:
        """Step: Compute multi-mapping execution order using topological sort."""
        in_degree = defaultdict(int)
        all_nodes = set()
        
        for workflow in self.workflows:
            all_nodes.add(workflow['name'])
            for link in workflow.get('links', []):
                all_nodes.add(link['fromtask'])
                all_nodes.add(link['totask'])
                in_degree[link['totask']] += 1
        
        queue = [n for n in all_nodes if in_degree[n] == 0]
        order = []
        
        while queue:
            node = queue.pop(0)
            order.append(node)
            
            for workflow in self.workflows:
                for link in workflow.get('links', []):
                    if link['fromtask'] == node:
                        in_degree[link['totask']] -= 1
                        if in_degree[link['totask']] == 0:
                            queue.append(link['totask'])
        
        if len(order) < len(all_nodes):
            print("⚠ Circular dependency detected in workflow")
        
        return order
    
    def _generate_handoffs(self, targets_data: List[Dict]) -> List[Dict]:
        """Step 12: Generate handoff objects for Agent 2."""
        handoffs = []
        workflow_name = self.workflows[0]['name'] if self.workflows else 'unknown'
        
        for target in targets_data:
            model_name = self._propose_model_name(target['target_table_name'], workflow_name)
            freq_hint = self._infer_freq_from_name(workflow_name)
            
            chain_coverage = 'ok'
            for t_type in target['transform_types_in_chain']:
                status = self.coverage_status.get(t_type, 'missing')
                if status in ('missing', 'escalate'):
                    chain_coverage = 'missing'
                    break
                elif status == 'thin' and chain_coverage == 'ok':
                    chain_coverage = 'thin'
            
            handoff = {
                'workflow_name': workflow_name,
                'mapping_name': self.mappings[0]['name'] if self.mappings else workflow_name,
                'target_table': target['target_table_name'],
                'target_schema': target['target_schema_name'],
                'proposed_model_name': model_name,
                'model_filepath': f"models/converted/{model_name}.sql",
                'transformation_chain': target['transformation_chain'],
                'source_tables': target['source_tables'],
                'field_lineage': target['field_lineage'],
                'transformation_types': target['transform_types_in_chain'],
                'corpus_coverage_status': chain_coverage,
                'freq_hint': freq_hint,
                'dbt_config': {
                    'materialization': self._infer_materialization(target, freq_hint),
                    'schema': self._infer_dbt_schema(target),
                    'tags': self._infer_tags(workflow_name, freq_hint),
                    'pre_hook': [],
                    'post_hook': []
                },
                'pipeline_config': {
                    'fidelity_threshold': 0.70 if chain_coverage == 'thin' else 0.85,
                    'max_retries': 3,
                    'quarantine_on_missing': chain_coverage == 'missing'
                },
                'agent1_timestamp': datetime.utcnow().isoformat(),
                'agent1_version': '2.0.0'
            }
            handoffs.append(handoff)
        
        return handoffs
    
    def _infer_materialization(self, target: Dict, freq_hint: str) -> str:
        """Infer dbt materialization type."""
        transform_types = set(target.get('transform_types_in_chain', []))
        
        if 'Update Strategy' in transform_types:
            return 'incremental'
        if freq_hint == 'incr':
            return 'incremental'
        if freq_hint in ('daily', 'weekly', 'monthly'):
            return 'table'
        if any(x in target['target_table_name'].lower() for x in ('dim', 'fact')):
            return 'table'
        return 'view'
    
    def _infer_dbt_schema(self, target: Dict) -> str:
        """Infer dbt schema strategy."""
        schema = target.get('target_schema_name', 'PUBLIC')
        if schema.upper() in ('EDW', 'DW', 'WAREHOUSE'):
            return 'ANALYTICS'
        if 'STG' in schema.upper() or 'STAGING' in schema.upper():
            return 'STAGING'
        return schema
    
    def _infer_tags(self, workflow_name: str, freq_hint: str) -> List[str]:
        """Infer dbt tags from workflow metadata."""
        tags = []
        
        if freq_hint != 'unknown':
            tags.append(freq_hint)
        
        name_lower = workflow_name.lower()
        if 'customer' in name_lower or 'cust' in name_lower:
            tags.append('customer')
        elif 'product' in name_lower or 'prod' in name_lower:
            tags.append('product')
        elif 'sales' in name_lower or 'order' in name_lower:
            tags.append('sales')
        elif 'finance' in name_lower or 'fin' in name_lower:
            tags.append('finance')
        
        tags.append('infa2dbt')
        return tags
    
    def _generate_summary(self) -> Dict:
        """Generate parsing summary."""
        return {
            'workflows_processed': len(self.workflows),
            'mappings_found': len(self.mappings),
            'targets_extracted': len(self.targets),
            'sources_found': len(self.sources),
            'transformations_found': len(self.transformations),
            'transform_types': list(self.transform_types_found),
            'coverage_gaps': [t for t, s in self.coverage_status.items() if s != 'ok'],
            'models_ready': len([t for t in self.targets if self.coverage_status.get(t.get('name'), 'ok') != 'escalate'])
        }
    
    def write_outputs(self, result: Dict):
        """Write all output artifacts."""
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, 'handoffs'), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, 'logs'), exist_ok=True)
        
        mapping_rows = []
        for handoff in result['handoffs']:
            mapping_rows.append({
                'workflow_name': handoff['workflow_name'],
                'target_table': handoff['target_table'],
                'target_schema': handoff['target_schema'],
                'proposed_model_name': handoff['proposed_model_name'],
                'model_filepath': handoff['model_filepath'],
                'transformation_types': ','.join(handoff['transformation_types']),
                'source_tables': ','.join(handoff['source_tables']),
                'corpus_coverage_status': handoff['corpus_coverage_status'],
                'materialization': handoff['dbt_config']['materialization'],
                'agent1_timestamp': handoff['agent1_timestamp'],
                'agent1_status': 'complete' if handoff['corpus_coverage_status'] != 'missing' else 'escalated'
            })
        
        mapping_path = os.path.join(self.output_dir, 'workflow_model_mapping.csv')
        if mapping_rows:
            with open(mapping_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=mapping_rows[0].keys())
                writer.writeheader()
                writer.writerows(mapping_rows)
            print(f"✓ Wrote {mapping_path}")
        
        for handoff in result['handoffs']:
            handoff_path = os.path.join(self.output_dir, 'handoffs', f"{handoff['proposed_model_name']}_handoff.json")
            with open(handoff_path, 'w') as f:
                json.dump(handoff, f, indent=2)
            print(f"✓ Wrote {handoff_path}")
        
        run_log = {
            'xml_path': self.xml_path,
            'timestamp': datetime.utcnow().isoformat(),
            'summary': result['summary'],
            'execution_order': result['execution_order'],
            'coverage_status': self.coverage_status
        }
        log_path = os.path.join(self.output_dir, 'logs', 'agent1_run.json')
        with open(log_path, 'w') as f:
            json.dump(run_log, f, indent=2)
        print(f"✓ Wrote {log_path}")


def main():
    parser = argparse.ArgumentParser(description='Parse Informatica PowerCenter XML')
    parser.add_argument('xml_path', help='Path to PowerCenter XML file')
    parser.add_argument('--output-dir', default='artifacts/', help='Output directory')
    parser.add_argument('--corpus-csv', help='Path to corpus coverage CSV')
    args = parser.parse_args()
    
    pc_parser = PowerCenterParser(args.xml_path, args.output_dir, args.corpus_csv)
    result = pc_parser.parse()
    pc_parser.write_outputs(result)
    
    print("\n" + "="*60)
    print("PARSING SUMMARY")
    print("="*60)
    summary = result['summary']
    print(f"Workflows processed: {summary['workflows_processed']}")
    print(f"Target tables: {summary['targets_extracted']}")
    print(f"Transform types: {summary['transform_types']}")
    print(f"Coverage gaps: {summary['coverage_gaps']}")
    print(f"Models ready for Agent 2: {summary['models_ready']}")
    
    return result


if __name__ == '__main__':
    main()
