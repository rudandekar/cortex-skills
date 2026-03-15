#!/usr/bin/env python3
"""Agent 1: Informatica XML Parser - Extracts transformation chains and generates handoff JSONs"""

import xml.etree.ElementTree as ET
import json
import os
from datetime import datetime
from pathlib import Path

def parse_informatica_xml(xml_path: str) -> dict:
    """Parse Informatica PowerCenter XML and extract workflow metadata."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    workflows = []
    
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
        
        targets = []
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
            targets.append(target_info)
        
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
            
            connectors = []
            for conn in mapping.findall('.//CONNECTOR'):
                connectors.append({
                    'from_field': conn.get('FROMFIELD'),
                    'from_instance': conn.get('FROMINSTANCE'),
                    'to_field': conn.get('TOFIELD'),
                    'to_instance': conn.get('TOINSTANCE')
                })
            
            for target in targets:
                workflow_info = {
                    'workflow_name': f"wf_{mapping_name}",
                    'mapping_name': mapping_name,
                    'mapping_description': mapping_desc,
                    'folder_name': folder_name,
                    'target_table': target['name'],
                    'target_schema': target.get('owner', 'PUBLIC'),
                    'proposed_model_name': generate_model_name(target['name']),
                    'source_tables': [s['name'] for s in sources],
                    'sources': sources,
                    'target': target,
                    'transformation_chain': transformations,
                    'transformation_types': list(transformation_types),
                    'connectors': connectors,
                    'field_lineage': build_field_lineage(transformations, connectors, target),
                    'corpus_coverage_status': assess_corpus_coverage(transformation_types)
                }
                workflows.append(workflow_info)
    
    return workflows

def generate_model_name(target_name: str) -> str:
    """Generate DBT model name from target table name."""
    name = target_name.lower().replace(' ', '_')
    if name.startswith(('dim_', 'fact_', 'stg_', 'int_', 'mart_')):
        return name
    if 'dim' in name or 'lookup' in name:
        return f"dim_{name}"
    if 'fact' in name or 'daily' in name or 'history' in name:
        return f"fact_{name}"
    return f"mart_{name}"

def build_field_lineage(transformations: list, connectors: list, target: dict) -> list:
    """Build field-level lineage from transformations to target."""
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

def assess_corpus_coverage(transformation_types: set) -> str:
    """Assess corpus coverage for transformation types."""
    well_covered = {'Source Qualifier', 'Expression', 'Filter', 'Aggregator', 'Lookup'}
    thin_covered = {'Router', 'Joiner', 'Rank', 'Update Strategy'}
    missing_covered = {'Normalizer', 'Custom Transformation', 'Java Transformation'}
    
    for t in transformation_types:
        if t in missing_covered:
            return 'missing'
    for t in transformation_types:
        if t in thin_covered:
            return 'thin'
    return 'ok'

def write_handoff(workflow: dict, output_dir: str) -> str:
    """Write handoff JSON file."""
    filename = f"{workflow['proposed_model_name']}_handoff.json"
    filepath = os.path.join(output_dir, filename)
    
    handoff = {
        **workflow,
        'generated_timestamp': datetime.utcnow().isoformat() + 'Z',
        'agent_version': 'INFA2DBT_v1.1',
        'quarantine_flag': workflow['corpus_coverage_status'] == 'missing'
    }
    
    with open(filepath, 'w') as f:
        json.dump(handoff, f, indent=2)
    
    return filepath

def main():
    xml_dir = "/mnt/c/Users/rudandekar/Downloads/xml"
    output_dir = "/tmp/infa2dbt_test/handoffs"
    
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml')]
    
    print(f"Agent 1: Parsing {len(xml_files)} XML files...")
    print("=" * 60)
    
    all_workflows = []
    for xml_file in xml_files:
        xml_path = os.path.join(xml_dir, xml_file)
        print(f"\nParsing: {xml_file}")
        
        try:
            workflows = parse_informatica_xml(xml_path)
            print(f"  Found {len(workflows)} target tables")
            
            for wf in workflows:
                print(f"    - {wf['proposed_model_name']}")
                print(f"      Sources: {', '.join(wf['source_tables'])}")
                print(f"      Transformations: {', '.join(wf['transformation_types'])}")
                print(f"      Coverage: {wf['corpus_coverage_status']}")
                
                handoff_path = write_handoff(wf, output_dir)
                print(f"      Handoff: {handoff_path}")
                all_workflows.append(wf)
        except Exception as e:
            print(f"  ERROR: {e}")
    
    print("\n" + "=" * 60)
    print(f"Agent 1 Complete: Generated {len(all_workflows)} handoff files")
    
    summary = {
        'total_workflows': len(all_workflows),
        'total_targets': len(all_workflows),
        'transformation_types_found': list(set(t for wf in all_workflows for t in wf['transformation_types'])),
        'coverage_summary': {
            'ok': len([w for w in all_workflows if w['corpus_coverage_status'] == 'ok']),
            'thin': len([w for w in all_workflows if w['corpus_coverage_status'] == 'thin']),
            'missing': len([w for w in all_workflows if w['corpus_coverage_status'] == 'missing'])
        }
    }
    
    with open(os.path.join(output_dir, '_summary.json'), 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\nTransformation types: {', '.join(summary['transformation_types_found'])}")
    print(f"Coverage: OK={summary['coverage_summary']['ok']}, Thin={summary['coverage_summary']['thin']}, Missing={summary['coverage_summary']['missing']}")

if __name__ == '__main__':
    main()
