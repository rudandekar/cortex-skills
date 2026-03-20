#!/usr/bin/env python3
"""
Informatica PowerCenter XML Parser — v3.0

Parses PowerCenter workflow XML exports and decomposes them into per-target-table
handoff objects for downstream dbt model generation.

v3.0 changelog (gap remediation from corpus scan of 34,503 XMLs):
  P0: Fixed transform type name mismatches (Lookup→Lookup Procedure, Sequence Generator→Sequence)
  P0: Added INSTANCE element extraction for connector graph accuracy
  P1: Added WORKLET, SHORTCUT, FLATFILE, ASSOCIATED_SOURCE_INSTANCE extraction
  P1: Added MAPPINGVARIABLE, WORKFLOWVARIABLE, Command task, TARGETLOADORDER
  P1: Added missing transform types (XML Source Qualifier, Input/Output Transformation, Mapplet)
  P2: Added CONFIG, PARTITION, SESSIONCOMPONENT, SCHEDULER, GROUP, SESSTRANSFORMATIONINST
  P3: Added ERPINFO, METADATAEXTENSION, FIELDDEPENDENCY, malformed XML pre-processor

Usage:
    python parse_pc_xml.py <workflow_xml_path> [--output-dir artifacts/] [--corpus-csv docs/corpus_coverage.csv]
    python parse_pc_xml.py --batch <directory> [--output-dir artifacts/] [--corpus-csv docs/corpus_coverage.csv]
"""

import xml.etree.ElementTree as ET
import json
import csv
import os
import re
import html
import argparse
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional, Any


class PowerCenterParser:
    """Parses Informatica PowerCenter XML exports."""

    STANDARD_TRANSFORMS = {
        'Source Qualifier', 'Expression', 'Lookup Procedure', 'Aggregator', 'Router',
        'Joiner', 'Filter', 'Normalizer', 'Rank', 'Update Strategy',
        'Sequence', 'Sorter', 'Union', 'Transaction Control',
        'XML Source Qualifier', 'Input Transformation', 'Output Transformation', 'Mapplet',
    }

    TYPE_ALIASES = {
        'Lookup': 'Lookup Procedure',
        'Sequence Generator': 'Sequence',
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
        self.corpus_coverage: Dict[str, int] = {}

        self.sources: List[Dict] = []
        self.targets: List[Dict] = []
        self.transformations: List[Dict] = []
        self.connectors: List[Dict] = []
        self.sessions: List[Dict] = []
        self.workflows: List[Dict] = []
        self.mappings: List[Dict] = []
        self.mapplets: List[Dict] = []

        self.instances: List[Dict] = []
        self.worklets: List[Dict] = []
        self.shortcuts: List[Dict] = []
        self.flatfiles: List[Dict] = []
        self.configs: List[Dict] = []
        self.config_references: List[Dict] = []
        self.partitions: List[Dict] = []
        self.mapping_variables: List[Dict] = []
        self.workflow_variables: List[Dict] = []
        self.associated_source_instances: List[Dict] = []
        self.target_load_orders: List[Dict] = []
        self.session_components: List[Dict] = []
        self.groups: List[Dict] = []
        self.sess_transform_insts: List[Dict] = []
        self.schedulers: List[Dict] = []
        self.erp_infos: List[Dict] = []
        self.metadata_extensions: List[Dict] = []
        self.field_dependencies: List[Dict] = []

        self.transform_types_found: Set[str] = set()
        self.coverage_status: Dict[str, str] = {}
        self.dependency_graph: Dict[str, List[str]] = defaultdict(list)
        self.instance_map: Dict[str, Dict] = {}
        self.parse_warnings: List[str] = []

    def _normalize_transform_type(self, t_type: str) -> str:
        return self.TYPE_ALIASES.get(t_type, t_type)

    def parse(self) -> Dict[str, Any]:
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
            'summary': self._generate_summary(),
            'parse_warnings': self.parse_warnings,
        }

    def _sanitize_xml(self, xml_path: str) -> str:
        with open(xml_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#)', '&amp;', content)

        def _fix_line_quotes(line):
            attr_pattern = re.compile(
                r'(\w+)\s*=\s*"((?:[^"\\]|\\.)*)(?="(?:\s+\w+="|[>\s/]))'
            )
            result = []
            last_end = 0
            for m in re.finditer(r'(\w+)\s*=\s*"', line):
                attr_name = m.group(1)
                val_start = m.end()
                next_attr = re.search(r'"\s+\w+\s*=\s*"', line[val_start:])
                tag_end = re.search(r'"\s*/?>', line[val_start:])

                if next_attr and (not tag_end or next_attr.start() < tag_end.start()):
                    val_end = val_start + next_attr.start()
                elif tag_end:
                    val_end = val_start + tag_end.start()
                else:
                    continue

                val = line[val_start:val_end]
                if '"' in val:
                    safe_val = val.replace('"', '&quot;')
                    result.append(line[last_end:val_start])
                    result.append(safe_val)
                    last_end = val_end

            if not result:
                return line
            result.append(line[last_end:])
            return ''.join(result)

        try:
            ET.fromstring(content)
            return content
        except ET.ParseError:
            lines = content.split('\n')
            fixed_lines = []
            for line in lines:
                if '="' in line and line.count('"') > line.count('="') * 2 + 2:
                    fixed_lines.append(_fix_line_quotes(line))
                else:
                    fixed_lines.append(line)
            return '\n'.join(fixed_lines)

    def _validate_xml(self):
        try:
            self.tree = ET.parse(self.xml_path)
            self.root = self.tree.getroot()
        except ET.ParseError:
            self.parse_warnings.append(f"Malformed XML, attempting sanitized re-parse: {self.xml_path}")
            try:
                sanitized = self._sanitize_xml(self.xml_path)
                self.root = ET.fromstring(sanitized)
                self.tree = ET.ElementTree(self.root)
            except ET.ParseError as e2:
                raise ValueError(f"Malformed XML even after sanitization: {e2}")

        if self.root.tag != 'POWERMART':
            raise ValueError(f"Expected POWERMART root element, got {self.root.tag}")

        repos = list(self.root.iter('REPOSITORY'))
        if not repos:
            raise ValueError("No REPOSITORY element found")

        folders = list(self.root.iter('FOLDER'))
        if not folders:
            raise ValueError("No FOLDER element found")

        print(f"  Valid PowerCenter XML: {len(repos)} repository(s), {len(folders)} folder(s)")

    def _load_corpus_coverage(self):
        if not self.corpus_csv or not os.path.exists(self.corpus_csv):
            return

        with open(self.corpus_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                t_type = row.get('transform_type', row.get('TRANSFORMATION_TYPE', ''))
                count = int(row.get('example_count', row.get('EXAMPLE_COUNT', 0)))
                self.corpus_coverage[t_type] = count

        print(f"  Loaded corpus coverage: {len(self.corpus_coverage)} transformation types")

    def _extract_all_nodes(self):
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
                'fields': [f.attrib for f in target.findall('.//TARGETFIELD')],
                'indexes': [{'name': idx.get('NAME'), 'fields': [idf.attrib for idf in idx.findall('.//TARGETINDEXFIELD')]} for idx in target.findall('.//TARGETINDEX')]
            })

        for transform in self.root.iter('TRANSFORMATION'):
            raw_type = transform.get('TYPE')
            t_type = self._normalize_transform_type(raw_type)
            self.transform_types_found.add(t_type)
            self.transformations.append({
                'name': transform.get('NAME'),
                'type': t_type,
                'raw_type': raw_type,
                'description': transform.get('DESCRIPTION', ''),
                'fields': [f.attrib for f in transform.findall('.//TRANSFORMFIELD')],
                'attributes': {a.get('NAME'): a.get('VALUE') for a in transform.findall('.//TABLEATTRIBUTE')},
                'ports': self._extract_ports(transform),
                'groups': [{'name': g.get('NAME'), 'type': g.get('TYPE', ''), 'expression': g.get('EXPRESSION', '')} for g in transform.findall('.//GROUP')],
                'field_attrs': [fa.attrib for fa in transform.findall('.//TRANSFORMFIELDATTR')],
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

        for instance in self.root.iter('INSTANCE'):
            inst = {
                'name': instance.get('NAME'),
                'transformation_name': instance.get('TRANSFORMATION_NAME'),
                'transformation_type': self._normalize_transform_type(instance.get('TRANSFORMATION_TYPE', '')),
                'type': instance.get('TYPE'),
                'reusable': instance.get('REUSABLE'),
                'description': instance.get('DESCRIPTION', ''),
            }
            self.instances.append(inst)
            self.instance_map[inst['name']] = inst

        for assoc in self.root.iter('ASSOCIATED_SOURCE_INSTANCE'):
            self.associated_source_instances.append({
                'name': assoc.get('NAME'),
                'source_instance': assoc.get('NAME'),
            })

        for tlo in self.root.iter('TARGETLOADORDER'):
            self.target_load_orders.append({
                'order': tlo.get('ORDER'),
                'targetinstance': tlo.get('TARGETINSTANCE'),
            })

        for session in self.root.iter('SESSION'):
            self.sessions.append({
                'name': session.get('NAME'),
                'mappingname': session.get('MAPPINGNAME'),
                'isvalid': session.get('ISVALID'),
                'connections': [c.attrib for c in session.findall('.//CONNECTIONREFERENCE')],
                'extensions': [e.attrib for e in session.findall('.//SESSIONEXTENSION')],
                'config_reference': session.findall('.//CONFIGREFERENCE')[0].attrib if session.findall('.//CONFIGREFERENCE') else None,
                'components': [{'name': sc.get('NAME'), 'type': sc.get('TYPE'), 'attrs': {a.get('NAME'): a.get('VALUE') for a in sc.findall('.//ATTRIBUTE')}} for sc in session.findall('.//SESSIONCOMPONENT')],
            })

        for workflow in self.root.iter('WORKFLOW'):
            tasks = []
            for task in workflow.findall('.//TASK'):
                task_data = {
                    'name': task.get('NAME'),
                    'type': task.get('TYPE'),
                    'reusable': task.get('REUSABLE'),
                    'description': task.get('DESCRIPTION', ''),
                }
                if task.get('TYPE') == 'Command':
                    task_data['commands'] = []
                    for attr in task.findall('.//ATTRIBUTE'):
                        aname = attr.get('NAME', '')
                        if aname.startswith('CmdLine'):
                            task_data['commands'].append(attr.get('VALUE', ''))
                tasks.append(task_data)

            links = []
            for link in workflow.findall('.//WORKFLOWLINK'):
                links.append({
                    'fromtask': link.get('FROMTASK'),
                    'totask': link.get('TOTASK'),
                    'condition': link.get('CONDITION')
                })

            wf_vars = []
            for wv in workflow.findall('.//WORKFLOWVARIABLE'):
                wf_vars.append({
                    'name': wv.get('NAME'),
                    'datatype': wv.get('DATATYPE'),
                    'defaultvalue': wv.get('DEFAULTVALUE', ''),
                    'isnull': wv.get('ISNULL', ''),
                    'ispersistent': wv.get('ISPERSISTENT', ''),
                    'userdefined': wv.get('USERDEFINED', ''),
                })

            self.workflows.append({
                'name': workflow.get('NAME'),
                'isvalid': workflow.get('ISVALID'),
                'tasks': tasks,
                'links': links,
                'variables': wf_vars,
            })

        for mapping in self.root.iter('MAPPING'):
            mv = []
            for v in mapping.findall('.//MAPPINGVARIABLE'):
                mv.append({
                    'name': v.get('NAME'),
                    'datatype': v.get('DATATYPE'),
                    'defaultvalue': v.get('DEFAULTVALUE', ''),
                    'description': v.get('DESCRIPTION', ''),
                    'isexprvar': v.get('ISEXPRVAR', ''),
                    'isparam': v.get('ISPARAM', ''),
                    'userdefined': v.get('USERDEFINED', ''),
                })
            self.mapping_variables.extend(mv)

            self.mappings.append({
                'name': mapping.get('NAME'),
                'isvalid': mapping.get('ISVALID'),
                'description': mapping.get('DESCRIPTION', ''),
                'variables': mv,
            })

        for mapplet in self.root.iter('MAPPLET'):
            mplt_transforms = []
            for t in mapplet.findall('.//TRANSFORMATION'):
                raw_type = t.get('TYPE')
                t_type = self._normalize_transform_type(raw_type)
                mplt_transforms.append({
                    'name': t.get('NAME'),
                    'type': t_type,
                    'fields': [f.attrib for f in t.findall('.//TRANSFORMFIELD')],
                    'attributes': {a.get('NAME'): a.get('VALUE') for a in t.findall('.//TABLEATTRIBUTE')},
                })
            mplt_connectors = [c.attrib for c in mapplet.findall('.//CONNECTOR')]
            self.mapplets.append({
                'name': mapplet.get('NAME'),
                'isvalid': mapplet.get('ISVALID'),
                'description': mapplet.get('DESCRIPTION', ''),
                'transformations': mplt_transforms,
                'connectors': mplt_connectors,
                'instances': [{'name': i.get('NAME'), 'transformation_name': i.get('TRANSFORMATION_NAME'), 'transformation_type': i.get('TRANSFORMATION_TYPE', '')} for i in mapplet.findall('.//INSTANCE')],
            })

        for worklet in self.root.iter('WORKLET'):
            wklt_tasks = []
            for t in worklet.findall('.//TASK'):
                wklt_tasks.append({
                    'name': t.get('NAME'),
                    'type': t.get('TYPE'),
                })
            wklt_links = []
            for link in worklet.findall('.//WORKFLOWLINK'):
                wklt_links.append({
                    'fromtask': link.get('FROMTASK'),
                    'totask': link.get('TOTASK'),
                    'condition': link.get('CONDITION'),
                })
            wklt_sessions = []
            for s in worklet.findall('.//TASKINSTANCE'):
                wklt_sessions.append({
                    'name': s.get('TASKNAME', s.get('NAME')),
                    'type': s.get('TASKTYPE', ''),
                })
            self.worklets.append({
                'name': worklet.get('NAME'),
                'isvalid': worklet.get('ISVALID'),
                'description': worklet.get('DESCRIPTION', ''),
                'tasks': wklt_tasks,
                'links': wklt_links,
                'task_instances': wklt_sessions,
            })

        for shortcut in self.root.iter('SHORTCUT'):
            self.shortcuts.append({
                'name': shortcut.get('NAME'),
                'objecttype': shortcut.get('OBJECTTYPE'),
                'refobjectname': shortcut.get('REFOBJECTNAME', shortcut.get('NAME')),
                'foldername': shortcut.get('FOLDERNAME', ''),
                'repositoryname': shortcut.get('REPOSITORYNAME', ''),
                'reusable': shortcut.get('REUSABLE', ''),
                'versionnumber': shortcut.get('VERSIONNUMBER', ''),
            })

        for ff in self.root.iter('FLATFILE'):
            self.flatfiles.append({
                'name': ff.get('NAME', ''),
                'codepage': ff.get('CODEPAGE', ''),
                'is_delimited': ff.get('DELIMITED', ''),
                'delimiters': ff.get('DELIMITERS', ''),
                'null_char': ff.get('NULL_CHAR', ''),
                'escape_char': ff.get('ESCAPE_CHAR', ''),
                'skip_rows': ff.get('SKIP_ROWS', '0'),
            })

        for config in self.root.iter('CONFIG'):
            cfg_attrs = {}
            for a in config.findall('.//ATTRIBUTE'):
                cfg_attrs[a.get('NAME', '')] = a.get('VALUE', '')
            self.configs.append({
                'name': config.get('NAME'),
                'isvalid': config.get('ISVALID', ''),
                'description': config.get('DESCRIPTION', ''),
                'attributes': cfg_attrs,
            })

        for partition in self.root.iter('PARTITION'):
            self.partitions.append({
                'name': partition.get('NAME', ''),
                'description': partition.get('DESCRIPTION', ''),
            })

        for sc in self.root.iter('SESSIONCOMPONENT'):
            self.session_components.append({
                'name': sc.get('NAME'),
                'type': sc.get('TYPE'),
                'attributes': {a.get('NAME'): a.get('VALUE') for a in sc.findall('.//ATTRIBUTE')},
            })

        for grp in self.root.iter('GROUP'):
            self.groups.append({
                'name': grp.get('NAME'),
                'type': grp.get('TYPE', ''),
                'expression': grp.get('EXPRESSION', ''),
                'order': grp.get('ORDER', ''),
            })

        for sti in self.root.iter('SESSTRANSFORMATIONINST'):
            self.sess_transform_insts.append({
                'pipeline': sti.get('PIPELINE', ''),
                'sinstancename': sti.get('SINSTANCENAME', ''),
                'stage': sti.get('STAGE', ''),
                'transformationname': sti.get('TRANSFORMATIONNAME', ''),
                'transformationtype': sti.get('TRANSFORMATIONTYPE', ''),
                'attributes': {a.get('NAME'): a.get('VALUE') for a in sti.findall('.//ATTRIBUTE')},
            })

        for sched in self.root.iter('SCHEDULER'):
            sinfo = sched.find('.//SCHEDULEINFO')
            schedule_data = sinfo.attrib if sinfo is not None else {}
            self.schedulers.append({
                'name': sched.get('NAME'),
                'description': sched.get('DESCRIPTION', ''),
                'schedule_info': schedule_data,
            })

        for erp in self.root.iter('ERPINFO'):
            self.erp_infos.append({
                'name': erp.get('NAME', ''),
                'connectionname': erp.get('CONNECTIONNAME', ''),
                'sourcename': erp.get('SOURCENAME', ''),
            })

        for mext in self.root.iter('METADATAEXTENSION'):
            self.metadata_extensions.append({
                'name': mext.get('NAME', ''),
                'datatype': mext.get('DATATYPE', ''),
                'value': mext.get('VALUE', ''),
                'domain': mext.get('DOMAINNAME', ''),
            })

        for fd in self.root.iter('FIELDDEPENDENCY'):
            self.field_dependencies.append({
                'infield': fd.get('INFIELD', ''),
                'outfield': fd.get('OUTFIELD', ''),
            })

        print(f"  Extracted: {len(self.sources)} sources, {len(self.targets)} targets, "
              f"{len(self.transformations)} transformations, {len(self.connectors)} connectors, "
              f"{len(self.instances)} instances, {len(self.worklets)} worklets, "
              f"{len(self.shortcuts)} shortcuts, {len(self.flatfiles)} flatfiles")

    def _extract_ports(self, transform) -> List[Dict]:
        ports = []
        for field in transform.findall('.//TRANSFORMFIELD'):
            ports.append({
                'name': field.get('NAME'),
                'datatype': field.get('DATATYPE'),
                'porttype': field.get('PORTTYPE'),
                'expression': field.get('EXPRESSION', ''),
                'defaultvalue': field.get('DEFAULTVALUE', ''),
                'precision': field.get('PRECISION', ''),
                'scale': field.get('SCALE', ''),
            })
        return ports

    def _check_corpus_coverage(self):
        for t_type in self.transform_types_found:
            canonical = self._normalize_transform_type(t_type)
            if canonical in self.ESCALATE_TRANSFORMS:
                self.coverage_status[canonical] = 'escalate'
            elif not self.corpus_coverage:
                if canonical in self.STANDARD_TRANSFORMS:
                    self.coverage_status[canonical] = 'ok'
                else:
                    self.coverage_status[canonical] = 'missing'
            elif canonical not in self.corpus_coverage:
                self.coverage_status[canonical] = 'missing'
            elif self.corpus_coverage[canonical] < 3:
                self.coverage_status[canonical] = 'thin'
            else:
                self.coverage_status[canonical] = 'ok'

        gaps = [t for t, s in self.coverage_status.items() if s in ('missing', 'thin', 'escalate')]
        if gaps:
            print(f"  Corpus coverage gaps: {gaps}")

    def _resolve_instance_to_transform(self, instance_name: str) -> Optional[Dict]:
        inst = self.instance_map.get(instance_name)
        if inst:
            tname = inst.get('transformation_name', '')
            match = next((t for t in self.transformations if t['name'] == tname), None)
            if match:
                return match
        return next((t for t in self.transformations if t['name'] == instance_name), None)

    def _build_dependency_graph(self):
        for workflow in self.workflows:
            for link in workflow.get('links', []):
                from_task = link['fromtask']
                to_task = link['totask']
                self.dependency_graph[to_task].append(from_task)

    def _enumerate_targets(self) -> List[Dict]:
        targets_data = []

        for target in self.targets:
            chain = self._trace_chain(target['name'])
            sources = self._find_sources_for_target(target['name'], chain)
            field_lineage = self._build_field_lineage(target)

            assoc_sources = [a for a in self.associated_source_instances if a.get('source_instance')]
            tlo = next((t for t in self.target_load_orders if t.get('targetinstance') == target['name']), None)

            targets_data.append({
                'target_instance_name': target['name'],
                'target_table_name': target.get('tablename', target['name']),
                'target_schema_name': target.get('dbdname') or target.get('ownername') or 'PUBLIC',
                'database_type': target.get('databasetype', 'Snowflake'),
                'fields': target['fields'],
                'indexes': target.get('indexes', []),
                'transformation_chain': chain,
                'source_tables': sources,
                'field_lineage': field_lineage,
                'transform_types_in_chain': list(set(t['type'] for t in chain)),
                'target_load_order': tlo.get('order') if tlo else None,
                'is_flatfile_target': target.get('databasetype') == 'Flat File',
            })

        return targets_data

    def _trace_chain(self, target_name: str) -> List[Dict]:
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

                transform = self._resolve_instance_to_transform(from_instance)
                if transform and transform not in chain:
                    chain.append(transform)

        chain.reverse()
        return chain

    def _find_sources_for_target(self, target_name: str, chain: List[Dict] = None) -> List[str]:
        sources = []
        if chain is None:
            chain = self._trace_chain(target_name)

        for transform in chain:
            if transform['type'] in ('Source Qualifier', 'XML Source Qualifier'):
                sql_override = transform['attributes'].get('Sql Query', '')
                if sql_override:
                    tables = re.findall(r'FROM\s+([^\s,()]+)', sql_override, re.IGNORECASE)
                    join_tables = re.findall(r'JOIN\s+([^\s,()]+)', sql_override, re.IGNORECASE)
                    sources.extend(tables)
                    sources.extend(join_tables)
                else:
                    for conn in self.connectors:
                        if conn['toinstance'] == transform['name']:
                            source = next((s for s in self.sources if s['name'] == conn['frominstance']), None)
                            if source:
                                schema = source.get('ownername') or source.get('dbdname') or ''
                                table = source['name']
                                sources.append(f"{schema}.{table}" if schema else table)

            elif transform['type'] == 'Lookup Procedure':
                lkp_sql = transform['attributes'].get('Lookup Sql Override', '')
                if lkp_sql:
                    tables = re.findall(r'FROM\s+([^\s,()]+)', lkp_sql, re.IGNORECASE)
                    sources.extend(tables)
                else:
                    lkp_table = transform['attributes'].get('Lookup table name', '')
                    if lkp_table:
                        sources.append(lkp_table)

        return list(set(sources))

    def _build_field_lineage(self, target: Dict) -> List[Dict]:
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
        if not name:
            return 'unknown'
        name_upper = name.upper()
        for pattern, freq in self.FREQ_PATTERNS.items():
            if re.search(pattern, name_upper):
                return freq
        return 'unknown'

    def _infer_freq_from_scheduler(self) -> Optional[str]:
        if not self.schedulers:
            return None
        for sched in self.schedulers:
            info = sched.get('schedule_info', {})
            stype = info.get('SCHEDULETYPE', '')
            if 'DAILY' in stype.upper():
                return 'daily'
            if 'WEEKLY' in stype.upper():
                return 'weekly'
            if 'MONTHLY' in stype.upper():
                return 'monthly'
            repeat_interval = info.get('REPEATINTERVAL', '')
            if repeat_interval:
                try:
                    interval_sec = int(repeat_interval)
                    if interval_sec <= 3600:
                        return 'hourly'
                except ValueError:
                    pass
        return None

    def _propose_model_name(self, target_table: str, workflow_name: str = '') -> str:
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', target_table.lower())
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')

        if any(x in clean_name for x in ('stg', 'staging', 'raw')):
            prefix = 'stg_'
        elif any(x in clean_name for x in ('int', 'intermediate', 'tmp', 'work')):
            prefix = 'int_'
        elif any(x in clean_name for x in ('dim', 'fact', 'mart', 'agg')):
            prefix = 'mart_'
        else:
            prefix = 'mart_'

        if not clean_name.startswith(prefix.rstrip('_')):
            return f"{prefix}{clean_name}"
        return clean_name

    def _compute_execution_order(self) -> List[str]:
        in_degree = defaultdict(int)
        all_nodes = set()

        for workflow in self.workflows:
            all_nodes.add(workflow['name'])
            for link in workflow.get('links', []):
                all_nodes.add(link['fromtask'])
                all_nodes.add(link['totask'])
                in_degree[link['totask']] += 1

        for worklet in self.worklets:
            for link in worklet.get('links', []):
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

            for worklet in self.worklets:
                for link in worklet.get('links', []):
                    if link['fromtask'] == node:
                        in_degree[link['totask']] -= 1
                        if in_degree[link['totask']] == 0:
                            queue.append(link['totask'])

        if len(order) < len(all_nodes):
            self.parse_warnings.append("Circular dependency detected in workflow/worklet topology")

        return order

    def _extract_command_hooks(self) -> Tuple[List[str], List[str]]:
        pre_hooks = []
        post_hooks = []
        if not self.workflows:
            return pre_hooks, post_hooks

        wf = self.workflows[0]
        session_tasks = {t['name'] for t in wf.get('tasks', []) if t['type'] != 'Command'}

        for task in wf.get('tasks', []):
            if task.get('type') != 'Command':
                continue
            cmds = task.get('commands', [])
            if not cmds:
                continue

            is_pre = True
            for link in wf.get('links', []):
                if link['fromtask'] == task['name'] and link['totask'] not in session_tasks:
                    pass
                if link['totask'] == task['name'] and link['fromtask'] in session_tasks:
                    is_pre = False
                    break

            for cmd in cmds:
                if is_pre:
                    pre_hooks.append(cmd)
                else:
                    post_hooks.append(cmd)

        return pre_hooks, post_hooks

    def _generate_handoffs(self, targets_data: List[Dict]) -> List[Dict]:
        handoffs = []
        workflow_name = self.workflows[0]['name'] if self.workflows else 'unknown'
        pre_hooks, post_hooks = self._extract_command_hooks()

        sched_freq = self._infer_freq_from_scheduler()

        var_map = {}
        for mv in self.mapping_variables:
            var_map[mv['name']] = {
                'datatype': mv.get('datatype', ''),
                'default': mv.get('defaultvalue', ''),
                'is_param': mv.get('isparam', '') == 'YES',
            }
        for wf in self.workflows:
            for wv in wf.get('variables', []):
                if wv.get('userdefined', '') == 'YES':
                    var_map[wv['name']] = {
                        'datatype': wv.get('datatype', ''),
                        'default': wv.get('defaultvalue', ''),
                        'is_persistent': wv.get('ispersistent', '') == 'YES',
                    }

        for target in targets_data:
            model_name = self._propose_model_name(target['target_table_name'], workflow_name)
            freq_hint = sched_freq or self._infer_freq_from_name(workflow_name)

            chain_coverage = 'ok'
            for t_type in target['transform_types_in_chain']:
                canonical = self._normalize_transform_type(t_type)
                status = self.coverage_status.get(canonical, 'missing')
                if status in ('missing', 'escalate'):
                    chain_coverage = 'missing'
                    break
                elif status == 'thin' and chain_coverage == 'ok':
                    chain_coverage = 'thin'

            session_overrides = {}
            for sti in self.sess_transform_insts:
                if sti.get('transformationname') in [t['name'] for t in target['transformation_chain']]:
                    session_overrides[sti['transformationname']] = sti.get('attributes', {})

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
                'database_type': target.get('database_type', 'Snowflake'),
                'is_flatfile_target': target.get('is_flatfile_target', False),
                'target_load_order': target.get('target_load_order'),
                'dbt_config': {
                    'materialization': self._infer_materialization(target, freq_hint),
                    'schema': self._infer_dbt_schema(target),
                    'tags': self._infer_tags(workflow_name, freq_hint),
                    'pre_hook': pre_hooks,
                    'post_hook': post_hooks,
                },
                'variables': {k: v for k, v in var_map.items()},
                'session_overrides': session_overrides,
                'shortcuts_used': [s for s in self.shortcuts],
                'worklets': [{'name': w['name'], 'task_instances': w.get('task_instances', [])} for w in self.worklets],
                'mapplets': [{'name': m['name'], 'transforms': m.get('transformations', [])} for m in self.mapplets],
                'partition_count': len([p for p in self.partitions]),
                'ref_dependencies': [],
                'pipeline_config': {
                    'fidelity_threshold': 0.70 if chain_coverage == 'thin' else 0.85,
                    'max_retries': 3,
                    'quarantine_on_missing': chain_coverage == 'missing'
                },
                'agent1_timestamp': datetime.now(timezone.utc).isoformat(),
                'agent1_version': '3.0.0'
            }
            handoffs.append(handoff)

        return handoffs

    def _infer_materialization(self, target: Dict, freq_hint: str) -> str:
        transform_types = set(target.get('transform_types_in_chain', []))

        if 'Update Strategy' in transform_types:
            return 'incremental'
        if freq_hint == 'incr':
            return 'incremental'
        if freq_hint in ('daily', 'weekly', 'monthly'):
            return 'table'
        if any(x in target['target_table_name'].lower() for x in ('dim', 'fact')):
            return 'table'
        if target.get('is_flatfile_target'):
            return 'table'
        return 'view'

    def _infer_dbt_schema(self, target: Dict) -> str:
        schema = target.get('target_schema_name', 'PUBLIC')
        if schema.upper() in ('EDW', 'DW', 'WAREHOUSE'):
            return 'ANALYTICS'
        if 'STG' in schema.upper() or 'STAGING' in schema.upper():
            return 'STAGING'
        if 'WORK' in schema.upper() or 'WRK' in schema.upper():
            return 'INTERMEDIATE'
        return schema

    def _infer_tags(self, workflow_name: str, freq_hint: str) -> List[str]:
        tags = []

        if freq_hint != 'unknown':
            tags.append(freq_hint)

        name_lower = workflow_name.lower()
        domain_patterns = [
            (['customer', 'cust'], 'customer'),
            (['product', 'prod', 'item'], 'product'),
            (['sales', 'order', 'booking'], 'sales'),
            (['finance', 'fin', 'gl_', 'ap_', 'ar_'], 'finance'),
            (['hr_', 'human', 'employee'], 'hr'),
            (['inventory', 'inv_', 'mtl_'], 'inventory'),
        ]
        for patterns, domain in domain_patterns:
            if any(p in name_lower for p in patterns):
                tags.append(domain)
                break

        if self.erp_infos:
            tags.append('erp_sourced')

        tags.append('infa2dbt')
        return tags

    def _generate_summary(self) -> Dict:
        return {
            'workflows_processed': len(self.workflows),
            'mappings_found': len(self.mappings),
            'targets_extracted': len(self.targets),
            'sources_found': len(self.sources),
            'transformations_found': len(self.transformations),
            'instances_found': len(self.instances),
            'worklets_found': len(self.worklets),
            'shortcuts_found': len(self.shortcuts),
            'mapplets_found': len(self.mapplets),
            'flatfiles_found': len(self.flatfiles),
            'mapping_variables_found': len(self.mapping_variables),
            'configs_found': len(self.configs),
            'partitions_found': len(self.partitions),
            'session_components_found': len(self.session_components),
            'schedulers_found': len(self.schedulers),
            'transform_types': list(self.transform_types_found),
            'coverage_gaps': [t for t, s in self.coverage_status.items() if s != 'ok'],
            'models_ready': len([t for t in self.targets if self.coverage_status.get(t.get('name'), 'ok') != 'escalate']),
            'database_types': list(set(s.get('databasetype', '') for s in self.sources + self.targets)),
            'parse_warnings': self.parse_warnings,
        }

    def write_outputs(self, result: Dict):
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
                'database_type': handoff.get('database_type', ''),
                'is_flatfile': handoff.get('is_flatfile_target', False),
                'variable_count': len(handoff.get('variables', {})),
                'partition_count': handoff.get('partition_count', 0),
                'agent1_timestamp': handoff['agent1_timestamp'],
                'agent1_status': 'complete' if handoff['corpus_coverage_status'] != 'missing' else 'escalated'
            })

        mapping_path = os.path.join(self.output_dir, 'workflow_model_mapping.csv')
        if mapping_rows:
            with open(mapping_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=mapping_rows[0].keys())
                writer.writeheader()
                writer.writerows(mapping_rows)
            print(f"  Wrote {mapping_path}")

        for handoff in result['handoffs']:
            handoff_path = os.path.join(self.output_dir, 'handoffs', f"{handoff['proposed_model_name']}_handoff.json")
            with open(handoff_path, 'w') as f:
                json.dump(handoff, f, indent=2, default=str)
            print(f"  Wrote {handoff_path}")

        summary_path = os.path.join(self.output_dir, '_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(result['summary'], f, indent=2, default=str)

        run_log = {
            'xml_path': self.xml_path,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'parser_version': '3.0.0',
            'summary': result['summary'],
            'execution_order': result['execution_order'],
            'coverage_status': self.coverage_status,
            'parse_warnings': self.parse_warnings,
        }
        log_path = os.path.join(self.output_dir, 'logs', 'agent1_run.json')
        with open(log_path, 'w') as f:
            json.dump(run_log, f, indent=2, default=str)
        print(f"  Wrote {log_path}")


def parse_single(xml_path: str, output_dir: str, corpus_csv: str = None) -> Optional[Dict]:
    try:
        pc_parser = PowerCenterParser(xml_path, output_dir, corpus_csv)
        result = pc_parser.parse()
        pc_parser.write_outputs(result)
        return result
    except ValueError as e:
        print(f"  SKIP {xml_path}: {e}")
        return None
    except Exception as e:
        print(f"  ERROR {xml_path}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Parse Informatica PowerCenter XML — v3.0')
    parser.add_argument('xml_path', nargs='?', help='Path to PowerCenter XML file')
    parser.add_argument('--batch', help='Directory to batch-process all XML files')
    parser.add_argument('--output-dir', default='artifacts/', help='Output directory')
    parser.add_argument('--corpus-csv', help='Path to corpus coverage CSV')
    args = parser.parse_args()

    if args.batch:
        xml_files = []
        for root, dirs, files in os.walk(args.batch):
            dirs[:] = [d for d in dirs if d != 'artifacts']
            for f in files:
                if f.lower().endswith('.xml'):
                    xml_files.append(os.path.join(root, f))

        print(f"Batch mode: {len(xml_files)} XML files found")
        success = 0
        fail = 0
        all_transform_types = set()
        all_database_types = set()

        for i, xf in enumerate(xml_files):
            if i % 100 == 0:
                print(f"Processing {i}/{len(xml_files)}...")

            sub_dir = os.path.join(args.output_dir, os.path.splitext(os.path.basename(xf))[0])
            result = parse_single(xf, sub_dir, args.corpus_csv)
            if result:
                success += 1
                all_transform_types.update(result.get('transform_types', []))
                for s in result.get('summary', {}).get('database_types', []):
                    if s:
                        all_database_types.add(s)
            else:
                fail += 1

        print(f"\n{'='*60}")
        print(f"BATCH SUMMARY")
        print(f"{'='*60}")
        print(f"Total files: {len(xml_files)}")
        print(f"Succeeded: {success}")
        print(f"Failed: {fail}")
        print(f"All transform types: {sorted(all_transform_types)}")
        print(f"All database types: {sorted(all_database_types)}")
        return

    if not args.xml_path:
        parser.error("Either xml_path or --batch is required")

    result = parse_single(args.xml_path, args.output_dir, args.corpus_csv)
    if not result:
        return

    print(f"\n{'='*60}")
    print("PARSING SUMMARY")
    print(f"{'='*60}")
    summary = result['summary']
    print(f"Workflows processed: {summary['workflows_processed']}")
    print(f"Target tables: {summary['targets_extracted']}")
    print(f"Sources: {summary['sources_found']}")
    print(f"Transformations: {summary['transformations_found']}")
    print(f"Instances: {summary.get('instances_found', 0)}")
    print(f"Worklets: {summary.get('worklets_found', 0)}")
    print(f"Shortcuts: {summary.get('shortcuts_found', 0)}")
    print(f"Mapplets: {summary.get('mapplets_found', 0)}")
    print(f"Flat files: {summary.get('flatfiles_found', 0)}")
    print(f"Mapping variables: {summary.get('mapping_variables_found', 0)}")
    print(f"Configs: {summary.get('configs_found', 0)}")
    print(f"Partitions: {summary.get('partitions_found', 0)}")
    print(f"Transform types: {summary['transform_types']}")
    print(f"Database types: {summary.get('database_types', [])}")
    print(f"Coverage gaps: {summary['coverage_gaps']}")
    print(f"Models ready for Agent 2: {summary['models_ready']}")
    if summary.get('parse_warnings'):
        print(f"Warnings: {summary['parse_warnings']}")

    return result


if __name__ == '__main__':
    main()
