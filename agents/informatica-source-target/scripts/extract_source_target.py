#!/usr/bin/env python3
"""
Extract source and target table names from Informatica PowerCenter XML exports
into a flat CSV. Reuses PowerCenterParser from the infa-xml-parser agent.

Usage:
    python extract_source_target.py --xml-file /path/to/workflow.xml --output source_target.csv
    python extract_source_target.py --xml-dir /path/to/xmls/ --output source_target.csv
"""

import argparse
import csv
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARSER_DIR = os.path.join(SCRIPT_DIR, '..', '..', 'infa-xml-parser', 'scripts')
sys.path.insert(0, os.path.abspath(PARSER_DIR))

from parse_pc_xml import PowerCenterParser


def extract_from_xml(xml_path: str) -> list[dict]:
    rows = []
    try:
        parser = PowerCenterParser(xml_path, output_dir='/dev/null')
        result = parser.parse()
    except (ValueError, Exception) as e:
        print(f"  SKIP {xml_path}: {e}", file=sys.stderr)
        return rows

    for handoff in result.get('handoffs', []):
        wf_name = handoff.get('workflow_name', 'unknown')

        for src in handoff.get('source_tables', []):
            if src:
                rows.append({'workflow_name': wf_name, 'type': 'SOURCE', 'table_name': src})

        tgt = handoff.get('target_table', '')
        if tgt:
            rows.append({'workflow_name': wf_name, 'type': 'TARGET', 'table_name': tgt})

    return rows


def collect_xml_files(directory: str) -> list[str]:
    xml_files = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if d != 'artifacts']
        for f in files:
            if f.lower().endswith('.xml'):
                xml_files.append(os.path.join(root, f))
    return sorted(xml_files)


def main():
    ap = argparse.ArgumentParser(description='Extract source/target tables from PowerCenter XML')
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument('--xml-file', help='Single XML file to process')
    group.add_argument('--xml-dir', help='Directory of XML files to process')
    ap.add_argument('--output', required=True, help='Output CSV path')
    args = ap.parse_args()

    if args.xml_file:
        xml_files = [args.xml_file]
    else:
        xml_files = collect_xml_files(args.xml_dir)

    print(f"Processing {len(xml_files)} XML file(s)...")

    all_rows = []
    for xf in xml_files:
        print(f"  Parsing: {os.path.basename(xf)}")
        all_rows.extend(extract_from_xml(xf))

    seen = set()
    deduped = []
    for row in all_rows:
        key = (row['workflow_name'], row['type'], row['table_name'])
        if key not in seen:
            seen.add(key)
            deduped.append(row)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True) if os.path.dirname(args.output) else None

    with open(args.output, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['workflow_name', 'type', 'table_name'])
        writer.writeheader()
        writer.writerows(deduped)

    src_count = sum(1 for r in deduped if r['type'] == 'SOURCE')
    tgt_count = sum(1 for r in deduped if r['type'] == 'TARGET')
    wf_count = len(set(r['workflow_name'] for r in deduped))

    print(f"\nDone. Wrote {len(deduped)} rows to {args.output}")
    print(f"  Workflows: {wf_count}")
    print(f"  Sources:   {src_count}")
    print(f"  Targets:   {tgt_count}")


if __name__ == '__main__':
    main()
