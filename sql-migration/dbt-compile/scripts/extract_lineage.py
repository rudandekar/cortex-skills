#!/usr/bin/env python3
"""Extract dependency lineage from compiled dbt SQL files.

Parses compiled SQL (post-Jinja resolution) to build a dependency graph
of sources and models. Outputs JSON adjacency list and optional Mermaid diagram.

Usage:
    python extract_lineage.py --compiled-dir target/compiled/project/models/ \
                              --output lineage_graph.json \
                              --mermaid lineage_diagram.md
"""

import argparse
import json
import re
from pathlib import Path


# Patterns for resolved references in compiled SQL
# After dbt compile, ref() becomes fully qualified table names
# and source() becomes fully qualified table names
FULLY_QUALIFIED_TABLE = re.compile(
    r"""(?:FROM|JOIN|MERGE\s+INTO|INSERT\s+INTO)\s+
    (?:
        "?(\w+)"?\."?(\w+)"?\."?(\w+)"?     # DATABASE.SCHEMA.TABLE
        |"?(\w+)"?\."?(\w+)"?                 # SCHEMA.TABLE
    )""",
    re.IGNORECASE | re.VERBOSE,
)

# CTE names to exclude from external dependencies
CTE_PATTERN = re.compile(
    r"(?:WITH\s+|,\s*)(\w+)\s+AS\s*\(",
    re.IGNORECASE,
)

# dbt manifest-style patterns (if manifest.json is available)
MANIFEST_REF = re.compile(r'"depends_on":\s*\{[^}]*"nodes":\s*\[([^\]]*)\]')


def extract_cte_names(sql: str) -> set[str]:
    """Extract all CTE names from a SQL query."""
    return {m.group(1).upper() for m in CTE_PATTERN.finditer(sql)}


def extract_table_references(sql: str) -> list[dict]:
    """Extract all table references from compiled SQL."""
    cte_names = extract_cte_names(sql)
    tables = []

    for match in FULLY_QUALIFIED_TABLE.finditer(sql):
        if match.group(1):  # 3-part name: DB.SCHEMA.TABLE
            db, schema, table = match.group(1), match.group(2), match.group(3)
            fqn = f"{db}.{schema}.{table}".upper()
            if table.upper() not in cte_names:
                tables.append({
                    "database": db.upper(),
                    "schema": schema.upper(),
                    "table": table.upper(),
                    "fqn": fqn,
                })
        elif match.group(4):  # 2-part name: SCHEMA.TABLE
            schema, table = match.group(4), match.group(5)
            fqn = f"{schema}.{table}".upper()
            if table.upper() not in cte_names:
                tables.append({
                    "schema": schema.upper(),
                    "table": table.upper(),
                    "fqn": fqn,
                })

    # Deduplicate by fqn
    seen = set()
    unique = []
    for t in tables:
        if t["fqn"] not in seen:
            seen.add(t["fqn"])
            unique.append(t)
    return unique


def build_lineage(compiled_dir: str) -> dict:
    """Build lineage graph from all compiled SQL files in a directory."""
    compiled_path = Path(compiled_dir)
    if not compiled_path.exists():
        return {"error": f"Directory not found: {compiled_dir}"}

    nodes = []
    edges = []
    model_names = set()
    source_names = set()

    sql_files = list(compiled_path.rglob("*.sql"))
    if not sql_files:
        return {"error": f"No .sql files found in {compiled_dir}"}

    for sql_file in sql_files:
        model_name = sql_file.stem
        model_id = f"model.{model_name}"
        model_names.add(model_id)

        sql = sql_file.read_text()
        refs = extract_table_references(sql)

        for ref in refs:
            source_id = f"source.{ref['fqn']}"
            source_names.add(source_id)
            edges.append({
                "from": source_id,
                "to": model_id,
                "source_table": ref["fqn"],
            })

    # Build node list
    for sid in sorted(source_names):
        nodes.append({"id": sid, "type": "source", "name": sid.split(".", 1)[1]})
    for mid in sorted(model_names):
        nodes.append({"id": mid, "type": "model", "name": mid.split(".", 1)[1]})

    return {
        "nodes": nodes,
        "edges": edges,
        "summary": {
            "source_count": len(source_names),
            "model_count": len(model_names),
            "edge_count": len(edges),
        },
    }


def generate_mermaid(lineage: dict) -> str:
    """Generate Mermaid diagram from lineage graph."""
    if "error" in lineage:
        return f"<!-- Error: {lineage['error']} -->"

    lines = ["graph LR"]

    # Create safe node IDs for Mermaid (no dots or special chars)
    def safe_id(node_id: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_]", "_", node_id)

    # Add nodes with styling
    for node in lineage["nodes"]:
        sid = safe_id(node["id"])
        name = node["name"]
        if node["type"] == "source":
            lines.append(f"    {sid}[(\"{name}\")]")
        else:
            lines.append(f"    {sid}[\"{name}\"]")

    # Add edges
    for edge in lineage["edges"]:
        from_id = safe_id(edge["from"])
        to_id = safe_id(edge["to"])
        lines.append(f"    {from_id} --> {to_id}")

    # Add styling
    source_ids = [safe_id(n["id"]) for n in lineage["nodes"] if n["type"] == "source"]
    model_ids = [safe_id(n["id"]) for n in lineage["nodes"] if n["type"] == "model"]

    if source_ids:
        lines.append(f"    classDef source fill:#e1f5fe,stroke:#0288d1")
        lines.append(f"    class {','.join(source_ids)} source")
    if model_ids:
        lines.append(f"    classDef model fill:#e8f5e9,stroke:#388e3c")
        lines.append(f"    class {','.join(model_ids)} model")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Extract lineage from compiled dbt SQL"
    )
    parser.add_argument(
        "--compiled-dir",
        required=True,
        help="Path to compiled SQL directory (target/compiled/project/models/)",
    )
    parser.add_argument(
        "--output", "-o",
        help="Path to write JSON lineage graph (default: stdout)",
    )
    parser.add_argument(
        "--mermaid", "-m",
        help="Path to write Mermaid diagram markdown",
    )
    args = parser.parse_args()

    lineage = build_lineage(args.compiled_dir)

    # Write JSON
    output_json = json.dumps(lineage, indent=2)
    if args.output:
        Path(args.output).write_text(output_json)
    else:
        print(output_json)

    # Write Mermaid diagram
    if args.mermaid:
        mermaid = generate_mermaid(lineage)
        diagram = f"```mermaid\n{mermaid}\n```\n"
        Path(args.mermaid).write_text(diagram)


if __name__ == "__main__":
    main()
