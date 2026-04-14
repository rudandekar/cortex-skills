#!/usr/bin/env python3
"""Diff two compiled dbt SQL files with CTE-level granularity.

Compares compiled SQL at the CTE level rather than raw line-level diff,
producing structured output showing which CTEs were added, removed, or modified.

Usage:
    python diff_compiled_sql.py --old target/compiled_prev/model.sql \
                                --new target/compiled/model.sql \
                                --output diff_result.json
"""

import argparse
import difflib
import json
import re
from pathlib import Path


CTE_SPLIT = re.compile(
    r"""(?:^WITH\s+|,\s*\n\s*)(\w+)\s+AS\s*\((.*?)\)(?=\s*,\s*\n\s*\w+\s+AS\s*\(|\s*\n\s*SELECT)""",
    re.IGNORECASE | re.DOTALL,
)

FINAL_SELECT = re.compile(
    r"""(?:^|\)\s*\n)\s*(SELECT\s+.*?)$""",
    re.IGNORECASE | re.DOTALL,
)


def extract_ctes(sql: str) -> dict[str, str]:
    """Extract CTEs from SQL into {name: body} dict."""
    ctes = {}
    # Normalize whitespace for consistent parsing
    normalized = sql.strip()

    # Use a simpler approach: split on CTE boundaries
    # Match: CTE_NAME AS ( ... )
    pattern = re.compile(
        r"(\w+)\s+AS\s*\(\s*(.*?)\s*\)(?=\s*,\s*\w+\s+AS\s*\(|\s*SELECT\b)",
        re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(normalized):
        cte_name = match.group(1).upper()
        cte_body = match.group(2).strip()
        ctes[cte_name] = cte_body

    return ctes


def extract_final_select(sql: str) -> str | None:
    """Extract the final SELECT statement after all CTEs."""
    # Find the last SELECT that isn't inside a CTE
    # Heuristic: the final SELECT is after the last closing paren of CTEs
    lines = sql.strip().split("\n")
    in_final = False
    final_lines = []

    for i, line in enumerate(lines):
        stripped = line.strip().upper()
        if not in_final and re.match(r"^SELECT\s+", stripped):
            # Check if this is the final select (not inside a CTE)
            # Simple heuristic: if there's no "AS (" after this SELECT, it's final
            remaining = "\n".join(lines[i:])
            if "AS (" not in remaining.upper() and "AS(" not in remaining.upper():
                in_final = True
        if in_final:
            final_lines.append(line)

    return "\n".join(final_lines) if final_lines else None


def extract_columns(select_sql: str) -> list[str]:
    """Extract column names/expressions from a SELECT statement."""
    # Remove SELECT keyword and FROM clause
    match = re.match(
        r"SELECT\s+(.*?)\s+FROM\b",
        select_sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        # Try SELECT * pattern
        if re.match(r"SELECT\s+\*", select_sql, re.IGNORECASE):
            return ["*"]
        return []

    columns_str = match.group(1)
    # Split by comma, respecting parentheses
    columns = []
    depth = 0
    current = []
    for char in columns_str:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            columns.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    if current:
        columns.append("".join(current).strip())

    return columns


def compute_text_diff(old_text: str, new_text: str) -> list[str]:
    """Compute unified diff between two text blocks."""
    old_lines = old_text.splitlines(keepends=True)
    new_lines = new_text.splitlines(keepends=True)
    return list(difflib.unified_diff(old_lines, new_lines, lineterm=""))


def diff_compiled_sql(old_sql: str, new_sql: str) -> dict:
    """Produce CTE-level diff between two compiled SQL strings."""
    old_ctes = extract_ctes(old_sql)
    new_ctes = extract_ctes(new_sql)

    old_final = extract_final_select(old_sql)
    new_final = extract_final_select(new_sql)

    all_cte_names = sorted(set(old_ctes.keys()) | set(new_ctes.keys()))

    cte_diffs = []
    for name in all_cte_names:
        old_body = old_ctes.get(name)
        new_body = new_ctes.get(name)

        if old_body is None:
            cte_diffs.append({
                "cte_name": name,
                "status": "added",
                "new_body_preview": new_body[:200] if new_body else "",
            })
        elif new_body is None:
            cte_diffs.append({
                "cte_name": name,
                "status": "removed",
                "old_body_preview": old_body[:200] if old_body else "",
            })
        elif old_body.strip() != new_body.strip():
            diff_lines = compute_text_diff(old_body, new_body)
            cte_diffs.append({
                "cte_name": name,
                "status": "modified",
                "diff": "".join(diff_lines)[:1000],
            })
        # else: unchanged — skip

    # Diff final SELECT
    final_diff = None
    if old_final and new_final and old_final.strip() != new_final.strip():
        old_cols = extract_columns(old_final)
        new_cols = extract_columns(new_final)
        added_cols = [c for c in new_cols if c not in old_cols]
        removed_cols = [c for c in old_cols if c not in new_cols]
        final_diff = {
            "status": "modified",
            "columns_added": added_cols,
            "columns_removed": removed_cols,
            "diff": "".join(compute_text_diff(old_final, new_final))[:1000],
        }
    elif old_final and not new_final:
        final_diff = {"status": "removed"}
    elif not old_final and new_final:
        final_diff = {"status": "added"}

    # Summary
    added = sum(1 for d in cte_diffs if d["status"] == "added")
    removed = sum(1 for d in cte_diffs if d["status"] == "removed")
    modified = sum(1 for d in cte_diffs if d["status"] == "modified")

    return {
        "summary": {
            "ctes_added": added,
            "ctes_removed": removed,
            "ctes_modified": modified,
            "ctes_unchanged": len(all_cte_names) - added - removed - modified,
            "final_select_changed": final_diff is not None,
        },
        "has_changes": added + removed + modified > 0 or final_diff is not None,
        "cte_diffs": cte_diffs,
        "final_select_diff": final_diff,
    }


def format_markdown(result: dict, old_path: str, new_path: str) -> str:
    """Format diff result as readable markdown."""
    lines = [
        f"# Compiled SQL Diff",
        f"",
        f"**Old:** `{old_path}`",
        f"**New:** `{new_path}`",
        f"",
    ]

    s = result["summary"]
    if not result["has_changes"]:
        lines.append("**No changes detected.**")
        return "\n".join(lines)

    lines.append(f"## Summary")
    lines.append(f"- CTEs added: {s['ctes_added']}")
    lines.append(f"- CTEs removed: {s['ctes_removed']}")
    lines.append(f"- CTEs modified: {s['ctes_modified']}")
    lines.append(f"- CTEs unchanged: {s['ctes_unchanged']}")
    lines.append(f"- Final SELECT changed: {s['final_select_changed']}")
    lines.append("")

    if result["cte_diffs"]:
        lines.append("## CTE Changes")
        for cte in result["cte_diffs"]:
            status_icon = {"added": "+", "removed": "-", "modified": "~"}
            icon = status_icon.get(cte["status"], "?")
            lines.append(f"\n### [{icon}] {cte['cte_name']} ({cte['status']})")
            if "diff" in cte:
                lines.append(f"```diff\n{cte['diff']}\n```")
            elif "new_body_preview" in cte:
                lines.append(f"```sql\n{cte['new_body_preview']}\n```")
            elif "old_body_preview" in cte:
                lines.append(f"```sql\n{cte['old_body_preview']}\n```")

    if result["final_select_diff"]:
        lines.append("\n## Final SELECT Changes")
        fd = result["final_select_diff"]
        if fd.get("columns_added"):
            lines.append(f"- Columns added: {', '.join(fd['columns_added'])}")
        if fd.get("columns_removed"):
            lines.append(f"- Columns removed: {', '.join(fd['columns_removed'])}")
        if fd.get("diff"):
            lines.append(f"```diff\n{fd['diff']}\n```")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Diff two compiled dbt SQL files at CTE level"
    )
    parser.add_argument(
        "--old",
        required=True,
        help="Path to old compiled SQL file",
    )
    parser.add_argument(
        "--new",
        required=True,
        help="Path to new compiled SQL file",
    )
    parser.add_argument(
        "--output", "-o",
        help="Path to write JSON diff result (default: stdout)",
    )
    parser.add_argument(
        "--markdown", "-m",
        help="Path to write markdown-formatted diff",
    )
    args = parser.parse_args()

    old_sql = Path(args.old).read_text()
    new_sql = Path(args.new).read_text()

    result = diff_compiled_sql(old_sql, new_sql)

    # Write JSON
    output_json = json.dumps(result, indent=2)
    if args.output:
        Path(args.output).write_text(output_json)
    else:
        print(output_json)

    # Write markdown
    if args.markdown:
        md = format_markdown(result, args.old, args.new)
        Path(args.markdown).write_text(md)


if __name__ == "__main__":
    main()
