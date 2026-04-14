#!/usr/bin/env python3
"""Parse dbt compile stdout/stderr into structured JSON.

Usage:
    python parse_compile_output.py --input compile_log.txt --output compile_result.json
    echo "dbt compile output..." | python parse_compile_output.py --output compile_result.json

Output JSON schema:
{
  "exit_code": int,
  "success": bool,
  "models_compiled": int,
  "errors": [...],
  "warnings": [...]
}
"""

import argparse
import json
import re
import sys
from pathlib import Path


# Error pattern definitions: (regex, error_type, extractor_function_name)
ERROR_PATTERNS = [
    (
        r"Compilation Error in model (\S+) \(([^)]+)\)\n(.*?)(?=\n\n|\Z)",
        "compilation_error",
    ),
    (
        r"Compilation Error.*?'ref\(['\"](\w+)['\"]\)'",
        "missing_ref",
    ),
    (
        r"Compilation Error.*?source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)",
        "missing_source",
    ),
    (
        r"Parsing Error in ([^\n]+)\n(.*?)(?=\n\n|\Z)",
        "yaml_parse",
    ),
    (
        r"Found a cycle.*?:\s*(.*?)(?=\n|\Z)",
        "circular_ref",
    ),
    (
        r"Duplicate.*?model.*?['\"](\w+)['\"]",
        "duplicate_model",
    ),
    (
        r"Jinja.*?Error[:\s]+(.*?)(?=\n\n|\Z)",
        "jinja_syntax",
    ),
]

# Warning patterns
WARNING_PATTERNS = [
    (
        r"WARNING.*?Deprecated.*?(.*?)(?=\n|\Z)",
        "deprecation",
    ),
    (
        r"WARNING.*?(.*?)(?=\n|\Z)",
        "generic_warning",
    ),
]

# Success pattern
SUCCESS_PATTERN = re.compile(
    r"Found (\d+) models?, (\d+) tests?, (\d+) sources?"
)
COMPILED_PATTERN = re.compile(
    r"Compiled node ['\"]?model\.(\S+?)['\"]?"
)


def extract_file_line(text: str) -> tuple[str | None, int | None]:
    """Extract file path and line number from error text."""
    match = re.search(r"in (models?/\S+\.sql)\s*(?:at line (\d+))?", text)
    if match:
        return match.group(1), int(match.group(2)) if match.group(2) else None
    match = re.search(r"\(([^)]+\.(?:sql|yml|yaml)):?(\d+)?\)", text)
    if match:
        return match.group(1), int(match.group(2)) if match.group(2) else None
    return None, None


def parse_errors(output: str) -> list[dict]:
    """Parse compile output for errors."""
    errors = []
    for pattern, error_type in ERROR_PATTERNS:
        for match in re.finditer(pattern, output, re.DOTALL | re.IGNORECASE):
            full_match = match.group(0)
            file_path, line_num = extract_file_line(full_match)

            error = {
                "type": error_type,
                "message": full_match.strip()[:500],
                "file": file_path,
                "line": line_num,
            }

            # Extract specifics per error type
            if error_type == "missing_ref":
                error["ref_name"] = match.group(1)
            elif error_type == "missing_source":
                error["source_name"] = match.group(1)
                error["table_name"] = match.group(2)
            elif error_type == "circular_ref":
                error["cycle"] = match.group(1).strip()
            elif error_type == "compilation_error":
                error["model_name"] = match.group(1)
                error["file"] = error["file"] or match.group(2)
                error["detail"] = match.group(3).strip()[:300]

            errors.append(error)

    return errors


def parse_warnings(output: str) -> list[dict]:
    """Parse compile output for warnings."""
    warnings = []
    for pattern, warn_type in WARNING_PATTERNS:
        for match in re.finditer(pattern, output, re.IGNORECASE):
            warnings.append({
                "type": warn_type,
                "message": match.group(0).strip()[:300],
            })
    return warnings


def count_compiled_models(output: str) -> int:
    """Count number of successfully compiled models."""
    matches = COMPILED_PATTERN.findall(output)
    return len(matches)


def parse_compile_output(output: str, exit_code: int = 0) -> dict:
    """Parse full dbt compile output into structured result."""
    errors = parse_errors(output)
    warnings = parse_warnings(output)
    models_compiled = count_compiled_models(output)

    # Check summary line for totals
    summary_match = SUCCESS_PATTERN.search(output)
    models_found = int(summary_match.group(1)) if summary_match else None

    return {
        "exit_code": exit_code,
        "success": exit_code == 0 and len(errors) == 0,
        "models_found": models_found,
        "models_compiled": models_compiled,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Parse dbt compile output into structured JSON"
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        help="Path to file containing dbt compile output (default: stdin)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Path to write JSON output (default: stdout)",
    )
    parser.add_argument(
        "--exit-code", "-e",
        type=int,
        default=0,
        help="Exit code from dbt compile command (default: 0)",
    )
    args = parser.parse_args()

    # Read input
    if args.input:
        text = Path(args.input).read_text()
    else:
        text = sys.stdin.read()

    # Parse
    result = parse_compile_output(text, args.exit_code)

    # Write output
    output_json = json.dumps(result, indent=2)
    if args.output:
        Path(args.output).write_text(output_json)
    else:
        print(output_json)


if __name__ == "__main__":
    main()
