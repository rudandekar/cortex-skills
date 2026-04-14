#!/usr/bin/env python3
"""
Sybase SQL Parser: Produces object_inventory.json and dependency_graph.json
from 12 Sybase source files (sql1-sql5, ETL_Script_01-ETL_Script_07).
"""

import json
import os
import re
from collections import defaultdict
from pathlib import Path

SOURCE_DIR = Path("/home/rudandekar/.snowflake/cortex/skills/sql-migration/sybase")
OUTPUT_DIR = SOURCE_DIR / "converted" / "logs"

FILES = [
    "sql1.sql", "sql2.sql", "sql3.sql", "sql4.sql", "sql5.sql",
    "ETL_Script_01.sql", "ETL_Script_02.sql", "ETL_Script_03.sql",
    "ETL_Script_04.sql", "ETL_Script_05.sql", "ETL_Script_06.sql",
    "ETL_Script_07.sql",
]


# ---------------------------------------------------------------------------
# Construct Detection Patterns (case-insensitive)
# ---------------------------------------------------------------------------
CONSTRUCT_PATTERNS = {
    "IF_EXISTS_DROP":       r'(?i)IF\s+EXISTS\s*\(.*?sysobjects.*?\)\s*(DROP|BEGIN)',
    "IF_NOT_EXISTS_CREATE": r'(?i)IF\s+NOT\s+EXISTS\s*\(.*?sysobjects.*?\)',
    "GETDATE":              r'(?i)\bGETDATE\s*\(',
    "ISNULL":               r'(?i)\bISNULL\s*\(',
    "CONVERT_WITH_STYLE":   None,  # Special handler
    "CONVERT_SIMPLE":       None,  # Special handler
    "DATEPART":             r'(?i)\bDATEPART\s*\(',
    "DATENAME":             r'(?i)\bDATENAME\s*\(',
    "DATEDIFF":             r'(?i)\bDATEDIFF\s*\(',
    "DATEADD":              r'(?i)\bDATEADD\s*\(',
    "IDENTITY_COLUMN":      r'(?i)\bIDENTITY\s*[\(\(]',
    "LINKED_SERVER":        r'(?i)\bOperationalDB\s*\.\s*\.|\bOperationalDB\s*\.\s*dbo\s*\.\s*|\bDataWarehouse\s*\.\s*\.',
    "SET_NOCOUNT":          r'(?i)\bSET\s+NOCOUNT\s+ON\b',
    "SET_TEMPORARY_OPTION": r'(?i)\bSET\s+TEMPORARY\s+OPTION\b',
    "PRINT":                r'(?i)\bPRINT\s+',
    "RAISERROR":            r'(?i)\bRAISERROR\s*\(',
    "DECLARE_AT_VAR":       r'(?i)\bDECLARE\s+@',
    "WHILE_LOOP":           r'(?i)\bWHILE\b.*\bBEGIN\b',
    "CURSOR":               r'(?i)\bCURSOR\s+FOR\b',
    "DYNAMIC_SQL":          r'(?i)\bEXEC\s*\(\s*@',
    "SELECT_INTO_TEMP":     r'(?i)\bSELECT\b.*?\bINTO\s+#',
    "CREATE_TEMP_TABLE":    r'(?i)\bCREATE\s+TABLE\s+#',
    "INSERT_EXEC":          r'(?i)\bINSERT\s+INTO\b.*\bEXEC\b',
    "AT_AT_ERROR":          r'@@ERROR',
    "AT_AT_ROWCOUNT":       r'@@ROWCOUNT',
    "AT_AT_FETCH_STATUS":   r'@@FETCH_STATUS',
    "BEGIN_TRAN":           r'(?i)\bBEGIN\s+TRAN',
    "TINYINT":              r'(?i)\bTINYINT\b',
    "MONEY":                r'(?i)\bMONEY\b',
    "SYSOBJECTS":           r'(?i)\bsysobjects\b',
    "SP_BINDRULE":          r'(?i)\bsp_bindrule\b',
    "SP_BINDDEFAULT":       r'(?i)\bsp_binddefault\b',
    "CREATE_RULE":          r'(?i)\bCREATE\s+RULE\b',
    "CREATE_DEFAULT":       r'(?i)\bCREATE\s+DEFAULT\b',
    "IQ_UNIQUE":            r'(?i)\bIQ\s+UNIQUE\b',
    "IN_DBASPACE":          r'(?i)\bIN\s+DBASPACE\b',
    "COMPUTE_BY":           r'(?i)\bCOMPUTE\b.*\bBY\b',
    "INSTEAD_OF_TRIGGER":   r'(?i)\bINSTEAD\s+OF\b',
    "AFTER_TRIGGER":        r'(?i)\bAFTER\s+(UPDATE|INSERT|DELETE)\b',
    "GOTO":                 r'(?i)\bGOTO\s+\w+',
    "USER_NAME":            r'(?i)\bUSER_NAME\s*\(',
    "STR_FUNCTION":         r'(?i)\bSTR\s*\(',
    "STUFF_FUNCTION":       r'(?i)\bSTUFF\s*\(',
    "PATINDEX":             r'(?i)\bPATINDEX\s*\(',
    "CHARINDEX":            r'(?i)\bCHARINDEX\s*\(',
    "LEN_FUNCTION":         r'(?i)\bLEN\s*\(',
    "OBJECT_ID":            r'(?i)\bOBJECT_ID\s*\(',
}


def detect_convert(block_text):
    """Detect CONVERT_WITH_STYLE (3 args) vs CONVERT_SIMPLE (2 args)."""
    has_style = False
    has_simple = False
    # Find all CONVERT( calls and count commas inside balanced parens
    for m in re.finditer(r'(?i)\bCONVERT\s*\(', block_text):
        start = m.end()
        depth = 1
        commas = 0
        i = start
        while i < len(block_text) and depth > 0:
            ch = block_text[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 1:
                commas += 1
            i += 1
        if commas >= 2:
            has_style = True
        elif commas == 1:
            has_simple = True
    return has_style, has_simple


def detect_constructs(block_text):
    """Return list of detected Sybase constructs in a block."""
    found = []
    # Always add GO since blocks are split on GO
    found.append("GO")

    for name, pattern in CONSTRUCT_PATTERNS.items():
        if name in ("CONVERT_WITH_STYLE", "CONVERT_SIMPLE"):
            continue
        if pattern and re.search(pattern, block_text):
            found.append(name)

    # Special CONVERT handling
    has_style, has_simple = detect_convert(block_text)
    if has_style:
        found.append("CONVERT_WITH_STYLE")
    if has_simple:
        found.append("CONVERT_SIMPLE")

    # Also check WHILE_LOOP more carefully (multiline)
    if "WHILE_LOOP" not in found:
        if re.search(r'(?is)\bWHILE\b.*?\bBEGIN\b', block_text):
            found.append("WHILE_LOOP")

    return sorted(set(found))


def classify_block(block_text):
    """Classify a block as DDL, DML, PROCEDURAL, or UTILITY."""
    t = block_text.strip()
    upper = t.upper()

    # Remove leading comments for classification
    lines = t.split('\n')
    first_stmt_line = ''
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('--'):
            first_stmt_line = stripped.upper()
            break

    # PROCEDURAL checks (highest priority for CREATE PROCEDURE, etc.)
    if re.search(r'(?i)\bCREATE\s+(PROCEDURE|PROC)\b', t):
        return "PROCEDURAL"
    if re.search(r'(?i)\bCREATE\s+TRIGGER\b', t):
        return "PROCEDURAL"
    if re.search(r'(?i)\bCREATE\s+RULE\b', t):
        return "PROCEDURAL"
    if re.search(r'(?i)\bCREATE\s+DEFAULT\b', t):
        return "PROCEDURAL"
    # Blocks with CURSOR, WHILE+BEGIN, DECLARE+complex logic
    if re.search(r'(?i)\bCURSOR\s+FOR\b', t):
        return "PROCEDURAL"
    if re.search(r'(?is)\bDECLARE\b.*\bWHILE\b.*\bBEGIN\b', t):
        return "PROCEDURAL"
    # Blocks with DECLARE + BEGIN TRAN + complex procedural logic
    if re.search(r'(?is)\bDECLARE\b.*\bBEGIN\s+TRAN\b', t):
        return "PROCEDURAL"
    # Blocks with significant DECLARE + conditional logic (DQ checks etc.)
    declare_count = len(re.findall(r'(?i)\bDECLARE\s+@', t))
    if_count = len(re.findall(r'(?i)\bIF\s+', t))
    if declare_count >= 2 and if_count >= 2:
        return "PROCEDURAL"

    # DDL checks
    if re.search(r'(?i)\bCREATE\s+TABLE\b', t) and not re.search(r'(?i)\bCREATE\s+TABLE\s+#', t):
        return "DDL"
    if re.search(r'(?i)\bCREATE\s+VIEW\b', t):
        return "DDL"
    if re.search(r'(?i)\bCREATE\s+INDEX\b', t):
        return "DDL"
    if re.search(r'(?i)\bALTER\s+TABLE\b', t):
        return "DDL"
    # IF EXISTS DROP patterns (DDL drops)
    if re.search(r'(?i)IF\s+EXISTS.*DROP\s+(TABLE|VIEW)\b', t) and not re.search(r'(?i)\bINSERT\b', t):
        return "DDL"
    # IF NOT EXISTS CREATE TABLE (non-temp)
    if re.search(r'(?i)IF\s+NOT\s+EXISTS.*CREATE\s+TABLE\b', t) and not re.search(r'(?i)CREATE\s+TABLE\s+#', t):
        return "DDL"

    # DML checks
    if re.search(r'(?i)\bINSERT\s+INTO\b', t):
        return "DML"
    if re.search(r'(?i)\bUPDATE\s+\w', t) and not re.search(r'(?i)\bCREATE\b', t):
        return "DML"
    if re.search(r'(?i)\bDELETE\s+FROM\b', t):
        return "DML"
    if re.search(r'(?i)\bMERGE\b', t):
        return "DML"

    # UTILITY
    if re.search(r'(?i)^\s*SET\s+', first_stmt_line):
        return "UTILITY"
    if re.search(r'(?i)^\s*PRINT\b', first_stmt_line):
        return "UTILITY"
    if re.search(r'(?i)^\s*EXEC\s+', first_stmt_line):
        return "UTILITY"
    if re.search(r'(?i)^\s*TRUNCATE\b', first_stmt_line):
        return "UTILITY"
    if re.search(r'(?i)^\s*SELECT\b', first_stmt_line) and not re.search(r'(?i)\bINTO\b', t):
        return "UTILITY"

    # Default
    return "UTILITY"


def extract_primary_object(block_text, classification):
    """Extract the primary object name from a block."""
    t = block_text.strip()

    # CREATE INDEX ON table
    m = re.search(r'(?i)\bCREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)', t)
    if m:
        return m.group(2)  # return the table name, not the index name

    # CREATE TABLE / VIEW / PROCEDURE / TRIGGER / RULE / DEFAULT
    m = re.search(r'(?i)\bCREATE\s+(TABLE|VIEW|PROCEDURE|PROC|TRIGGER|RULE|DEFAULT)\s+(?:dbo\.)?([\w#]+)', t)
    if m:
        return m.group(2)

    # IF EXISTS DROP TABLE/VIEW
    m = re.search(r'(?i)DROP\s+(TABLE|VIEW|PROCEDURE|PROC|TRIGGER|RULE|DEFAULT)\s+(?:dbo\.)?([\w#]+)', t)
    if m:
        return m.group(2)

    # IF NOT EXISTS ... CREATE TABLE
    m = re.search(r'(?i)CREATE\s+TABLE\s+([\w#]+)', t)
    if m:
        return m.group(1)

    # INSERT INTO target
    m = re.search(r'(?i)\bINSERT\s+INTO\s+(?:DataWarehouse\.\.)?([\w#]+)', t)
    if m:
        return m.group(1)

    # UPDATE target — need to handle Sybase UPDATE alias pattern
    # In Sybase: UPDATE alias SET ... FROM table alias ...
    # So first try to find the actual table from the FROM clause
    um = re.search(r'(?i)\bUPDATE\s+([\w]+)\b', t)
    if um:
        alias_or_table = um.group(1)
        # Check if there's a FROM clause with the real table
        fm = re.search(r'(?i)\bFROM\s+([\w]+)\s+' + re.escape(alias_or_table) + r'\b', t)
        if fm:
            return fm.group(1)  # Return the real table, not the alias
        # Also check: UPDATE table SET ... FROM table alias
        if alias_or_table.upper() not in ('SET',):
            return alias_or_table

    # TRUNCATE TABLE
    m = re.search(r'(?i)\bTRUNCATE\s+TABLE\s+([\w]+)', t)
    if m:
        return m.group(1)

    # EXEC sp_bind*
    m = re.search(r'(?i)\bEXEC\s+(sp_bind\w+)', t)
    if m:
        return m.group(1)

    # EXEC sp_*
    m = re.search(r'(?i)\bEXEC\s+(sp_\w+)', t)
    if m:
        return m.group(1)

    # SET TEMPORARY OPTION
    m = re.search(r'(?i)\bSET\s+TEMPORARY\s+OPTION\s+(\w+)', t)
    if m:
        return f"SET_{m.group(1)}"

    # SELECT validation
    if re.search(r'(?i)^\s*SELECT\b', t):
        # Try to find FROM table
        m = re.search(r'(?i)\bFROM\s+([\w]+)', t)
        if m:
            return m.group(1)

    # PRINT
    if re.search(r'(?i)^\s*PRINT\b', t.strip()):
        return "PRINT_STMT"

    # ALTER TABLE
    m = re.search(r'(?i)\bALTER\s+TABLE\s+([\w]+)', t)
    if m:
        return m.group(1)

    return "UNKNOWN"


def _strip_comments_and_strings(text):
    """Remove SQL comments and string literals to avoid false positive object refs."""
    # Remove single-line comments
    text = re.sub(r'--[^\n]*', '', text)
    # Remove string literals (replace with empty string placeholder)
    text = re.sub(r"'[^']*'", "''", text)
    return text


# Words that should never be treated as object names
_SQL_NOISE = {
    # SQL keywords
    'SELECT', 'WHERE', 'AND', 'OR', 'NOT', 'NULL', 'EXISTS', 'AS', 'SET',
    'BEGIN', 'END', 'ON', 'IN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'VALUES',
    'INTO', 'FROM', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS',
    'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE', 'VIEW',
    'INDEX', 'TRIGGER', 'PROCEDURE', 'PROC', 'RULE', 'DEFAULT', 'EXEC',
    'DECLARE', 'PRINT', 'IF', 'WHILE', 'RETURN', 'ROLLBACK', 'COMMIT',
    'TRAN', 'TRANSACTION', 'FETCH', 'OPEN', 'CLOSE', 'DEALLOCATE',
    'CURSOR', 'FOR', 'READ', 'ONLY', 'NEXT', 'GO', 'LIKE', 'BETWEEN',
    'GROUP', 'BY', 'ORDER', 'HAVING', 'UNION', 'ALL', 'DISTINCT',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ROUND', 'CAST', 'CONVERT',
    'ISNULL', 'NULLIF', 'COALESCE', 'UPPER', 'LOWER', 'LTRIM', 'RTRIM',
    'DATEPART', 'DATENAME', 'DATEDIFF', 'DATEADD', 'GETDATE', 'SUBSTRING',
    'CHARINDEX', 'PATINDEX', 'LEN', 'REPLACE', 'STUFF', 'STR', 'ABS',
    'PRIMARY', 'KEY', 'IDENTITY', 'CONSTRAINT', 'UNIQUE', 'CHECK',
    'RAISERROR', 'GOTO', 'COMPUTE', 'AFTER', 'INSTEAD', 'OF',
    'VARCHAR', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'DATE',
    'DATETIME', 'CHAR', 'MONEY', 'BIT', 'NUMERIC', 'FLOAT', 'REAL',
    # Common aliases and non-objects
    'dups', 'dbo', 'inserted', 'deleted', 'combos', 'rc', 'x', 'cnt',
    'DQ', 'FAIL', 'PASS', 'WARNING', 'VALID', 'INVALID',
    # Short words / noise from regex hits in prose
    'a', 'to', 'the', 'of', 'is', 'no', 'at', 'by', 'or', 'an', 'do',
    # False positives from RAISERROR messages, PRINT strings, comments
    'DOB', 'AS', 'approved', 'attempts', 'audit', 'changed', 'concatenated',
    'encountered', 'existing', 'external', 'facts', 'failed', 'incident',
    'loss', 'operational', 'position', 'staging', 'territory', 'date',
    'sysobjects', 'OBJECT_ID', 'tempdb',
    # Common table aliases that are not real objects
    'dp', 'dc', 'da', 'dd', 'fp', 'fc', 'sp', 'sc', 'acm', 'apm',
    's', 'd', 'p', 'c', 'r', 'u', 'h', 'v', 't', 'z', 'ag', 'ag2',
    'dd_eff', 'dd_exp', 'dd_inc', 'dd_rep', 'dd_cls', 'py', 'a2',
    # UPDATE aliases (Sybase UPDATE alias SET pattern)
    'pmt', 'pr', 'cl',
}


def extract_secondary_objects(block_text, primary_obj):
    """Extract all referenced objects (tables, views, procs) from a block."""
    t = _strip_comments_and_strings(block_text)
    objects = set()

    # FROM OperationalDB..table pattern (two dots)
    for m in re.finditer(r'(?i)\bOperationalDB\.\.([\w]+)', t):
        objects.add("OperationalDB.." + m.group(1))

    # FROM OperationalDB.dbo.table pattern
    for m in re.finditer(r'(?i)\bOperationalDB\.dbo\.([\w]+)', t):
        objects.add("OperationalDB.dbo." + m.group(1))

    # DataWarehouse..table
    for m in re.finditer(r'(?i)\bDataWarehouse\.\.([\w]+)', t):
        name = m.group(1)
        if name.upper() not in _SQL_NOISE:
            objects.add(name)

    # FROM / JOIN references (non-linked-server)
    for m in re.finditer(r'(?i)\b(?:FROM|JOIN)\s+([\w]+)\b', t):
        name = m.group(1)
        if name.upper() not in _SQL_NOISE and not name.startswith('@'):
            objects.add(name)

    # INSERT INTO target
    for m in re.finditer(r'(?i)\bINSERT\s+INTO\s+(?:DataWarehouse\.\.)?([\w#]+)', t):
        name = m.group(1)
        if name.upper() not in _SQL_NOISE:
            objects.add(name)

    # UPDATE target (table name directly after UPDATE, not an alias)
    for m in re.finditer(r'(?i)\bUPDATE\s+([\w]+)', t):
        name = m.group(1)
        if name.upper() not in _SQL_NOISE and not name.startswith('@'):
            objects.add(name)

    # EXEC procedure (only sp_ prefixed stored procs, not sp_bindrule/sp_binddefault)
    for m in re.finditer(r'(?i)\bEXEC\s+(sp_\w+)', t):
        objects.add(m.group(1))

    # sp_bindrule / sp_binddefault targets (from non-stripped text for string literals)
    for m in re.finditer(r"(?i)\bsp_bind(?:rule|default)\s+'([^']+)'\s*,\s*'([^']+)'", block_text):
        rule_name = m.group(1)
        target = m.group(2).split('.')[0]  # table.column -> table
        objects.add(rule_name)
        objects.add(target)

    # TRUNCATE TABLE
    for m in re.finditer(r'(?i)\bTRUNCATE\s+TABLE\s+([\w]+)', t):
        objects.add(m.group(1))

    # CREATE INDEX ... ON table
    for m in re.finditer(r'(?i)\bON\s+([\w]+)\s*\(', t):
        name = m.group(1)
        if name.upper() not in _SQL_NOISE:
            objects.add(name)

    # Remove primary object, UNKNOWN, and noise words
    objects.discard(primary_obj)
    objects.discard("UNKNOWN")
    objects.discard("PRINT_STMT")

    # Filter out noise
    filtered = set()
    # Known cursor names and non-objects to exclude
    cursor_names = {'agent_cursor', 'archive_cursor', 'claims_dq_cursor', 'table_cursor'}
    noise_lower = {n.lower() for n in _SQL_NOISE}
    for obj in objects:
        lower = obj.lower()
        if lower in noise_lower:
            continue
        if len(obj) <= 2 and '.' not in obj and '#' not in obj:
            continue
        if obj in cursor_names:
            continue
        if obj == 'OperationalDB':  # bare name without dots
            continue
        # Keep: anything with underscore, dot, or hash (real object names)
        # Keep: anything that looks like a table/view/proc/rule name
        if ('_' in obj or '.' in obj or '#' in obj
            or obj.startswith('stg_') or obj.startswith('dim_')
            or obj.startswith('fact_') or obj.startswith('agg_')
            or obj.startswith('v_') or obj.startswith('sp_')
            or obj.startswith('rule_') or obj.startswith('def_')
            or obj.startswith('trg_') or obj.startswith('etl_')
            or obj.startswith('#')
            or 'OperationalDB' in obj):
            filtered.add(obj)

    return sorted(filtered)


def compute_line_range(file_lines, block_text, search_start):
    """Find approximate line range of a block in the original file."""
    # Normalize for matching
    block_stripped = block_text.strip()
    if not block_stripped:
        return [search_start + 1, search_start + 1]

    first_line = block_stripped.split('\n')[0].strip()
    last_line = block_stripped.split('\n')[-1].strip()

    start_line = search_start + 1
    end_line = start_line

    for i in range(search_start, len(file_lines)):
        if first_line and first_line in file_lines[i]:
            start_line = i + 1  # 1-indexed
            break

    for i in range(start_line - 1, len(file_lines)):
        if last_line and last_line in file_lines[i]:
            end_line = i + 1
            break

    if end_line < start_line:
        end_line = start_line + len(block_stripped.split('\n')) - 1

    return [start_line, end_line]


def parse_file(filepath):
    """Parse a single SQL file into inventory blocks."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        file_lines = content.split('\n')

    file_stem = Path(filepath).stem

    # Split on GO batch separator: a line that is exactly "GO" (possibly with whitespace)
    # We need to handle lines that have "GO" possibly with leading/trailing spaces
    blocks_raw = re.split(r'\n\s*GO\s*\n', content, flags=re.IGNORECASE)

    # Also handle GO at end of file
    if blocks_raw and re.match(r'^\s*GO\s*$', blocks_raw[-1].strip(), re.IGNORECASE):
        blocks_raw[-1] = ''

    inventory = []
    seq = 0
    search_pos = 0

    for block_text in blocks_raw:
        # Skip empty blocks (whitespace/comments only)
        stripped = re.sub(r'--[^\n]*', '', block_text).strip()
        if not stripped:
            # Still advance search position
            search_pos += len(block_text.split('\n'))
            continue

        seq += 1
        block_id = f"{file_stem}_{seq:03d}"

        classification = classify_block(block_text)
        primary_obj = extract_primary_object(block_text, classification)
        secondary_objs = extract_secondary_objects(block_text, primary_obj)
        constructs = detect_constructs(block_text)
        line_range = compute_line_range(file_lines, block_text, search_pos)

        inventory.append({
            "block_id": block_id,
            "file": Path(filepath).name,
            "classification": classification,
            "primary_object_name": primary_obj,
            "secondary_objects": secondary_objs,
            "sybase_constructs_detected": constructs,
            "line_range": line_range,
        })

        search_pos = line_range[1]

    return inventory


def classify_object_layer(obj_name):
    """Classify an object into a dependency layer."""
    name = obj_name.lower()

    if 'operationaldb' in name:
        return "external"
    if name.startswith('stg_'):
        return "staging"
    if name.startswith('dim_'):
        return "dimension"
    if name.startswith('fact_'):
        return "fact"
    if name.startswith('agg_'):
        return "aggregation"
    if name.startswith('v_'):
        return "view"
    if name.startswith('sp_') and name not in ('sp_bindrule', 'sp_binddefault'):
        return "procedure"
    if name.startswith('#'):
        return "utility"
    if name.startswith('rule_') or name.startswith('def_'):
        return "utility"
    if name.startswith('trg_'):
        return "utility"
    if name == 'etl_audit_log':
        return "utility"
    if name == 'fact_policy_archive':
        return "utility"

    return "utility"


def _is_real_object_name(name):
    """Return True if name looks like a real database object, not a noise word."""
    if not name or name == 'UNKNOWN' or name == 'PRINT_STMT':
        return False
    lower = name.lower()
    noise_lower = {n.lower() for n in _SQL_NOISE}
    if lower in noise_lower:
        return False
    if len(name) <= 2 and '.' not in name and '#' not in name:
        return False
    if name in {'agent_cursor', 'archive_cursor', 'claims_dq_cursor', 'table_cursor'}:
        return False
    if name == 'OperationalDB':
        return False
    # Must contain underscore, dot, hash, or be a known prefix pattern
    if ('_' in name or '.' in name or '#' in name
        or any(name.startswith(p) for p in ('stg_', 'dim_', 'fact_', 'agg_',
                                             'v_', 'sp_', 'rule_', 'def_',
                                             'trg_', 'etl_', '#'))
        or 'OperationalDB' in name):
        return True
    return False


def build_dependency_graph(all_inventory):
    """Build dependency graph from inventory."""
    # Collect all CREATED objects
    created_objects = {}
    for item in all_inventory:
        cls = item['classification']
        prim = item['primary_object_name']
        if cls in ('DDL', 'PROCEDURAL') and _is_real_object_name(prim):
            if prim not in created_objects:
                created_objects[prim] = {
                    "created_in": [],
                    "layer": classify_object_layer(prim),
                }
            created_objects[prim]["created_in"].append(item['block_id'])

    # Also collect objects referenced in DML blocks as "created" if they are INSERT targets
    for item in all_inventory:
        prim = item['primary_object_name']
        if _is_real_object_name(prim) and prim not in created_objects:
            # Check if it appears as a real table target in DML
            if item['classification'] == 'DML' and not prim.startswith('#'):
                created_objects[prim] = {
                    "created_in": [item['block_id']],
                    "layer": classify_object_layer(prim),
                }

    # Collect all REFERENCED objects
    all_refs = set()
    for item in all_inventory:
        for obj in item['secondary_objects']:
            # Normalize linked server refs
            clean = obj
            if 'OperationalDB..' in obj:
                clean = obj
            elif 'OperationalDB.dbo.' in obj:
                clean = obj
            all_refs.add(clean)

    # Build nodes
    nodes = {}
    for obj, info in created_objects.items():
        nodes[obj] = {
            "name": obj,
            "layer": info["layer"],
            "created_in_blocks": info["created_in"],
            "type": "created",
        }

    # Add external nodes for referenced-but-not-created
    for ref in all_refs:
        if not _is_real_object_name(ref):
            continue
        clean_name = ref
        if 'OperationalDB' in ref:
            if clean_name not in nodes:
                nodes[clean_name] = {
                    "name": clean_name,
                    "layer": "external",
                    "created_in_blocks": [],
                    "type": "external_reference",
                }
        elif ref not in nodes and ref not in ('sp_bindrule', 'sp_binddefault') and not ref.startswith('#'):
            nodes[ref] = {
                "name": ref,
                "layer": classify_object_layer(ref),
                "created_in_blocks": [],
                "type": "referenced_only",
            }

    # Build edges
    edges = []
    edge_set = set()
    for item in all_inventory:
        target = item['primary_object_name']
        if not _is_real_object_name(target):
            continue
        for src in item['secondary_objects']:
            if src == target:
                continue
            # Normalize
            src_clean = src
            tgt_clean = target
            edge_key = (src_clean, tgt_clean)
            if edge_key not in edge_set:
                edge_set.add(edge_key)
                edges.append({
                    "from": src_clean,
                    "to": tgt_clean,
                    "via_block": item['block_id'],
                    "relationship": "feeds_into",
                })

    # Group nodes by layer
    layers = defaultdict(list)
    for name, info in nodes.items():
        layers[info["layer"]].append(name)

    # Topological sort
    topo_order = topological_sort(nodes, edges, layers)

    # Warnings
    warnings = []
    # External references
    ext_refs = [n for n, info in nodes.items() if info["type"] == "external_reference"]
    if ext_refs:
        warnings.append({
            "type": "EXTERNAL_REFERENCES",
            "message": f"{len(ext_refs)} objects are referenced but not created in these scripts",
            "objects": sorted(ext_refs),
        })

    # Check for potential circular dependencies
    circular = detect_cycles(edges)
    if circular:
        warnings.append({
            "type": "POTENTIAL_CIRCULAR_DEPENDENCY",
            "message": "Potential circular dependencies detected",
            "cycles": circular,
        })

    return {
        "nodes": nodes,
        "edges": edges,
        "layers": {k: sorted(v) for k, v in layers.items()},
        "topological_order": topo_order,
        "warnings": warnings,
    }


def topological_sort(nodes, edges, layers):
    """Produce a topological sort order for conversion."""
    # Layer priority
    layer_priority = {
        "external": 0,
        "utility": 1,
        "staging": 2,
        "dimension": 3,
        "fact": 4,
        "aggregation": 5,
        "view": 6,
        "procedure": 7,
    }

    # Build adjacency for Kahn's algorithm
    in_degree = defaultdict(int)
    adj = defaultdict(set)
    all_nodes = set(nodes.keys())

    for edge in edges:
        src = edge["from"]
        tgt = edge["to"]
        if src in all_nodes and tgt in all_nodes:
            if tgt not in adj[src]:
                adj[src].add(tgt)
                in_degree[tgt] += 1

    # Initialize in_degree for nodes with no incoming
    for n in all_nodes:
        if n not in in_degree:
            in_degree[n] = 0

    # Kahn's with layer-priority tie-breaking
    queue = []
    for n in all_nodes:
        if in_degree[n] == 0:
            layer = nodes[n]["layer"]
            queue.append((layer_priority.get(layer, 99), n))
    queue.sort()

    result = []
    visited = set()

    while queue:
        _, node = queue.pop(0)
        if node in visited:
            continue
        visited.add(node)
        result.append(node)

        for neighbor in sorted(adj.get(node, [])):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0 and neighbor not in visited:
                layer = nodes[neighbor]["layer"]
                queue.append((layer_priority.get(layer, 99), neighbor))
                queue.sort()

    # Add any remaining (cycle members)
    remaining = all_nodes - visited
    for n in sorted(remaining, key=lambda x: (layer_priority.get(nodes[x]["layer"], 99), x)):
        result.append(n)

    return result


def detect_cycles(edges):
    """Simple cycle detection."""
    adj = defaultdict(set)
    for e in edges:
        adj[e["from"]].add(e["to"])

    cycles = []
    # Check for direct A->B->A cycles
    for src, targets in adj.items():
        for tgt in targets:
            if src in adj.get(tgt, set()):
                cycle = sorted([src, tgt])
                if cycle not in cycles:
                    cycles.append(cycle)
    return cycles


def main():
    all_inventory = []

    for fname in FILES:
        fpath = SOURCE_DIR / fname
        if not fpath.exists():
            print(f"WARNING: {fpath} not found, skipping")
            continue
        blocks = parse_file(fpath)
        all_inventory.extend(blocks)
        print(f"Parsed {fname}: {len(blocks)} blocks")

    # Write object_inventory.json
    inv_path = OUTPUT_DIR / "object_inventory.json"
    with open(inv_path, 'w') as f:
        json.dump({
            "metadata": {
                "source_files": FILES,
                "total_blocks": len(all_inventory),
                "generated_by": "parse_sybase.py",
            },
            "blocks": all_inventory,
        }, f, indent=2)
    print(f"\nWrote {inv_path} ({len(all_inventory)} blocks)")

    # Build and write dependency_graph.json
    graph = build_dependency_graph(all_inventory)
    graph_path = OUTPUT_DIR / "dependency_graph.json"
    with open(graph_path, 'w') as f:
        json.dump(graph, f, indent=2)
    print(f"Wrote {graph_path}")

    # === SUMMARY TABLES ===
    print("\n" + "=" * 100)
    print("FINAL SUMMARY")
    print("=" * 100)

    # Per-file summary
    file_stats = defaultdict(lambda: {"blocks": 0, "DDL": 0, "DML": 0, "PROCEDURAL": 0, "UTILITY": 0, "constructs": set()})
    for item in all_inventory:
        f = item["file"]
        file_stats[f]["blocks"] += 1
        file_stats[f][item["classification"]] += 1
        for c in item["sybase_constructs_detected"]:
            file_stats[f]["constructs"].add(c)

    print("\n| File | Block Count | DDL | DML | PROC | UTIL | Key Sybase Constructs |")
    print("|------|-------------|-----|-----|------|------|-----------------------|")
    for fname in FILES:
        s = file_stats.get(fname, {"blocks": 0, "DDL": 0, "DML": 0, "PROCEDURAL": 0, "UTILITY": 0, "constructs": set()})
        key_constructs = sorted(s["constructs"] - {"GO"})
        # Show top constructs
        display = ", ".join(key_constructs[:12])
        if len(key_constructs) > 12:
            display += f" (+{len(key_constructs) - 12} more)"
        print(f"| {fname:<22s} | {s['blocks']:>5d}       | {s['DDL']:>3d} | {s['DML']:>3d} | {s['PROCEDURAL']:>4d} | {s['UTILITY']:>4d} | {display} |")

    # Layer summary
    print("\n| Layer | Objects | Count |")
    print("|-------|---------|-------|")
    for layer_name in ["external", "utility", "staging", "dimension", "fact", "aggregation", "view", "procedure"]:
        objs = graph["layers"].get(layer_name, [])
        if objs:
            display = ", ".join(objs[:8])
            if len(objs) > 8:
                display += f" (+{len(objs) - 8} more)"
            print(f"| {layer_name:<12s} | {display:<60s} | {len(objs):>5d} |")

    # Topological order
    print("\nTopological Sort Order (conversion sequence):")
    for i, obj in enumerate(graph["topological_order"], 1):
        layer = graph["nodes"][obj]["layer"]
        print(f"  {i:>3d}. [{layer:<12s}] {obj}")

    # Warnings
    if graph["warnings"]:
        print("\nWarnings:")
        for w in graph["warnings"]:
            print(f"  - {w['type']}: {w['message']}")
            if 'objects' in w:
                for o in w['objects'][:10]:
                    print(f"      * {o}")

    print("\n" + "=" * 100)
    print(f"Total: {len(all_inventory)} blocks across {len(FILES)} files")
    print(f"Output: {inv_path}")
    print(f"Output: {graph_path}")


if __name__ == "__main__":
    main()
