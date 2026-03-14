---
name: AnalyzeLegacySQL
description: A guided task to help a user deconstruct a legacy SQL script and produce a machine-readable logic map.
tools:
  - snowflake_sql_execute
  - Read
---

# When to Use
- When called automatically by the main `ConvertLegacySQL` orchestrator.
- As a standalone skill when you need to quickly understand a complex or undocumented SQL script.
- User intent: "Analyze this SQL script for me."

# What This Skill Provides
This skill provides a structured `logic_map.json` file. This JSON object represents the script's write operations, the target tables, the specific `SELECT` logic, and any user-confirmed procedural dependencies.

# Prerequisites
- Required roles: A role with `USAGE` privileges on a virtual warehouse.
- Required privileges: If using Cortex AI summary, the role needs `SELECT` privileges on `SNOWFLAKE.CORTEX` functions.
- Environment: The legacy SQL script must be available to read.

# Instructions
## Step 1: Automated Summary
The skill ingests the SQL script text. If the script is over 1000 lines long, it first calls `SNOWFLAKE.CORTEX.SUMMARIZE` to provide a high-level summary for context.
### Checkpoint
Before proceeding, verify: The summary is coherent. If not, proceed with manual analysis.

## Step 2: Interactive Logic Isolation
The skill uses pattern matching to find all potential DML statements. For each one, it attempts to isolate the corresponding `SELECT` logic.
- If the skill detects dynamic SQL patterns (e.g., `EXECUTE IMMEDIATE`), it will pause and ask you to manually provide the logic.
- For each isolated snippet, the skill will render it and ask for your explicit confirmation (`y/n`). If you respond `n`, it will prompt you to provide the correct SQL.
- **[NEW] Dependency Check:** After you approve a snippet, the skill will ask: "**Does this operation depend on any previous `UPDATE` or `temporary table` created in this script? If so, please describe the dependency.**" This is critical for building the correct dbt DAG.

# Validation
After completion, verify:
1. You have given final approval for all isolated logic snippets.
2. You have accurately described all procedural dependencies when prompted.
3. The generated `logic_map.json` accurately reflects the script's functionality and dependency graph.

# Known Pitfalls
- **Dynamic SQL:** The skill cannot parse dynamically generated SQL. This requires complete manual deconstruction by the engineer.
- **Cortex Inaccuracy:** The AI-generated summary is for context only and can be imprecise. The ground truth is the code itself.

# Defaults
- Assumes a write operation is an `INSERT` unless `UPDATE` or `MERGE` keywords are explicitly found in the same statement block.

# Best Practices
- **You Are the Authority:** You, the engineer, are the final authority on the correctness of the business logic. Review every snippet carefully.
- **Document Dependencies:** When the skill prompts you for dependencies, be explicit. For example: "Yes, this logic depends on the `temp_customer_updates` table created in Step 2."

# Examples
## Example 1: Basic Usage
User: `AnalyzeLegacySQL @my_proc.sql`
Assistant: "Analysis complete. I've identified the following logic for the `INSERT` into `tbl1`. Is this correct? (y/n)"
```sql
SELECT * FROM src

User: y
Assistant: "Understood. Does this operation depend on any previous UPDATE or temporary table created in this script? If so, please describe the dependency."

---

