---
name: ReconcileTables
description: A utility to perform a deep comparison of two tables and report on any data discrepancies.
tools:
  - snowflake_sql_execute
---

# When to Use
- As a critical QA gate after a data migration or ETL conversion to prove the new process matches the old one.
- Anytime you need to confirm that two tables, which are supposed to be identical, truly are.
- User intent: "Compare these two tables for me." or "Did my dbt model produce the correct data?"

# What This Skill Provides
This skill provides a definitive, multi-stage data reconciliation report. It proves correctness by checking row counts and full data hashes, and provides row-level diffs for debugging when mismatches occur.

# Prerequisites
- Required roles: A role with `USAGE` on a virtual warehouse.
- Required privileges: `SELECT` privileges on both tables being compared.
- Environment: The two tables must exist in Snowflake.

# Instructions
## Step 1: Proactive Schema Check
Before running any comparisons, the skill runs `DESCRIBE TABLE` on both tables to inspect their schemas.
- **[NEW] FLOAT Warning:** If it detects any `FLOAT` columns, it will issue a proactive warning: "**⚠️ Floating Point Detected:** The schema contains `FLOAT` columns. `HASH_AGG` may fail due to minor precision differences. If reconciliation fails, consider casting these columns to a fixed-precision `NUMBER` type in your dbt model."
### Checkpoint
Before proceeding, acknowledge the warning if it appears.

## Step 2: Row Count Validation
The skill runs `SELECT COUNT(*)` on both tables. If the counts do not match, the process stops immediately and reports the failure.

## Step 3: Data Hash Reconciliation
If counts match, the skill runs `HASH_AGG(* ORDER BY <primary_key>)` on both tables. If the hashes do not match, it automatically proceeds to Step 4.

## Step 4: Row-level Difference Extraction
The skill runs `MINUS` queries in both directions to find the exact differing rows and saves the results to a `.csv` file for debugging.

# Validation
After completion, verify:
1. The final report shows ✅ for `Row Count`.
2. The final report shows ✅ for `Data Hash`.
3. If `Data Hash` fails, a `mismatch_details.csv` file is generated.

# Known Pitfalls
- **FLOAT Precision:** `FLOAT` columns are a primary cause of `HASH_AGG` failure. The skill now warns about this proactively.
- **Timezone Differences:** Discrepancies between `TIMESTAMP_TZ` and `TIMESTAMP_NTZ` are a common cause of hash mismatches. Ensure timezone handling is consistent.

# Defaults
- Reconciliation Method: `HASH_AGG` is the default. The skill auto-recovers by running `MINUS` on failure.

# Best Practices
- **Provide a Stable Key:** Always provide a stable `primary_key` in the `ORDER BY` clause for consistent `HASH_AGG` results.
- **Pre-filter Large Tables:** For initial development checks on huge tables, manually add a `WHERE` clause to your model to test only a subset of data.

# Examples
## Example 1: Basic Usage
User: `ReconcileTables --table-a PROD.LEGACY.CUSTOMERS --table-b DEV.DBT.DIM_CUSTOMERS --primary-key CUSTOMER_ID`
Assistant: "⚠️ **Floating Point Detected:** The schema contains a `FLOAT` column `LTV`. If reconciliation fails, consider casting this to a `NUMBER`..." (Then proceeds with reconciliation).
