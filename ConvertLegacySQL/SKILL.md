---
name: ConvertLegacySQL
description: Orchestrates the end-to-end conversion of a legacy multi-target SQL script into optimized, target-specific dbt models.
tools:
  - Bash
  - Read
---

# When to Use
- When starting a new project to convert a legacy procedural SQL script into a set of modern, modular dbt models on Snowflake.
- To guide a Forward Deployed Engineer through a repeatable, best-practice workflow for analysis, development, testing, and optimization.

# What This Skill Provides
This skill provides a stateful, interactive, and governed workflow that manages the entire lifecycle of a legacy code conversion. It acts as an orchestrator, calling specialized sub-skills for each phase and ensuring best practices are followed from start to finish.

# Prerequisites
- Required roles: A dedicated Snowflake role with permissions to read from source and write to a development database (e.g., `FDE_DEVELOPER_ROLE`).
- Required privileges:
  - `USAGE` on the target virtual warehouse.
  - `USAGE` on all source databases and schemas.
  - `SELECT` on all source tables.
  - `USAGE` on the target development database.
  - `CREATE SCHEMA` on the target development database (for creating isolated dev schemas).
- Environment: Snowflake Enterprise Edition is recommended to leverage all features like Clustering and Cortex AI. A `config.yml` file must be created and provided.

# Instructions
## Step 1: Pre-flight & Analysis
The skill begins by running a series of governance pre-flight checks, verifying all required RBAC privileges and setting a `QUERY_TAG` for auditing. It then calls the `AnalyzeLegacySQL` sub-skill. **This sub-skill will now explicitly ask you to confirm procedural dependencies (e.g., reliance on temporary tables) to ensure the DAG is correct.**
### Checkpoint
Before proceeding, verify: The analysis phase is complete and you have given final approval for the `logic_map.json`, including any manually specified dependencies.

## Step 2: Model Generation & Validation
The skill calls `GenerateDbtModel` and immediately runs `dbt compile` and `dbt build`. If the build fails due to a data test, it will parse the failure, **provide the exact SQL query needed for debugging**, and pause for manual correction.
### Checkpoint
Before proceeding, verify: `dbt build` completes successfully with all models running and all tests passing.

## Step 3: Data Reconciliation
The skill calls `ReconcileTables` for each converted model. This skill will **proactively warn you if it detects `FLOAT` columns** that might cause reconciliation failures before it runs the main checks.
### Checkpoint
Before proceeding, verify: The reconciliation report shows a full match for `Row Count` and `Data Hash`.

## Step 4: Performance & Cost Optimization
Once data correctness is proven, the skill calls `OptimizeDbtModel` to guide you through an interactive session of analyzing the Snowflake Query Profile and applying performance optimizations.

# Validation
After completion, verify:
1. All sub-skills completed successfully.
2. The `ReconcileTables` report shows a ✅ for all converted models.
3. The final dbt models are running efficiently in the client environment.

# Known Pitfalls
- **Procedural Dependencies:** A legacy script's reliance on the order of execution can be missed by automated parsing. The skill will now prompt for this, but you must answer carefully.
- **Implicit Type Casting:** A legacy database might implicitly cast data types in a `JOIN`. Snowflake is stricter. You must verify that the `CAST` statements in your staging models produce the correct types.
- **Timestamp and Timezone Nuances:** This is a frequent cause of `HASH_AGG` mismatches. Ensure you are handling timezones consistently in your dbt logic.

# Defaults
- Warehouse: The `snowflake_warehouse` specified in the `config.yml` file, which should default to `X-SMALL`.

# Best Practices
- **Configuration is Key:** Ensure the `config.yml` file is filled out completely and accurately before starting.
- **Start Small:** For very complex scripts, run the first conversion on only one or two target tables to validate the process.
- **Trust but Verify:** Do not blindly accept all AI-generated suggestions. Use your engineering judgment to review logic snippets and optimization suggestions.

# Examples
## Example 1: Basic Usage
User: `ConvertLegacySQL --script-path /path/to/my_proc.sql --config /path/to/client_config.yml`
Assistant: "I will now begin the conversion process for `my_proc.sql`. First, let's run the pre-flight governance checks..." (The interactive journey begins).
