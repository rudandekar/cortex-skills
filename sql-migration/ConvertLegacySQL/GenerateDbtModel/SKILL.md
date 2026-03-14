---
name: GenerateDbtModel
description: A utility that takes a structured logic description and generates boilerplate dbt model files.
tools:
  - Bash
---

# When to Use
- When called automatically by the `ConvertLegacySQL` orchestrator after the analysis phase.
- As a standalone utility to quickly scaffold dbt files from a pre-existing `logic_map.json`.
- User intent: "Generate dbt models from this logic map."

# What This Skill Provides
This skill provides a set of `.sql` and `schema.yml` files structured for a dbt project. It translates the abstract logic map into concrete, runnable dbt code.

# Prerequisites
- Required roles: None. This skill operates on the local filesystem.
- Required privileges: None.
- Environment: A valid `logic_map.json` file as input.

# Instructions
## Step 1: Generate Staging Models
For each unique source table in the `logic_map.json`, the skill creates a `stg_*.sql` file. It explicitly lists all columns and adds `CAST` functions. If the user provided a column subset during analysis, only those columns will be used.
### Checkpoint
Before proceeding, verify: The source tables in the logic map are correct.

## Step 2: Generate Marts Models
For each target table in the logic map, the skill creates a final model file and a corresponding `schema.yml` with placeholder tests.
- If the source logic contained an `UPDATE` or `MERGE`, the generated model will be configured as `materialized='incremental'` with `strategy='merge'`.
- If a procedural dependency was noted in the logic map, the skill will add a `{{ ref(...) }}` to the appropriate CTE.

# Validation
After completion, verify:
1. The `.sql` files are created in the correct `models/` subdirectories.
2. The `schema.yml` file is created and syntactically correct.
3. A subsequent `dbt compile` command runs successfully.

# Known Pitfalls
- **Garbage In, Garbage Out:** If the input `logic_map.json` contains flawed logic, this skill will faithfully reproduce that flawed logic in the dbt model.
- **Complex Logic:** The skill generates boilerplate. It does not attempt to refactor highly complex joins or window functions; that is a manual refinement step.

# Defaults
- Staging Models: Default to `materialized='view'`.
- Final Models: Default to `materialized='incremental'`.

# Best Practices
- **Treat as a First Draft:** The generated code is a starting point. Always review and refine it to add more specific business logic and robust testing.
- **Review Generated Tests:** The placeholder tests are generic. Review the generated `schema.yml` and add more specific accepted value tests or relationship tests as needed.

# Examples
## Example 1: Basic Usage
User: `GenerateDbtModel --logic-map /path/to/logic_map.json --output-dir /path/to/dbt/project/`
Assistant: "Model generation complete. The following files have been created: `models/staging/stg_customers.sql`, `models/marts/dim_customers.sql`, `models/marts/schema.yml`."
