---
name: OptimizeDbtModel
description: A guided task to help a user apply performance and cost optimizations to a dbt model on Snowflake.
tools:
  - snowflake_sql_execute
---

# When to Use
- As the final step in the `ConvertLegacySQL` workflow, after data correctness has been fully verified.
- As a standalone skill on any existing dbt model that is running slower or costing more than expected.
- User intent: "Optimize this dbt model." or "Why is this query slow?"

# What This Skill Provides
This skill provides guided expertise on interpreting Snowflake's Query Profile. It helps you diagnose performance bottlenecks and suggests concrete, cost-aware optimizations like warehouse resizing and data clustering.

# Prerequisites
- Required roles: A role with `USAGE` on a virtual warehouse.
- Required privileges: To apply clustering, the role must have `OWNERSHIP` or `ALTER` privileges on the target table.
- Environment: Snowflake Enterprise Edition is required to apply clustering. The model must have been run at least once to have a Query Profile to inspect.

# Instructions
## Step 1: Guided Query Profile Analysis
The skill prompts you to inspect the Query Profile for your `dbt run` in the Snowflake UI. It then asks targeted questions to diagnose issues.
### Checkpoint
Before proceeding, verify: The reconciliation for this model in Phase 3 was successful. Do not optimize an incorrect model.

## Step 2: Propose & Execute Optimizations
Based on your answers, the skill proposes a specific optimization.
- If you reported 'Bytes spilled to disk', it will recommend increasing the warehouse size.
- If you reported 'Ineffective partition pruning' and the table is larger than the configured threshold, it will recommend adding a clustering key.
- Before executing any DDL, it will render the exact command and ask for your explicit approval (`y/n`).

# Validation
After completion, verify:
1. You have run the model again after the optimization.
2. The Query Profile shows improvement (e.g., no more disk spilling, better partition pruning).
3. **[NEW] Graceful Failure Handling:** If a DDL command fails due to permissions, the skill will not crash. It will catch the error and provide you with the exact `GRANT` statement to give to your Snowflake administrator.

# Known Pitfalls
- **Over-clustering:** Clustering tables smaller than 1 TB can waste credits for little performance gain. The skill has a guardrail for this.
- **Bad Clustering Key:** Choosing a high-cardinality key (like `transaction_id`) to cluster on is ineffective. A `DATE` or low-cardinality `STATUS` column is much better.

# Defaults
- Warehouse: The skill assumes development is on an `X-SMALL` warehouse and only recommends increasing it when there is evidence of need.
- Clustering Key: Defaults to suggesting a `DATE` or `TIMESTAMP` column if one exists in the model.

# Best Practices
- **Correctness First, Performance Second:** Always prove your model's logic is correct before trying to make it fast.
- **Measure Everything:** Don't just apply an optimization. Re-run the model and measure the change in run time and credit consumption to prove it was effective.

# Examples
## Example 1: Basic Usage
User: `OptimizeDbtModel --model-name fct_orders`
Assistant: "Please inspect the Query Profile for the `fct_orders` model. Did you observe any significant 'Bytes spilled to local/remote storage'? (y/n)"
User: `y`
Assistant: "Disk spilling suggests the warehouse may be too small for this operation. Consider re-running on a `MEDIUM` warehouse for this specific model."

