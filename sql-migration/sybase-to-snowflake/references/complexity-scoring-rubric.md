# Complexity Scoring Rubric

> Reference for the `sybase-to-snowflake` skill v0.3.0. Consulted during Step 4 (Complexity Classifier)
> and Step 7 (Fidelity Scoring) for threshold adjustment and quarantine routing.
> Each object receives a weighted score. Total score determines the complexity tier.

---

## Scoring Dimensions

Each SQL object is scored across 7 dimensions. Each dimension contributes 0-3 points.

### Dimension 1: Line Count

| Source Lines | Points | Rationale |
|-------------|--------|-----------|
| 1-25 | 0 | Trivial object — single statement or simple DDL |
| 26-75 | 1 | Moderate size — multi-statement block or medium DDL |
| 76-200 | 2 | Large object — multi-step DML or procedure |
| 200+ | 3 | Very large — complex procedure or multi-operation block |

### Dimension 2: Join Complexity

| Join Count | Points | Rationale |
|-----------|--------|-----------|
| 0-1 | 0 | No joins or single join — straightforward |
| 2-3 | 1 | Multiple joins — moderate complexity |
| 4-6 | 2 | Many joins — careful ordering and key alignment needed |
| 7+ | 3 | Complex multi-table operation — high translation risk |

### Dimension 3: SCD Pattern

| SCD Type | Points | Rationale |
|----------|--------|-----------|
| None | 0 | No slowly changing dimension logic |
| SCD Type 1 (overwrite) | 1 | Simple UPDATE + INSERT → MERGE conversion |
| SCD Type 2 (versioning) | 3 | Expire + insert pattern requires multi-step MERGE or staged approach; high risk of logic error |

### Dimension 4: Procedural Depth

| Construct | Points | Rationale |
|-----------|--------|-----------|
| No procedural code | 0 | Pure SQL — direct translation |
| DECLARE variables only | 1 | Variable conversion to Snowflake Scripting |
| IF/ELSE branching | 2 | Control flow translation with END IF syntax changes |
| Nested WHILE/IF, cursors, dynamic SQL | 3 | Deep procedural code requiring careful Snowflake Scripting translation |

### Dimension 5: CASE Expression Depth

| CASE Complexity | Points | Rationale |
|----------------|--------|-----------|
| No CASE | 0 | No conditional expressions |
| Simple CASE (1-3 branches) | 0 | Direct translation |
| Nested CASE (CASE within CASE) | 1 | Moderate complexity — verify bracket alignment |
| CASE with 6+ branches or 2+ nesting levels | 2 | High complexity — consider refactoring to lookup table |
| CASE with derived calculations in branches | 3 | Each branch has its own formula — high verification effort |

### Dimension 6: Derived Measures

| Derived Measure Count | Points | Rationale |
|----------------------|--------|-----------|
| 0-1 calculated columns | 0 | Minimal derivation |
| 2-4 calculated columns | 1 | Moderate — verify each formula translates correctly |
| 5-8 calculated columns with window functions | 2 | High — window function syntax must be verified |
| 9+ derived columns or multi-step calculations | 3 | Very high — each measure is a separate translation risk |

### Dimension 7: Sybase-Specific Construct Count

Count the number of distinct Sybase-specific constructs requiring non-trivial translation:

| Construct Count | Points | Rationale |
|----------------|--------|-----------|
| 0-2 | 0 | Few conversions needed — mostly standard SQL |
| 3-5 | 1 | Moderate number of Sybase-specific rewrites |
| 6-9 | 2 | Many Sybase constructs — high translation surface area |
| 10+ | 3 | Pervasive Sybase-specific code — near-total rewrite |

**Constructs counted:** `GETDATE()`, `CONVERT()` with style, `ISNULL()`, `DATEPART()`, `DATENAME()`, `DATEDIFF()` (with non-standard parts), `IF EXISTS/DROP`, `DECLARE @var`, `WHILE`, `IDENTITY()`, `sysobjects`/`syscolumns`, `GO`, `PRINT`, `RAISERROR`, linked server `..` notation, `SET NOCOUNT ON`, `@@ROWCOUNT`, `@@ERROR`, `SELECT INTO #temp`, `EXEC`

---

## Tier Classification

| Total Score | Tier | Description | Review Approach |
|-------------|------|-------------|-----------------|
| 0-3 | **Simple** | Direct translation with minimal Sybase-specific rewrites. Standard DDL, simple DML, or views with basic joins. | Bulk approval — show names and count. Spot-check 1-2. |
| 4-6 | **Medium** | Moderate translation involving function mappings, SCD Type 1, or light procedural code. Multiple constructs require attention. | Individual review — show converted SQL with key translation decisions highlighted. |
| 7-21 | **Complex** | Significant rewrite required. SCD Type 2, deep procedural logic, many derived measures, or pervasive Sybase-specific constructs. | Side-by-side review — show Sybase source alongside Snowflake target with annotations for every non-trivial conversion. |

---

## Example Scoring

### Example: `stg_policies` (staging table from sql1.sql)

| Dimension | Assessment | Points |
|-----------|-----------|--------|
| Line Count | ~40 lines | 1 |
| Join Complexity | 0 joins (SELECT FROM source) | 0 |
| SCD Pattern | None | 0 |
| Procedural Depth | None | 0 |
| CASE Depth | No CASE | 0 |
| Derived Measures | 2 (LTRIM/RTRIM, CONVERT date) | 1 |
| Sybase Constructs | 4 (IF EXISTS/DROP, GO, GETDATE, CONVERT, linked server) | 1 |
| **Total** | | **3 → Simple** |

### Example: `dim_customer` (SCD2 dimension from sql2.sql)

| Dimension | Assessment | Points |
|-----------|-----------|--------|
| Line Count | ~120 lines | 2 |
| Join Complexity | 3 joins (stg + dim self-join + NOT EXISTS) | 1 |
| SCD Pattern | SCD Type 2 (expire + insert) | 3 |
| Procedural Depth | DECLARE variables | 1 |
| CASE Depth | Nested CASE (age_band, credit_band, region) | 2 |
| Derived Measures | 5 (age_band, credit_band, region, tenure, full_name) | 2 |
| Sybase Constructs | 7 (GETDATE, ISNULL, CONVERT, DATEPART, DATEDIFF, IF EXISTS, GO) | 2 |
| **Total** | | **13 → Complex** |

### Example: `agg_policy_monthly` (aggregation mart from sql5.sql)

| Dimension | Assessment | Points |
|-----------|-----------|--------|
| Line Count | ~50 lines | 1 |
| Join Complexity | 4 joins (fact + 3 dims) | 2 |
| SCD Pattern | None | 0 |
| Procedural Depth | None | 0 |
| CASE Depth | Simple CASE (1-2 branches) | 0 |
| Derived Measures | 6 (SUM/COUNT/AVG aggregates) | 2 |
| Sybase Constructs | 3 (IF EXISTS/DROP, GO, ISNULL) | 1 |
| **Total** | | **6 → Medium** |

---

## Scoring Guidelines

1. **Be conservative:** When a dimension falls on a boundary, round up. It is better to over-classify complexity than under-classify.
2. **Count distinct constructs:** If `GETDATE()` appears 10 times, it counts as 1 construct for Dimension 7. The count is of *distinct construct types*, not occurrences.
3. **SCD Type 2 always dominates:** Any object with SCD Type 2 logic scores at least Medium (3 points from SCD alone), and almost always Complex due to accompanying procedural code and derived measures.
4. **Procedural depth is cumulative:** An object with both `WHILE` loops and `IF/ELSE` branching scores 3 (the maximum), not 2+2.
5. **Window functions count as derived measures:** Each `ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()`, or analytic function in a SELECT list counts toward Dimension 6.

---

## Fidelity Threshold Adjustment by Tier

Complex objects are inherently harder to convert with perfect fidelity. The fidelity
threshold can be adjusted by tier when the user provides a `fidelity_threshold` at the
default level (0.85). If the user provides a custom threshold, use it for all tiers.

| Tier | Default Threshold | Rationale |
|------|------------------|-----------|
| **Simple** | 0.85 | Standard threshold — direct translations should score high |
| **Medium** | 0.85 | Standard threshold — moderate translations are still expected to pass |
| **Complex** | 0.70 | Reduced threshold — SCD2, deep procedural code, and many derived measures introduce inherent scoring noise |

**When to apply the reduced Complex threshold:**
- Only when using the default `fidelity_threshold` (0.85)
- Only for objects with `total_score >= 10` (deep Complex, not borderline)
- If the user explicitly sets a custom threshold, apply it uniformly across all tiers

---

## Quarantine Routing Rules

Quarantine routing is determined by the combination of complexity tier, fidelity score,
and retry count. These rules are applied in Step 7 (Fidelity Scoring).

### Decision Matrix

```
For each object after fidelity scoring:

1. Score >= threshold for its tier?
   → PASS: proceed to Step 8

2. Score < threshold AND retry_count < max_retries?
   → RETRY: generate structured diagnosis, return to Step 5

3. Score < threshold AND retry_count >= max_retries?
   → QUARANTINE: move to quarantine directory
```

### Retry Budget

The retry budget is shared across fidelity scoring (Step 7) and compilation
validation. Total retries per object must not exceed `max_retries` (default: 2).

| Retry Source | Counts Toward Budget |
|-------------|---------------------|
| Compilation failure auto-fix | Yes |
| Fidelity score below threshold | Yes |
| HITL-requested revision | No (human-directed corrections are unlimited) |

### Quarantine Severity Classification

Quarantined objects are further classified to guide manual remediation priority:

| Severity | Criteria | Recommended Action |
|----------|----------|-------------------|
| **High** | Score < 0.50 OR TODO blocks > 3 | Assign to senior developer; may need full manual rewrite |
| **Medium** | Score 0.50-0.69 AND TODO blocks ≤ 3 | Targeted fix of failing components; partial automation possible |
| **Low** | Score ≥ 0.70 (but below threshold) AND 0 TODO blocks | Minor adjustment likely — review diagnosis for quick fix |

### Pre-Quarantine Flagging

During Step 5 (Generate Snowflake SQL), objects can be pre-flagged for likely
quarantine based on complexity characteristics:

- Objects with `total_score >= 15` — flag as "high quarantine risk"
- Objects with 3+ distinct unsupported constructs — flag as "quarantine candidate"
- Objects referencing cursors or dynamic SQL — flag as "likely manual conversion"

Pre-flagged objects are still converted (best-effort), but the flag appears in
`conversion_log.json` and is surfaced during Step 6 (HITL review) to set expectations.
