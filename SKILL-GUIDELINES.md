# SKILL-GUIDELINES.md — How to Create Effective Skills

> This document is the authoritative guide for designing, writing, testing, maintaining,
> and retiring Skills. It is a Tier 1 companion to `CLAUDE.md` — read it before creating
> any new Skill.

---

## What Is a Skill?

A **Skill** is a reusable, modular instruction package that encodes how to do a specific
class of task well. It is injected into the agent's context on demand — only when the
current task matches its declared scope — and then removed. It is not a prompt. It is not
a chat instruction. It is **executable IP**.

Skills transform repeated one-off effort into systematic, version-controlled capability.
Each Skill represents distilled expertise: the best known approach to a specific problem,
with examples, error handling, and output standards built in.

**The Deloitte FDE framing:** A Skill is a deployable accelerator. When a Skill exists for
a task type, that task should take materially less time, produce higher-quality output, and
require less human correction than without it. If it doesn't, the Skill needs improvement.

---

## When to Create a Skill

Create a Skill when **all three** of the following are true:

1. **Recurrence** — You have performed this task type more than twice, or you confidently
   expect to perform it multiple times across engagements
2. **Specificity** — The task has a well-defined input format, a consistent transformation
   or workflow, and a predictable output structure
3. **Leverage** — A Skill would materially reduce time, error rate, or required human
   intervention compared to starting from scratch

**Strong Skill candidates:**
- ETL migration patterns (Informatica → Snowflake, Talend → Snowflake, Health Catalyst → Snowflake)
- Repeatable document generation (effort estimation model, proposal section, client report)
- Code generation with a fixed architectural pattern (Cortex Code pipeline, Streamlit agent template)
- Data quality check patterns (schema validation, null audit, referential integrity)
- Structured analysis tasks (dependency mapping, complexity classification, cost estimation)

**Poor Skill candidates — do NOT create a Skill for:**
- One-off tasks you will never repeat
- Tasks with no consistent input or output format
- Tasks that require so much judgment they cannot be parameterized
- General knowledge questions answerable from training data
- Tasks already handled well by existing built-in tools

---

## The Three-Tier Skill Hierarchy

Skills exist in three layers. Know which tier your Skill belongs to before creating it.

```
TIER 1 — BUILT-IN
  Platform-provided. Not authored by users.
  Examples: code execution, file creation, web search
  Location: skills/built-in/

TIER 2 — PROJECT
  Encodes patterns specific to one client or codebase.
  Retired when the engagement ends.
  Examples: tch-migration-validator, disney-cortex-demo-builder
  Location: skills/project/<engagement-name>/

TIER 3 — USER
  Personal reusable IP. Portable across engagements.
  Version-controlled and maintained indefinitely.
  Examples: etl-complexity-classifier, effort-estimator, sql-reviewer
  Location: skills/user/<skill-name>/
```

**Tier determination rules:**
- If the Skill references client-specific schemas, systems, or names → Tier 2 (Project)
- If the Skill encodes a general method reusable across clients → Tier 3 (User)
- When in doubt, build Tier 3 with parameters for client-specific values

---

## Skill Anatomy

Every Skill follows this mandatory folder structure:

```
skills/user/<skill-name>/
├── SKILL.md              ← Required. Instructions + YAML frontmatter
├── examples/             ← Required. At least one real input/output pair
│   ├── example-01-descriptive-name/
│   │   ├── README.md     ← What this example demonstrates, notable decisions
│   │   ├── input/        ← Real input file(s) or input.md
│   │   └── output/       ← Expected output file(s) or output.md
│   └── example-02-edge-case/
├── scripts/              ← Optional. Executable code for deterministic steps
│   └── *.py / *.sql / *.sh
├── references/           ← Optional. Domain reference material
│   └── *.md              ← Datatype maps, API specs, rule tables
├── evals/                ← Required before v1.0. Test cases + assertions
│   └── evals.json
└── CHANGELOG.md          ← Required. Version history
```

**Naming rules:**
- Folder name: `kebab-case`, descriptive, specific (`talend-job-parser` not `etl-tool`)
- No version numbers in folder names — use `CHANGELOG.md` and git tags
- `SKILL.md` is always uppercase — convention, not preference

---

## SKILL.md Structure

`SKILL.md` has two parts: a **YAML frontmatter block** and a **Markdown body**. Both required.

### Part 1 — YAML Frontmatter

```yaml
---
name: <skill-name>               # Must match folder name exactly
version: 0.1.0                   # Semantic version — bump on every meaningful change
tier: user | project             # Which tier this skill belongs to
author: <your-name>              # Who owns this skill
created: YYYY-MM-DD
last_updated: YYYY-MM-DD
status: draft | active | deprecated

description: >
  <Trigger description — see full guidance in the next section>

compatibility:
  tools: [bash_tool, create_file, present_files]   # Tools this skill requires
  context: [CLAUDE.md, PROJECT.md]                 # Files to load alongside
---
```

### Part 2 — Markdown Body

The body must contain these sections in this order:

```markdown
# <Skill Title>

<One-paragraph description: what this skill does, why it exists, what value it delivers>

## Inputs
## Pre-conditions
## Workflow
## Output Specification
## Error Handling
## HITL Checkpoints
## Reference Tables
```

---

## Writing the Description (Trigger Text) — The Most Critical Field

The `description` field is the **primary mechanism** by which the agent decides whether
to load this Skill. A poor description means the Skill never triggers, or triggers on the
wrong tasks. This is the field most Skill authors get wrong.

### The description must answer three questions:
1. **What does this Skill produce?** — the output
2. **When should it trigger?** — exact user phrases, file types, task patterns, keywords
3. **What should NOT trigger it?** — disambiguation from similar Skills

### Description writing rules:

**List real trigger phrases explicitly:**
```yaml
# WEAK — too abstract
description: "Converts ETL mappings to Snowflake SQL."

# STRONG — real phrases, file types, keywords, disambiguation
description: >
  Converts Talend Job XML exports to Snowflake SQL stored procedures and a migration
  complexity report. Use this skill when the user uploads a .job or .item file, mentions
  "Talend job", "Talend migration", "ETL to Snowflake", or asks to analyze, convert,
  or assess Talend workflows. Also trigger when the user mentions Talend components:
  tMap, tJoin, tAggregateRow, tFileInput, or tDBInput.
  Do NOT use for Informatica PowerCenter — use informatica-pc-parser instead.
```

**Bias toward triggering** — Claude tends to undertrigger Skills. Be assertive:
```yaml
# TOO PASSIVE
description: "May help with SQL review tasks."

# APPROPRIATELY ASSERTIVE
description: >
  Reviews Snowflake SQL for performance, correctness, and style. Use this skill for
  ANY SQL review, optimization, or audit task — even if the user just says "can you
  check this query" or "does this look right". Always prefer this skill over ad-hoc
  SQL review.
```

**Disambiguate explicitly** — if two Skills share a domain, each description must name
the other and say when NOT to use it.

**Include file extension triggers:**
```yaml
description: >
  ... Trigger when the user uploads .xml, .dtsx, or .json files that represent ETL
  workflow exports, or pastes ETL mapping definitions into the chat.
```

### Description length: 50–150 words.
Under 50 is too vague. Over 150 dilutes the signal.

---

## Writing the Workflow — The Core of the Skill

The workflow is the numbered sequence of steps the agent executes. It determines whether
output is consistent or varies every time.

### Workflow writing rules:

**Use imperative voice throughout:**
```markdown
# WRONG — passive/descriptive
The XML file should be located and read before proceeding.

# RIGHT — imperative
Locate the uploaded XML file in `/mnt/user-data/uploads/`. If multiple files are
present, ask the user which one to use before proceeding.
```

**Anchor every step to a concrete action** — steps produce artifacts, state changes, or decisions:
```markdown
# VAGUE
### Step 2: Process the input

# CONCRETE
### Step 2: Parse the input file and build an intermediate representation
Run `scripts/parse_input.py` with the input path. Read the resulting JSON at
`/tmp/parsed_ir.json`. Verify it contains at least one mapping — if not, report
the error and stop.
```

**Specify every branch** — happy path only is incomplete:
```markdown
### Step 3: Validate the schema
- If validation passes: proceed to Step 4
- If validation fails with recoverable errors: log each warning, proceed with valid
  records, note all skipped records in the output report
- If validation fails with a fatal error: STOP, report the specific failure, ask
  how to proceed
```

**Make HITL checkpoints explicit and visible:**
```markdown
### Step 4: ⚠️ HITL CHECKPOINT — 🟡 MEDIUM — Confirm before writing output files

Present this summary to the user:
- Number of objects to be written
- File names and locations
- Any warnings or anomalies found

Wait for explicit approval ("yes", "proceed", "approved") before executing Step 5.
Do NOT proceed on silence or ambiguous responses.
```

**Reference scripts by exact path:**
```bash
python3 /mnt/skills/user/talend-parser/scripts/parse_talend.py \
  --input "<input-path>" \
  --output /tmp/parsed.json \
  --schema "<target-schema>"
```

**Target 6–12 steps.** Under 4 = task too simple for a Skill. Over 15 = Skill too broad, split it.

---

## The Examples Requirement — Non-Negotiable

**Every Skill must have at least one real worked example before status = active.**

One real example is worth more than ten paragraphs of instructions. The agent uses
examples to calibrate behavior when instructions are ambiguous.

### What counts as a real example:
- An actual input from a real task (anonymized as needed)
- The actual output the Skill produced or should have produced
- Annotated to explain non-obvious decisions

### What does NOT count:
- A hypothetical or invented input/output pair
- A description of what an example would look like
- A simplified toy case that doesn't reflect real-world complexity

### Example README.md template:
```markdown
# Example 01 — <Descriptive Name>

## What this demonstrates
<The specific scenario, input characteristics, transformation type>

## Input characteristics
- <Key feature 1 of the input>
- <Key feature 2>

## Notable decisions in the output
- <Why a specific choice was made in the output — especially non-obvious ones>
- <Any limitations of this example>

## Known gaps
- <What this example does NOT cover — point to another example that does>
```

---

## Progressive Disclosure Architecture

Skills use a three-level loading system. Design your Skill to respect all three levels.

```
LEVEL 1 — Metadata (~100 words, always in context)
  name + description from YAML frontmatter
  Agent reads this to decide whether to load the Skill.
  Must be self-contained enough to trigger correctly.

LEVEL 2 — SKILL.md body (< 500 lines, loaded when Skill triggers)
  Full workflow, output spec, error handling, HITL checkpoints.
  Agent executes from this. Must be complete without reading anything else.

LEVEL 3 — Bundled resources (unlimited, loaded on demand)
  scripts/   — executed without loading into context
  references/ — read only when the workflow instructs
  examples/  — consulted when calibrating output format
```

### Hard limits:
- `SKILL.md` body: **500 lines maximum** (soft limit 300)
- If approaching the limit: move content to `references/`, split by domain variant
- `references/` files over 300 lines: add a table of contents at the top
- Scripts are executed, not read — keep them out of SKILL.md body

### Splitting a Skill that's too large:
```
# WRONG — one giant skill
skills/user/etl-migrator/
  SKILL.md  (800 lines — too long)

# RIGHT — orchestrator + variants
skills/user/etl-migrator/
  SKILL.md               (orchestrator: reads input, picks the right variant)
  references/
    talend.md            (Talend-specific instructions)
    informatica.md       (Informatica-specific instructions)
    health-catalyst.md   (Health Catalyst-specific instructions)
```

---

## HITL Checkpoints in Skills

Every Skill must explicitly define where human review is required. The same 🟢/🟡/🔴/⛔
risk classification from `CLAUDE.md` applies inside Skills.

### Mandatory HITL checkpoint situations:

| Situation | Risk Level | Required Action |
|-----------|-----------|-----------------|
| Before writing any file to `outputs/` | 🟡 MEDIUM | Summarize what will be written, wait for "proceed" |
| Before executing SQL against a real DB | 🔴 HIGH | Show dry-run output, require explicit approval |
| Before schema migration or DDL | 🔴 HIGH | Show full DDL + rollback plan, require sign-off |
| Before bulk data modification | ⛔ CRITICAL | Full STOP — impact analysis + written confirmation |
| Unexpected anomalies in validation | 🟡 MEDIUM | Surface findings, ask how to proceed |
| Input format doesn't match expected | 🟡 MEDIUM | Describe mismatch, propose path forward, wait |

### HITL checkpoint format in SKILL.md:
```markdown
### ⚠️ HITL CHECKPOINT: [RISK LEVEL] — [What requires approval]

Present the following before proceeding:
- [Specific information to surface]
- [Expected impact or risk]
- [Rollback option if applicable]

Required response: Explicit approval — "yes", "proceed", or "approved [action]"
On silence: Wait. Do not proceed.
On rejection: Ask what to do differently. Do not improvise.
```

---

## Output Specification

Every Skill must define its outputs precisely. Ambiguous outputs are the primary cause
of Skills that "work but produce the wrong thing."

```markdown
## Output Specification

### Files produced
| File | Location | Format | Description |
|------|----------|--------|-------------|
| `output_dml.sql` | `/mnt/user-data/outputs/` | SQL | Snowflake MERGE statements |
| `output_report.md` | `/mnt/user-data/outputs/` | Markdown | Migration summary |

### Delivery
Always call `present_files` with all output files.
Inline summary must include: [row counts / object counts / warnings / items needing review]

### Quality bar
- [Specific validation the output must pass before being delivered]
- [What constitutes an incomplete or invalid output]
- Never silently omit objects that failed — always report them
```

---

## Error Handling

Skills must never fail silently. Every anticipated failure mode has an explicit response.

```markdown
## Error Handling

### File not found
List what IS in `/mnt/user-data/uploads/`. Ask user to confirm or upload the file.

### Parse failure (non-zero exit code)
Report exact error from stderr. Do not continue with partial results. Suggest fix.

### Partial success
Complete all valid objects. Report each failed object with its identifier and reason.
Never suppress partial failures from the output report.

### Unexpected format / schema mismatch
Describe what was expected vs. what was found. Issue HITL checkpoint before proceeding
with any assumptions. Do not guess.
```

---

## Versioning and Lifecycle

### Semantic versioning for Skills:

| Bump | When |
|------|------|
| `PATCH` (0.0.x) | Bug fix, typo, output format clarification |
| `MINOR` (0.x.0) | New capability, new example, new error case handled |
| `MAJOR` (x.0.0) | Workflow restructured, input/output format changed, trigger description changed |

### Skill status lifecycle:
```
DRAFT → ACTIVE → DEPRECATED → RETIRED

DRAFT:       Under development. Not safe to use in production tasks.
ACTIVE:      Validated with at least one real example. Safe to use.
DEPRECATED:  Superseded. Still usable but should not be chosen for new work.
RETIRED:     Removed from active library. Archive only.
```

**Never delete a Skill — deprecate it.** Set `status: deprecated` and add a note pointing
to the replacement.

### CHANGELOG.md format:
```markdown
# SKILL CHANGELOG — <skill-name>

## [1.2.0] - 2025-06-15
### Added
- Joiner transformation support
- Example 03: complex multi-source join
### Fixed
- Lookup with custom SQL override no longer silently skipped

## [1.0.0] - 2025-05-01
### Initial release
```

---

## Quality Bar — Minimum Criteria for ACTIVE Status

A Skill must meet ALL of the following before `status: active`:

### Mandatory:
- [ ] YAML frontmatter complete (name, version, tier, author, status, description)
- [ ] Description 50–150 words with explicit trigger phrases and disambiguation
- [ ] At least one real (not hypothetical) worked example in `examples/`
- [ ] All workflow steps use imperative voice and reference concrete actions
- [ ] Every anticipated failure mode has an explicit error handling response
- [ ] All HITL checkpoints explicitly marked with risk level
- [ ] Output specification defines every file, format, and delivery method
- [ ] `evals/evals.json` exists with at least 3 test prompts
- [ ] `CHANGELOG.md` exists with initial version entry
- [ ] Skill has been run at least once against a real input and output reviewed

### Required for v1.0.0:
- [ ] At least 2 examples covering different complexity levels
- [ ] Evals include assertions (not just prompts)
- [ ] External scripts use absolute paths
- [ ] Skill produces consistent output on repeated runs with same input
- [ ] Description has been validated for trigger accuracy

---

## Testing a Skill

```
Step 1 — Smoke test (before ACTIVE)
  Run against Example 01. Output must match expected output in all material respects.

Step 2 — Regression test (before every MINOR or MAJOR bump)
  Run against all examples. No example should produce materially worse output.

Step 3 — Edge case test (before v1.0.0)
  At least one test case exercises a known failure mode. Verify error handling matches
  the documented response.

Step 4 — Trigger test (before v1.0.0)
  Write 5 prompts that SHOULD trigger the Skill and 3 that should NOT. Verify correctly.
  If it undertriggers, revise the description.
```

### evals.json structure:
```json
{
  "skill_name": "talend-job-parser",
  "version": "1.1.0",
  "evals": [
    {
      "id": 1,
      "name": "simple-single-source",
      "prompt": "Please convert the attached Talend job to Snowflake SQL",
      "input_files": ["examples/example-01/input/sample_job.xml"],
      "expected_output": "SQL with MERGE statement targeting DIM_CUSTOMER",
      "assertions": [
        "Output contains a MERGE INTO statement",
        "Output references the correct target table name",
        "Report includes a complexity classification"
      ]
    }
  ]
}
```

---

## Skill Improvement Loop

When a Skill produces incorrect or suboptimal output, fix the Skill — not just the output:

```
Task fails or produces suboptimal output
    ↓
Identify: instruction gap OR example gap?
    ↓
Instruction gap → revise the relevant workflow step
Example gap    → add a new example covering this case
    ↓
Bump PATCH or MINOR version
    ↓
Update CHANGELOG.md
    ↓
Re-run affected test cases to verify fix
    ↓
Commit: fix(<skill-name>): <description of what was wrong>
    ↓
Add the insight to tasks/lessons.md for session-level learning
```

---

## Parameterizing for Client Reuse

Tier 3 Skills must accept client-specific values as parameters, not hardcode them:

```markdown
## Inputs
- `--schema`: Target Snowflake schema name (default: `PUBLIC`)
- `--database`: Target Snowflake database (default: from `PROJECT.md`)
- `--client-prefix`: Table prefix convention for this client (default: none)
```

### Promoting a Project Skill to User tier:
1. Remove all client-specific hardcoding — replace with parameters
2. Add a second example from a different client context (anonymized)
3. Move from `skills/project/` to `skills/user/`
4. Update `tier: project` → `tier: user` in frontmatter
5. Log in CHANGELOG.md: `## Promoted from project to user tier`

---

## Anti-Patterns

| Anti-Pattern | Why It Fails | Fix |
|-------------|-------------|-----|
| Description too vague ("helps with ETL") | Skill never triggers | Add explicit trigger phrases and keywords |
| No real example | Inconsistent output — agent has no calibration | Add a real worked example immediately |
| Happy path only | First real failure causes agent to improvise | Add error handling for every failure mode |
| Passive voice in steps ("should be read") | Agent may skip or reorder steps | Imperative voice throughout |
| SKILL.md over 500 lines | Context bloat, degraded precision | Split into main + references/ |
| No HITL checkpoints | Agent writes files without confirmation | Add explicit checkpoints before every write |
| Hardcoded client names/schemas | Skill not reusable | Parameterize all client-specific values |
| No CHANGELOG | Can't tell what changed between versions | Add CHANGELOG.md from day one |
| Draft status in production | Quality not validated | Complete quality bar checklist first |
| Fixing output but not Skill | Same mistake recurs | Always update the Skill when fixing a recurring error |

---

## Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Skill folder | `kebab-case`, specific | `talend-job-parser` |
| SKILL.md frontmatter `name` | Same as folder | `talend-job-parser` |
| Script files | `snake_case.py` | `parse_talend_xml.py` |
| Reference files | `kebab-case.md` | `transformation-type-map.md` |
| Example folders | `example-NN-descriptive-name` | `example-01-simple-mapping` |
| Git tags | `skill/<name>/v<semver>` | `skill/talend-job-parser/v1.2.0` |

---

## Complete SKILL.md Template

```markdown
---
name: <skill-name>
version: 0.1.0
tier: user
author: <your-name>
created: YYYY-MM-DD
last_updated: YYYY-MM-DD
status: draft

description: >
  <What this skill produces — 1 sentence.>
  Use this skill when the user <trigger phrase 1>, <trigger phrase 2>,
  or mentions <keyword 1>, <keyword 2>, <keyword 3>.
  Also trigger when the user uploads <file type> or references <system name>.
  Do NOT use for <disambiguating case> — use <other-skill-name> instead.

compatibility:
  tools: [bash_tool, create_file, present_files]
  context: [CLAUDE.md, PROJECT.md]
---

# <Skill Title>

<One paragraph: what this Skill does, why it exists, what problem it solves,
what value it delivers. Be specific about domain and output.>

## Inputs
- **Required:** `<input-name>` — <format, where to find it>
- **Optional:** `<param-name>` — <description, default value>

## Pre-conditions
- <Tool/system that must be available>
- <Access or credential that must exist>

## Workflow

### Step 1: <Action verb — what happens>
<Imperative instructions. What to do, how, what success looks like.>

### Step 2: <Action verb — what happens>
<Reference scripts by exact absolute path.>

```bash
python3 /mnt/skills/user/<skill-name>/scripts/<script>.py \
  --input "<input-path>" \
  --output /tmp/output.json
```

### Step 3: ⚠️ HITL CHECKPOINT — 🟡 MEDIUM — Confirm before writing output

Present to the user:
- <What will be written>
- <Any warnings or anomalies>

Required response: Explicit approval. Do not proceed on silence.

### Step 4: <Continue...>

## Output Specification

| File | Location | Format | Description |
|------|----------|--------|-------------|
| `<filename>` | `/mnt/user-data/outputs/` | <format> | <description> |

Always call `present_files` with all output files.
Inline summary must include: <objects processed, warnings, items needing review>.

## Error Handling

### <Error scenario 1 — e.g., File not found>
<What to do. Never fail silently.>

### <Error scenario 2 — e.g., Parse failure>
<What to do.>

### <Error scenario 3 — e.g., Partial success>
<What to do. Always report failures in output.>

## HITL Checkpoints Summary
| Step | Risk Level | What Requires Approval |
|------|-----------|----------------------|
| Step 3 | 🟡 MEDIUM | Writing output files |

## Reference Tables
<Lookup tables, type maps, rule tables needed during execution>

| Source Type | Target Type | Notes |
|-------------|-------------|-------|
| <value> | <value> | <note> |
```

---

## Relationship to CLAUDE.md

| Concern | Governed By |
|---------|------------|
| When to load a Skill | `CLAUDE.md` — Progressive Context Architecture |
| How to build a Skill | This document |
| Skill HITL risk classification | `CLAUDE.md` — HITL Framework (same levels apply) |
| Skill versioning | This document + `CLAUDE.md` Checkpointing section |
| Skill quality bar | This document |
| Skill folder hierarchy | Both — must be consistent |

---

*SKILL-GUIDELINES.md is itself a Tier 1 document. Version it, maintain it, and update it
when these guidelines improve. Log every change in its own CHANGELOG.*
