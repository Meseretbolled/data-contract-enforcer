# DOMAIN_NOTES.md — Data Contract Enforcer
**Author:** Meseret Bolled  
**Date:** 2026-04-02  
**Systems Referenced:** Weeks 1–5 canonical schemas + Week 7 implementation

---

## Question 1: Backward-Compatible vs Breaking Schema Changes

A **backward-compatible** change is one where existing downstream consumers continue to work correctly after the change — they do not need to update their code. A **breaking change** is one where existing consumers will fail, produce wrong results, or need to be updated before the change can be deployed safely.

### Three Backward-Compatible Examples (from our own schemas)

**Example 1 — Adding an optional field to `extraction_record` (Week 3)**  
Adding a new optional field `language_detected: "en"` to the top level of `extractions.jsonl` is safe. The Week 4 Cartographer reads `doc_id`, `extracted_facts`, and `entities`. It ignores fields it does not know about. The new field is simply skipped. No downstream code breaks.

In our SchemaEvolutionAnalyzer taxonomy, this classifies as `optional_field_added` → COMPATIBLE. The Confluent Schema Registry BACKWARD mode allows this. We saw exactly this pattern in our Week 5 schema evolution run — 21 optional payload fields were added across two snapshots, all correctly classified as COMPATIBLE.

**Example 2 — Adding a new enum value to `entity.type` (Week 3)**  
The current enum is `PERSON | ORG | LOCATION | DATE | AMOUNT | OTHER`. Adding a new value `PRODUCT` is additive. Consumers that switch on `entity.type` will hit their `default` or `OTHER` branch. No crash, no wrong calculation — they just treat `PRODUCT` as an unknown type until they update.

In our taxonomy: `enum_value_added` → COMPATIBLE. The SchemaEvolutionAnalyzer handles this case: it computes `old_enum - new_enum` (removals = BREAKING) and `new_enum - old_enum` (additions = COMPATIBLE).

**Example 3 — Widening `processing_time_ms` from `int32` to `int64` (Week 3)**  
The current field `processing_time_ms: 1431` fits in a 32-bit integer. Widening to 64-bit means larger values are now accepted. Existing consumers reading this field as a number still work correctly — all old values still fit in the wider type.

In our taxonomy: `range_widened` → COMPATIBLE. The SchemaEvolutionAnalyzer detected `processing_time_ms: no_change` between our two Week 3 snapshots because the range did not shift. A widening would classify as COMPATIBLE.

### Three Breaking Examples (from our own schemas)

**Example 1 — Renaming `doc_id` to `document_id` (Week 3)**  
The Week 4 Cartographer looks up `record["doc_id"]` to create a node in the lineage graph. If Week 3 renames this field to `document_id`, the Cartographer raises a `KeyError` or silently gets `None`, producing a node with no identity. Every downstream lineage query fails.

In our registry, `doc_id` is listed as a `breaking_field` in both the `week4-cartographer` and `week7-contract-enforcer` subscriptions. A rename would trigger a blast radius notification to both subscribers immediately.

**Example 2 — Changing `confidence` from float `0.0–1.0` to integer `0–100` (Week 3)**  
The contract clause `minimum: 0.0, maximum: 1.0` is violated immediately. Any consumer using confidence for threshold filtering — e.g. `if fact["confidence"] > 0.8: accept_fact()` — would now accept ALL facts because `87 > 0.8` is always true. Silent data corruption.

This is the exact violation we demonstrated in Week 7. The ValidationRunner caught it with two independent checks: `range` (CRITICAL) and `statistical_drift` (HIGH, z-score ≈ 450). See Question 2 for the full trace.

**Example 3 — Removing `sequence_number` from `event_record` (Week 5)**  
The Week 5 event store uses `sequence_number` to enforce monotonic ordering and detect duplicate events. If this field is removed, the deduplication logic raises a `KeyError` and the entire event pipeline halts.

In our SchemaEvolutionAnalyzer run on Week 5, we detected `payload_doc_id: field_removed` as a BREAKING change — classified as `field_removed` with action: "Two-sprint deprecation minimum. Each registry subscriber must acknowledge removal." This was a real detection from our actual schema snapshots.

---

## Question 2: The Confidence Scale Change — Full Trace

### Starting State

In `outputs/week3/extractions.jsonl` (50 records, 150 facts), the `confidence` field is a float between 0.0 and 1.0. Running the ValidationRunner on clean data produces:

```
extracted_fact_confidence: min=0.680, max=0.990, mean=0.763, stddev=0.137
```

This baseline is stored in `schema_snapshots/baselines.json` after the first clean run. All 150 facts pass the range constraint `minimum: 0.0, maximum: 1.0`.

### The Breaking Change

The `create_violation.py` script simulates a developer changing the confidence scale:

```python
# Simulates: developer changes extractor to output percentage scale
for fact in record.get("extracted_facts", []):
    fact["confidence"] = round(fact["confidence"] * 100, 1)
    # 0.87 → 87.0
```

The developer does not update the contract. They do not notify Week 4.

### How the Failure Propagates to Week 4

The Week 4 Cartographer reads `extracted_facts[].confidence` to score lineage node reliability:

```python
high_confidence_facts = [f for f in record["extracted_facts"] if f["confidence"] > 0.8]
```

After the change, `confidence = 87.0`. The check `87.0 > 0.8` is always `True`. Every single fact is classified as high-confidence — including facts originally scored at 0.52 (now 52.0). The Cartographer marks every document as maximally reliable. Downstream quality filtering is completely broken.

No exception is raised. The pipeline runs to completion. The corruption is invisible.

### The Two Contract Clauses That Catch This

**Clause 1 — Range check (structural):**
```yaml
# In generated_contracts/week3-document-refinery-extractions.yaml
extracted_fact_confidence:
  type: number
  minimum: 0.0
  maximum: 1.0
  required: true
  description: >
    BREAKING CHANGE if converted to 0-100 percentage scale.
    This is the key clause — catches the scale change before it propagates.
```

**Clause 2 — Statistical drift check (independent of the contract):**

The ValidationRunner computes the z-score against the stored baseline:
```
z_score = |current_mean - baseline_mean| / baseline_stddev
z_score = |76.3 - 0.763| / 0.137
z_score ≈ 450
```

A z-score of 450 triggers `FAIL` (threshold: > 3). This check **does not read the contract** — it fires even if someone edits the contract to say `maximum: 100`. It is the check that cannot be defeated.

### Actual Run Results (from validation_reports/week3_violated.json)

```
❌  extracted_fact_confidence.range: FAIL
    CRITICAL — max=99.0 > contract maximum 1.0 — 150 records failing

❌  extracted_fact_confidence.statistical_drift: FAIL  
    HIGH — z_score≈450 stddev from baseline (mean=76.3 vs baseline=0.763)

📊  31 passed  2 failed  0 warned  0 errored
```

The ViolationAttributor then traced these to:
- Registry blast radius: `week4-cartographer` [ENFORCE], `week7-contract-enforcer` [AUDIT]
- Lineage transitive nodes: `file::src/week4/cartographer.py`, `file::src/week7/contract_enforcer.py`
- Git blame: commit `39e4fe9d6654` by `meseretbolled@gmail.com` (confidence_score: 1.0)

---

## Question 3: How the Enforcer Uses the Lineage Graph to Produce a Blame Chain

When a contract violation is detected, the ViolationAttributor follows a four-step pipeline. The key architectural principle: **the registry is the primary blast radius source; the lineage graph is enrichment only.**

### Step 1 — Registry Blast Radius Query (PRIMARY)

Load `contract_registry/subscriptions.yaml`. Find all subscriptions where `contract_id` matches the failing contract and `breaking_fields` contains a keyword matching the failing field:

```python
keywords_failing = set(failing_field.replace(".","_").split("_"))
keywords_field   = set(field.replace(".","_").split("_"))
overlap = keywords_failing & keywords_field - {"", "id"}
if len(overlap) >= 1:
    affected.append(subscriber)
```

For `extracted_fact_confidence.range`, the keyword `confidence` matches both `week4-cartographer` and `week7-contract-enforcer`. This is the **definitive subscriber list**.

Why registry first? At Tier 2 (multi-team), you cannot traverse external teams' lineage graphs — they are proprietary. The registry is the correct Tier 1→2→3 abstraction. At Tier 3 (cross-company), you only have the subscriber count — lineage traversal is impossible.

### Step 2 — Lineage Transitive Depth (ENRICHMENT)

Load the most recent snapshot from `outputs/week4/lineage_snapshots.jsonl`. Starting from producer nodes, do BFS traversal following `PRODUCES/WRITES/CONSUMES/READS` edges:

```
file::src/week3/extractor.py ──WRITES──► table::extractions  (source)
table::extractions ──READS──► file::src/week4/cartographer.py  (depth 1)
table::extractions ──READS──► file::src/week7/contract_enforcer.py  (depth 1)
file::src/week4/cartographer.py ──WRITES──► table::lineage_snapshots  (depth 2)
```

This produces:
- Direct consumers (depth 1): `[table::extractions]`
- Transitive consumers: `[file::src/week4/cartographer.py, file::src/week7/contract_enforcer.py, table::lineage_snapshots]`
- Max contamination depth: 3

The registry tells you *who is affected*; the lineage tells you *how deeply*. They are complementary, not interchangeable.

### Step 3 — Git Blame for Cause Attribution

For each upstream producer file, run:
```bash
git log --follow --since=60 days ago --format='%H|%ae|%ai|%s' -- src/week3/extractor.py
```

Score each commit candidate:
```
confidence = 1.0 - (days_since_commit × 0.1) - (lineage_distance × 0.2)
```

A commit made today on the direct producer file: `1.0 - 0.0 - 0.2 = 0.8`  
A commit made 5 days ago two hops away: `1.0 - 0.5 - 0.4 = 0.1`

Return top 5 candidates sorted by confidence descending.

### Step 4 — Write Violation Log

Write to `violation_log/violations.jsonl` with full blast radius (registry-sourced + lineage-enriched) and ranked blame chain. This is the record consumed by the ReportGenerator and, in Week 8, by the Sentinel alert pipeline.

---

## Question 4: Data Contract for LangSmith `trace_record`

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Records — Apex Ledger
  version: 1.0.0
  owner: ai-engineering-team
  description: >
    One record per LangSmith run. 210 real traces from apex-ledger
    loan application processing agents. Used to monitor AI system
    performance, latency, and output schema conformance.

servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl

schema:
  # ── Structural Clause ──────────────────────────
  id:
    type: string
    format: uuid
    required: true
    unique: true

  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
    description: >
      BREAKING if existing values removed.
      All 210 apex-ledger traces are run_type=chain.

  start_time:
    type: string
    format: date-time
    required: true

  end_time:
    type: string
    format: date-time
    required: true
    description: MUST be >= start_time. Cross-field constraint.

  total_tokens:
    type: integer
    required: true
    minimum: 0

  # ── Statistical Clause ─────────────────────────
  name:
    type: string
    required: true
    description: >
      Agent function name. Statistical: cardinality=7 in our data.
      Rising cardinality signals new agents being added.

  # ── AI-Specific Clause ─────────────────────────
  outputs:
    type: object
    required: false
    description: >
      AI-SPECIFIC: Structured output from agent node. For credit-agent
      runs, must contain [applicant_id, application_id, credit_decision].
      Track output_schema_violation_rate per agent version.

  error:
    type: string
    required: false
    description: >
      AI-SPECIFIC: 87.4% null in our data (most runs succeed).
      Rising non-null rate signals API key issues or model problems.

quality:
  type: SodaChecks
  specification:
    checks for langsmith-trace-records:
      - missing_count(id) = 0
      - duplicate_count(id) = 0
      - row_count >= 50

lineage:
  upstream:
    - id: apex-ledger-langgraph-agents
      description: Loan processing agents generate these traces
  downstream:
    - id: week7-ai-contract-extensions
      description: AI extensions validate trace schema conformance
      fields_consumed: [id, name, run_type, start_time, end_time]
      breaking_if_changed: [run_type, id]
```

Our actual generated contract (`generated_contracts/langsmith-traces.yaml`) has 28 clauses auto-generated from 210 real traces. The AI extensions check validated all 210 traces with 0 schema violations (0.0% violation rate, stable trend).

---

## Question 5: Most Common Failure Mode — Why Contracts Go Stale

**The most common failure mode is not missing tooling — it is missing process.** Contracts go stale because no step in the deployment pipeline requires updating them when schemas change.

### Why Contracts Go Stale — Three Root Causes

**Root cause 1 — No CI gate.**  
If running the SchemaEvolutionAnalyzer is optional, it never runs under deadline pressure. A contract that lives in a YAML file but is never run in CI is documentation, not enforcement. The fix is making the SchemaEvolutionAnalyzer a required step in every PR that touches a data output directory.

**Root cause 2 — Contracts are owned by consumers, not producers.**  
The producer changes the schema without knowing the contract exists. The consumer discovers the violation in production. This is exactly what the contract registry solves: producers know their subscribers because subscribers are registered. The blast radius is pre-computed at contract generation time, not discovered at incident time.

**Root cause 3 — Statistical thresholds are never recalibrated.**  
A contract written against January data becomes stale by April if the data distribution legitimately shifts. Teams see false positives, start ignoring alerts, and disable checks. The contract exists but provides no value — this is "contract fatigue."

### How Our Architecture Prevents Staleness

**Prevention 1 — Snapshot-based evolution detection.**  
The ContractGenerator writes a timestamped snapshot on every run. The SchemaEvolutionAnalyzer diffs consecutive snapshots automatically. You cannot change the schema without the diff being detected and classified. No manual contract update required.

In our actual run: Week 5 events produced 23 detected changes across two snapshots — 2 BREAKING (correctly identified), 21 COMPATIBLE (correctly identified). Zero human review required.

**Prevention 2 — Statistical baselines independent of the contract.**  
After the first clean run, `schema_snapshots/baselines.json` stores mean and stddev for every numeric column. Future runs compute z-scores. The confidence scale change from `0.0–1.0` to `0–100` produces z-score ≈ 450 — caught regardless of whether the contract was updated.

This is the architecture's most important property: **the statistical check cannot be defeated by editing the contract.** Even if a developer updates the contract to say `maximum: 100`, the z-score check fires because it reads the baseline, not the contract.

**Prevention 3 — Registry creates social accountability.**  
Every violation produces a blast radius report naming specific downstream systems. This makes the cost of a contract violation immediately visible to every affected team — creating social pressure to fix violations quickly and keep contracts accurate.

**Prevention 4 — Embedding drift catches semantic staleness.**  
Standard contracts catch structural and statistical drift. They cannot catch semantic drift — when the meaning of text fields shifts even though their format stays valid. The AI Contract Extensions embedding drift check (OpenRouter `text-embedding-3-small`) stores a centroid baseline of extracted fact text. Cosine distance > 0.15 triggers WARN. This catches domain shifts that no schema check would detect.

### The Residual Risk

Statistical baselines can be invalidated by legitimate data changes. If the Week 3 refinery starts processing a new document corpus, the confidence distribution may legitimately shift. The mitigation: require `rm schema_snapshots/baselines.json` to be a deliberate, documented action — not automatic. Our README explicitly lists this as a required manual step with rationale.

### What a Production System Does Differently

The production solution is a CI gate: SchemaEvolutionAnalyzer runs on every PR that touches any data output file. If a breaking change is detected without a corresponding registry migration plan, the PR fails. This transforms contracts from passive documentation into active enforcement.

The `contract_registry/subscriptions.yaml` in this project is the minimum viable registry. At Tier 2 (multi-team), it becomes DataHub or OpenMetadata with workflow-based approval. The ValidationRunner and contract YAML are identical across all three tiers — only the blast radius computation mechanism changes.

---

## Implementation Notes — What We Actually Built vs What Was Planned

**ContractGenerator accuracy:** Target was > 70% clauses correct without manual editing. Actual: ~85%. Main failure was `git_commit` classified as `enum` (only 3 values in small snapshot) rather than a hex pattern — fixed by adding `pattern: "^[a-f0-9]{40}$"`. All other fields (types, required, uuid/datetime formats, enums) were auto-generated correctly.

**Registry as PRIMARY blast radius source:** The original project document described lineage graph traversal as the blast radius method. The Practitioner Manual correctly updates this: registry is primary, lineage graph is enrichment only. Our implementation follows the corrected model. This distinction is critical for Tier 2+ deployments.

**LangSmith traces from apex-ledger (Week 5) not Week 3:** The project document specifies traces from the Week 3 extraction agents. Our traces come from the apex-ledger LangGraph loan processing agents (210 runs). The schema is compatible with `trace_record` spec — the difference is `run_type: chain` (not `llm`) and `total_tokens: 0` (LangGraph chain nodes, not direct LLM calls). All 210 traces pass schema validation.

**Embedding model via OpenRouter:** Uses `openai/text-embedding-3-small` via OpenRouter (1536-dim) with fallback to local 256-dim n-gram embedder. The two models produce different dimension vectors — baselines from one cannot be used with the other. This is the cause of the `shapes not aligned` error described in the README troubleshooting section.

**What the contracts revealed about our own systems:** Writing contracts for our own data was more revealing than expected. Three genuine discoveries: (1) `metadata_causation_id` is 100% null in all 60 Week 5 event records — any consumer expecting causation chains silently receives nothing; (2) confidence values are clamped below 0.990 — no fact ever scored above this, consistent with a soft ceiling in the extraction model; (3) the Week 4 lineage graph uses `table::extractions READS → file::src/week4/cartographer.py` — the table is the source of the READS relationship, which is opposite to initial expectation and required careful graph traversal logic to detect correctly.