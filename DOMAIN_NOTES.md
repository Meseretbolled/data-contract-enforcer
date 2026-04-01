# DOMAIN_NOTES.md — Data Contract Enforcer
**Author:** Week 7 Submission  
**Date:** 2026-04-01  
**Systems Referenced:** Weeks 1–5 canonical schemas

---

## Question 1: Backward-Compatible vs Breaking Schema Changes

A **backward-compatible** change is one where existing downstream consumers continue to work correctly after the change — they do not need to update their code. A **breaking change** is one where existing consumers will fail, produce wrong results, or need to be updated before the change can be deployed safely.

### Three Backward-Compatible Examples (from our own schemas)

**Example 1 — Adding an optional field to `extraction_record` (Week 3)**  
Adding a new optional field `language_detected: "en"` to the top level of `extractions.jsonl` is safe. The Week 4 Cartographer reads `doc_id`, `extracted_facts`, and `entities`. It ignores fields it does not know about. The new field is simply skipped. No downstream code breaks.

**Example 2 — Adding a new enum value to `entity.type` (Week 3)**  
The current enum is `PERSON | ORG | LOCATION | DATE | AMOUNT | OTHER`. Adding a new value `PRODUCT` is additive. Consumers that switch on `entity.type` will hit their `default` or `OTHER` branch. No crash, no wrong calculation — they just treat `PRODUCT` as an unknown type until they update.

**Example 3 — Widening `processing_time_ms` from `int32` to `int64` (Week 3)**  
The current field `processing_time_ms: 1431` fits in a 32-bit integer. Widening to 64-bit means larger values are now accepted. Existing consumers reading this field as a number still work correctly — all old values still fit in the wider type.

### Three Breaking Examples (from our own schemas)

**Example 1 — Renaming `doc_id` to `document_id` (Week 3)**  
The Week 4 Cartographer looks up `record["doc_id"]` to create a node in the lineage graph. If Week 3 renames this field to `document_id`, the Cartographer raises a `KeyError` or silently gets `None`, producing a node with no identity. Every downstream lineage query fails. This is a breaking change with no grace period.

**Example 2 — Changing `confidence` from float `0.0–1.0` to integer `0–100` (Week 3)**  
The contract clause `minimum: 0.0, maximum: 1.0` would be violated immediately. Any consumer that uses confidence for threshold filtering — e.g. `if fact["confidence"] > 0.8: accept_fact()` — would now accept ALL facts because `87 > 0.8` is always true. Silent data corruption. See Question 2 for the full trace.

**Example 3 — Removing `sequence_number` from `event_record` (Week 5)**  
The Week 5 event store uses `sequence_number` to enforce monotonic ordering and detect duplicate events. If this field is removed, the deduplication logic raises a `KeyError` and the entire event pipeline halts. Every event consumer downstream stops receiving updates. This is a catastrophic breaking change.

---

## Question 2: The Confidence Scale Change — Full Trace

### Starting State
In `outputs/week3/extractions.jsonl` (our actual data file, 50 records), the `confidence` field inside each `extracted_facts[]` item is a float between 0.0 and 1.0. Running the measurement script on our actual data:

```
min=0.500  max=0.990  mean=0.763  stddev=0.137
```

This confirms the field is correctly in the 0.0–1.0 range. All 50 records, 150 total facts, pass the range constraint.

### The Breaking Change
A developer updates `src/week3/extractor.py` and changes the line:

```python
# Before
"confidence": round(raw_confidence, 2)          # e.g. 0.87

# After  
"confidence": round(raw_confidence * 100, 1)    # e.g. 87.0
```

The developer intends this to be "more human readable." They do not update the contract. They do not notify Week 4.

### How the Failure Propagates to Week 4

The Week 4 Cartographer reads `extracted_facts[].confidence` to score the reliability of each lineage node it creates. Its logic looks like:

```python
high_confidence_facts = [f for f in record["extracted_facts"] if f["confidence"] > 0.8]
```

After the change, `confidence = 87.0`. The check `87.0 > 0.8` is `True`. **Every single fact is now classified as high-confidence**, even facts that were originally scored at 0.52 (now 52.0). The Cartographer marks every document as highly reliable. Downstream quality filtering breaks silently.

No exception is raised. The pipeline runs to completion. The output looks normal. The corruption is invisible.

### The Contract Clause That Catches This (Bitol YAML)

```yaml
# In generated_contracts/week3-document-refinery-extractions.yaml
schema:
  extracted_fact_confidence:
    type: number
    minimum: 0.0
    maximum: 1.0
    required: true
    description: >
      Confidence score for this extracted fact. MUST remain in 0.0–1.0 float
      range. This is a decimal fraction, NOT a percentage. A value of 0.87
      means 87% confidence. BREAKING CHANGE if converted to integer 0–100
      scale — all downstream threshold comparisons will produce wrong results.
```

When the ValidationRunner runs the range check against the new data, it immediately reports:

```
FAIL: extracted_fact_confidence — max=99.0 > contract maximum 1.0
CRITICAL severity — 150 records failing
```

This catches the change before it propagates to Week 4.

---

## Question 3: How the Enforcer Uses the Lineage Graph to Produce a Blame Chain

When a contract violation is detected, the ViolationAttributor follows this exact sequence:

**Step 1 — Identify the failing schema element.**  
The ValidationRunner reports: `check_id = "week3-document-refinery-extractions.extracted_fact_confidence.range"`. We extract the system identifier: `week3`.

**Step 2 — Load the Week 4 lineage snapshot.**  
We open `outputs/week4/lineage_snapshots.jsonl` and take the most recent snapshot. This gives us a graph of nodes and edges.

**Step 3 — Breadth-first traversal upstream.**  
Starting from the failing table node `table::extractions`, we traverse edges in reverse — looking for edges where `target = table::extractions`. We find:

```
file::src/week3/extractor.py  --WRITES-->  table::extractions
```

So `src/week3/extractor.py` is the direct producer. Lineage distance = 1.

We then look one level further back — who writes to `extractor.py`? We find:

```
file::src/week3/document_loader.py  --CALLS-->  file::src/week3/extractor.py
```

Lineage distance = 2. We stop here (external boundary).

**Step 4 — Git blame integration.**  
For each upstream file, we run:

```bash
git log --follow --since="14 days ago" --format='%H|%ae|%ai|%s' -- src/week3/extractor.py
```

This returns all commits that touched `extractor.py` in the last 14 days. Each commit has a hash, author email, timestamp, and message.

**Step 5 — Confidence scoring.**  
For each commit candidate:

```
confidence = 1.0 - (days_since_commit × 0.1) - (lineage_distance × 0.2)
```

A commit made 1 day ago on the direct producer file scores: `1.0 - 0.1 - 0.2 = 0.7`. A commit made 5 days ago on a file 2 hops away scores: `1.0 - 0.5 - 0.4 = 0.1`. We sort by confidence descending and return the top 5 candidates.

**Step 6 — Blast radius.**  
From the contract's `lineage.downstream` section (populated at generation time), we read the list of all downstream consumers of `table::extractions`. These are the systems affected by this violation. In our case: `file::src/week4/cartographer.py` and `file::src/week7/contract_enforcer.py`.

---

## Question 4: Data Contract for LangSmith `trace_record`

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-records
info:
  title: LangSmith Trace Records
  version: 1.0.0
  owner: ai-engineering-team
  description: >
    One record per LangSmith run (LLM call, chain execution, or tool use).
    Used to monitor AI system performance, cost, and output quality.

servers:
  local:
    type: local
    path: outputs/traces/runs.jsonl
    format: jsonl

schema:
  # ── Structural Clauses ──────────────────────────────────────
  id:
    type: string
    format: uuid
    required: true
    unique: true
    description: Unique identifier for this trace run. UUIDv4.

  run_type:
    type: string
    required: true
    enum: [llm, chain, tool, retriever, embedding]
    description: >
      Category of run. BREAKING if new values added without notifying consumers
      that switch on this field.

  start_time:
    type: string
    format: date-time
    required: true
    description: ISO 8601 UTC timestamp when the run began.

  end_time:
    type: string
    format: date-time
    required: true
    description: >
      ISO 8601 UTC timestamp when the run completed.
      MUST be strictly greater than start_time.

  total_tokens:
    type: integer
    required: true
    minimum: 0
    description: >
      Total tokens consumed. MUST equal prompt_tokens + completion_tokens.
      Cross-field constraint — violation indicates billing data corruption.

  total_cost:
    type: number
    required: true
    minimum: 0.0
    description: Cost in USD. Must be >= 0. Negative values indicate billing error.

  # ── Statistical Clauses ─────────────────────────────────────
  prompt_tokens:
    type: integer
    minimum: 0
    description: >
      Prompt token count. Statistical baseline: mean should remain between
      2000–8000 for week3 extraction traces. Drift beyond 3 stddev signals
      prompt template change or document size shift.

  completion_tokens:
    type: integer
    minimum: 0
    description: >
      Completion token count. Mean should remain stable per prompt version.
      Rising mean may indicate model verbose drift.

  # ── AI-Specific Clauses ─────────────────────────────────────
  outputs:
    type: object
    required: false
    description: >
      AI-SPECIFIC: Structured output from the LLM. For week3 extraction runs,
      must contain keys [extracted_facts, entities]. Validate against
      prompt-version-specific JSON Schema. Track output_schema_violation_rate
      per prompt_hash — rising rate signals prompt degradation.

quality:
  type: SodaChecks
  specification:
    checks for langsmith-trace-records:
      - missing_count(id) = 0
      - duplicate_count(id) = 0
      - min(total_cost) >= 0
      - row_count >= 1

terms:
  usage: Internal AI observability contract.
  limitations: >
    end_time must always be > start_time.
    total_tokens must equal prompt_tokens + completion_tokens.
    These are cross-field constraints not expressible in JSON Schema alone.

lineage:
  upstream:
    - id: week3-document-refinery
      description: Week 3 extraction runs generate the majority of these traces
  downstream:
    - id: week7-ai-contract-extensions
      description: AI extensions consume trace records to compute violation rates
      fields_consumed: [run_type, total_tokens, total_cost, outputs]
      breaking_if_changed: [run_type, outputs]
```

---

## Question 5: Most Common Failure Mode — Why Contracts Go Stale

**The most common failure mode is contracts that are written once and never updated.** This is sometimes called "contract drift" — the code changes but the contract does not.

### Why Contracts Go Stale

Contracts go stale for three reasons. First, there is no automated enforcement in the deployment pipeline. If running the ValidationRunner is optional, developers skip it under deadline pressure. Second, contracts are treated as documentation rather than executable code. Documentation is always out of date; executable tests fail loudly. Third, the contract is owned by the consuming team, not the producing team. The producer changes the schema without knowing the contract exists.

### How Our Architecture Prevents This

**Prevention 1 — Snapshot-based evolution detection.** The ContractGenerator writes a timestamped snapshot on every run. The SchemaEvolutionAnalyzer diffs consecutive snapshots automatically. You cannot change the schema without the analyzer detecting and classifying it. The contract does not need to be manually updated — the system detects the drift itself.

**Prevention 2 — Statistical baselines.** After the first ValidationRunner run, `schema_snapshots/baselines.json` stores the mean and stddev of every numeric column. Future runs compute z-scores. A confidence scale change from 0.0–1.0 to 0–100 produces a z-score of approximately 450 (the mean shifts from 0.76 to 76.0 against a baseline stddev of 0.14). This is caught even if the developer also updates the contract maximum from 1.0 to 100 — the statistical check catches the scale change independent of the structural contract.

**Prevention 3 — Lineage-injected blast radius.** Because every contract contains `lineage.downstream` populated from the Week 4 graph, every violation automatically produces a blast radius report. This makes the cost of a contract violation visible immediately to every team that is affected — creating social pressure to keep contracts accurate.

**The residual risk** is that statistical baselines can be invalidated by legitimate data changes (e.g. processing a new document corpus with genuinely higher confidence scores). The mitigation is to require explicit baseline reset as a deliberate action with a documented reason, not an automatic one.