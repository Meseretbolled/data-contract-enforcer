# Data Contract Enforcer — Week 7

> **Turns every inter-system data interface into a machine-checked promise.**  
> Detects silent schema violations, traces them to the guilty git commit,  
> and reports the blast radius in plain English.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA CONTRACT ENFORCER                           │
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌─────────────────────────┐  │
│  │    Week 3    │    │    Week 5    │    │       Week 4            │  │
│  │  extractions │    │    events   │    │  lineage_snapshots      │  │
│  │   .jsonl     │    │   .jsonl    │    │      .jsonl             │  │
│  └──────┬───────┘    └──────┬──────┘    └───────────┬─────────────┘  │
│         │                   │                        │                 │
│         └───────────────────┼────────────────────────┘                 │
│                             │  INPUT                                   │
│                             ▼                                          │
│                  ┌──────────────────────┐                              │
│                  │  ContractGenerator   │  contracts/generator.py      │
│                  │  • Profile columns   │                              │
│                  │  • Detect ranges     │                              │
│                  │  • Inject lineage    │                              │
│                  │  • LLM annotations  │                              │
│                  └──────────┬───────────┘                              │
│                             │                                          │
│              ┌──────────────┴───────────────────┐                     │
│              ▼                                   ▼                     │
│  ┌───────────────────────┐       ┌───────────────────────────┐        │
│  │  week3-...-extractions│       │   week3-..._dbt.yml       │        │
│  │       .yaml           │       │  (dbt-compatible tests)   │        │
│  │  (Bitol contract)     │       └───────────────────────────┘        │
│  └───────────┬───────────┘                                             │
│              │  CONTRACT                                               │
│              ▼                                                         │
│    ┌──────────────────────┐                                            │
│    │  ValidationRunner    │  contracts/runner.py                       │
│    │  • required checks   │                                            │
│    │  • type checks       │◄── data snapshot (JSONL)                  │
│    │  • range checks ◄────┼──── THE KEY CHECK (0.0–1.0 vs 0–100)     │
│    │  • enum checks       │                                            │
│    │  • uuid checks       │                                            │
│    │  • drift checks      │◄── schema_snapshots/baselines.json        │
│    └──────────┬───────────┘                                            │
│               │  REPORT JSON                                           │
│        ┌──────┴──────┐                                                 │
│        │  PASS / FAIL│                                                 │
│        └──────┬──────┘                                                 │
│         FAIL  │                                                        │
│               ▼                                                        │
│    ┌──────────────────────┐    ┌──────────────────────────┐           │
│    │ ViolationAttributor  │    │ SchemaEvolutionAnalyzer  │           │
│    │ • Lineage traversal  │    │ • Diff snapshots         │           │
│    │ • git log / blame    │    │ • Classify changes       │           │
│    │ • Blame chain        │    │ • Migration report       │           │
│    │ • Blast radius       │    └──────────────────────────┘           │
│    └──────────┬───────────┘                                            │
│               │                                                        │
│               ▼                                                        │
│    ┌──────────────────────┐    ┌──────────────────────────┐           │
│    │  AI Contract Exts    │    │    ReportGenerator       │           │
│    │ • Embedding drift    │    │ • Data Health Score      │           │
│    │ • Prompt validation  │───►│ • Plain-English report   │           │
│    │ • Output schema rate │    │ • Recommendations        │           │
│    └──────────────────────┘    └──────────────────────────┘           │
│                                         │                              │
│                                         ▼                              │
│                              enforcer_report/report_data.json          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Between Systems

```
Week 3 extractions.jsonl
  doc_id, extracted_facts[].confidence (0.0–1.0), entities[]
        │
        ├──► Week 4 lineage_snapshots.jsonl
        │       (doc_id becomes a node; facts become node metadata)
        │
        └──► Week 7 ContractGenerator + ValidationRunner
                (contract enforces confidence range 0.0–1.0)

Week 5 events.jsonl
  event_type, sequence_number (monotonic), occurred_at <= recorded_at
        │
        └──► Week 7 ContractGenerator + ValidationRunner
                (contract enforces temporal ordering & enum values)

Week 4 lineage_snapshots.jsonl
  nodes[], edges[] (FILE/TABLE/PIPELINE + WRITES/READS/PRODUCES)
        │
        └──► Week 7 ViolationAttributor
                (used to traverse upstream to find blame candidates)
```

---

## Repository Structure

```
data-contract-enforcer/
│
├── contracts/
│   ├── generator.py          # ContractGenerator entry point
│   ├── runner.py             # ValidationRunner entry point
│   ├── attributor.py         # ViolationAttributor (Sunday)
│   ├── schema_analyzer.py    # SchemaEvolutionAnalyzer (Sunday)
│   ├── ai_extensions.py      # AI Contract Extensions (Sunday)
│   └── report_generator.py   # ReportGenerator (Sunday)
│
├── generated_contracts/      # OUTPUT — auto-generated YAML contracts
│   ├── week3-document-refinery-extractions.yaml
│   ├── week3-document-refinery-extractions_dbt.yml
│   ├── week5-event-records.yaml
│   └── week5-event-records_dbt.yml
│
├── validation_reports/       # OUTPUT — structured validation JSONs
├── violation_log/            # OUTPUT — violation + blame chain records
├── schema_snapshots/         # OUTPUT — timestamped schema snapshots
│   └── baselines.json        # Statistical baselines
├── enforcer_report/          # OUTPUT — final stakeholder report
│
├── outputs/
│   ├── week3/
│   │   ├── extractions.jsonl           # 50 real records
│   │   └── extractions_violated.jsonl  # injected violation (run create_violation.py)
│   ├── week4/
│   │   └── lineage_snapshots.jsonl     # 1 lineage snapshot
│   └── week5/
│       └── events.jsonl               # 50 real records
│
├── create_violation.py       # Injects confidence scale violation for testing
├── DOMAIN_NOTES.md
└── README.md
```

---

## Prerequisites

```bash
pip install pandas pyyaml numpy scikit-learn jsonschema gitpython openai anthropic
```

Python 3.11+ required.

---

## Running the Full Pipeline (Thursday Submission)

Run these commands **in order** from the repo root. Each step depends on the previous.

---

### Step 1 — Generate Week 3 Contract

```bash
python contracts/generator.py \
  --source      outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage     outputs/week4/lineage_snapshots.jsonl \
  --output      generated_contracts/
```

**Expected output:**
```
✅  Contract written    → generated_contracts/week3-document-refinery-extractions.yaml
✅  dbt schema written  → generated_contracts/week3-document-refinery-extractions_dbt.yml
📸  Snapshot saved      → schema_snapshots/week3-document-refinery-extractions/YYYYMMDD_HHMMSS.yaml
```

Minimum 8 clauses in the YAML. Open and verify `confidence` clause shows `minimum: 0.0, maximum: 1.0`.

---

### Step 2 — Generate Week 5 Contract

```bash
python contracts/generator.py \
  --source      outputs/week5/events.jsonl \
  --contract-id week5-event-records \
  --lineage     outputs/week4/lineage_snapshots.jsonl \
  --output      generated_contracts/
```

**Expected output:**
```
✅  Contract written    → generated_contracts/week5-event-records.yaml
✅  dbt schema written  → generated_contracts/week5-event-records_dbt.yml
```

---

### Step 3 — Run Validation on Clean Data (establishes baselines)

```bash
python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data     outputs/week3/extractions.jsonl \
  --output   validation_reports/week3_clean.json
```

**Expected output:**
```
✅  All structural checks PASS
📐  Baselines saved → schema_snapshots/baselines.json
📊  N passed  0 failed  0 warned  0 errored
```

---

### Step 4 — Inject Violation

```bash
python create_violation.py
```

**Expected output:**
```
✅  Violated dataset written : outputs/week3/extractions_violated.jsonl
   50 records modified
   Injection: confidence × 100  (0.0–1.0 → 0–100)
```

---

### Step 5 — Run Validation on Violated Data (must catch the violation)

```bash
python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data     outputs/week3/extractions_violated.jsonl \
  --output   validation_reports/week3_violated.json
```

**Expected output — violation MUST be caught:**
```
❌  week3-document-refinery-extractions.extracted_fact_confidence.range: FAIL
   CRITICAL — max=99.0 > contract maximum 1.0
   Likely cause: scale changed from 0.0–1.0 to 0–100
📊  N passed  1 failed  0 warned  0 errored
```

If `failed = 0` here, the range check is broken. Do not proceed until fixed.

---

### Step 6 — Verify the Report Schema

```bash
python -c "
import json
with open('validation_reports/week3_violated.json') as f:
    r = json.load(f)
print('report_id   :', r['report_id'])
print('contract_id :', r['contract_id'])
print('total_checks:', r['total_checks'])
print('failed      :', r['failed'])
fails = [x for x in r['results'] if x['status'] == 'FAIL']
for v in fails:
    print(f\"  ❌ {v['check_id']}\")
    print(f\"     {v['message']}\")
"
```

**Expected output:**
```
report_id   : <uuid>
contract_id : week3-document-refinery-extractions
total_checks: <N>
failed      : 1
  ❌ week3-document-refinery-extractions.extracted_fact_confidence.range
     'extracted_fact_confidence' range violation: max=99.0 > 1.0 ...
```

---

## Verification Checklist

After running all steps, verify the following files exist:

```bash
ls generated_contracts/
# week3-document-refinery-extractions.yaml        ← min 8 clauses
# week3-document-refinery-extractions_dbt.yml     ← dbt counterpart
# week5-event-records.yaml
# week5-event-records_dbt.yml

ls validation_reports/
# week3_clean.json          ← all PASS (clean data)
# week3_violated.json       ← at least 1 FAIL (violated data)

ls schema_snapshots/
# baselines.json
# week3-document-refinery-extractions/
# week5-event-records/

cat enforcer_report/report_data.json | python -c \
  "import json,sys; r=json.load(sys.stdin); \
   print('Health score:', r.get('data_health_score','NOT GENERATED YET'))"
```

---

## Key Design Decisions

**Why Week 4 lineage is an input, not a contracted output (Thursday)**  
The ContractGenerator needs the lineage graph to populate `downstream_consumers[]` in each contract. This tells the blast radius calculator which systems are affected by a violation. Week 4 is contracted in the Sunday submission.

**Why statistical drift catches what structural checks miss**  
A confidence value of `87.0` has type `float64` — the type check passes. Only the range check (`max <= 1.0`) and the statistical drift check (z-score ≈ 450) catch this violation. Both are implemented in `runner.py`.

**Why baselines are written only on the first run**  
If baselines were overwritten every run, a violated dataset would become the new baseline, defeating the purpose. Baselines are written once on a clean run and only explicitly reset when a legitimate data change requires it.