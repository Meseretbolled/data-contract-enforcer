# Data Contract Enforcer

> Turns every inter-system data interface into a machine-checked promise.  
> Detects silent schema violations, traces them to the guilty git commit, and maps the blast radius across all downstream consumers.

**Author:** Meseret Bolled · **TRP1 Week 7** · [GitHub](https://github.com/Meseretbolled/data-contract-enforcer)

---

## Architecture Diagrams

### Input-Output Contract Flow
![Input-Output Contract Flow](assets/Input-Output%20Contract%20Flow.png)

### Full System Architecture
![Full System Architecture](assets/architecture_overview.png)

### Violation Detection Flow
![Violation Detection Flow](assets/violation_flow.png)


## The Problem This Solves

The Week 3 Document Refinery outputs `extracted_facts[].confidence` as a float `0.0–1.0`. A developer changes it to a percentage scale `0–100`. No exception is raised. No pipeline crashes. The output is silently wrong — until this system catches it with two independent checks:

| Check | What it detects | Can it be bypassed? |
|-------|----------------|---------------------|
| `range` — max=99.0 exceeds contract max 1.0 | Structural violation | Only by editing the contract |
| `statistical_drift` — z-score ≈ 797 from baseline | Distribution shift | **No** — reads baseline, not the contract |

---

## Architecture

```
Data Sources (Weeks 1–5 + LangSmith)
        │
        ▼
ContractGenerator          8-step pipeline: load → flatten → profile →
contracts/generator.py     LLM annotate (OpenRouter) → lineage inject →
                           Bitol YAML → dbt schema.yml → snapshot
        │
        ▼
ValidationRunner           --mode AUDIT | WARN | ENFORCE
contracts/runner.py        Checks: required, type, range, uuid, enum,
                           datetime, statistical drift (z-score)
        │
   ┌────┴────┐
 PASS      FAIL ──► ViolationAttributor    Registry blast radius (PRIMARY)
                    contracts/attributor.py Lineage transitive BFS
                           │               Git blame + confidence score
                           ▼
                    violation_log/violations.jsonl
        │
        ├──► SchemaEvolutionAnalyzer   Snapshot diffs · BREAKING/COMPATIBLE
        │    contracts/schema_analyzer.py  taxonomy · migration checklist
        │
        ├──► AI Contract Extensions    Embedding drift · prompt validation
        │    contracts/ai_extensions.py   output violation rate · writes to log
        │
        ▼
ReportGenerator            Health score 0–100 · plain-English violations
contracts/report_generator.py  per-consumer failure analysis · recommendations
        │
        ▼
enforcer_report/report_data.json
```

---

## Repository Structure

```
data-contract-enforcer/
│
├── contracts/                      Core enforcement components
│   ├── generator.py                ContractGenerator — 8-step auto-generation
│   ├── runner.py                   ValidationRunner  — AUDIT / WARN / ENFORCE
│   ├── attributor.py               ViolationAttributor — blast radius + git blame
│   ├── schema_analyzer.py          SchemaEvolutionAnalyzer — snapshot diffing
│   ├── ai_extensions.py            AI Extensions — drift, validation, rate
│   └── report_generator.py         ReportGenerator — health score + 5 sections
│
├── contract_registry/
│   └── subscriptions.yaml          7 subscriptions · tier · failure_mode · on_violation
│
├── generated_contracts/            Auto-generated — do not edit manually
│   ├── week1-intent-records        8 clauses
│   ├── week2-verdict-records       8 clauses
│   ├── week3-document-refinery-extractions   13 clauses ← confidence 0.0–1.0
│   ├── week4-lineage-snapshots     8 clauses
│   ├── week5-event-records         31 clauses
│   └── langsmith-traces            28 clauses
│   (each contract has a .yaml + _dbt.yml counterpart)
│
├── outputs/                        Input JSONL data files
│   ├── week1/intent_records.jsonl          50 records
│   ├── week2/verdicts.jsonl                50 records
│   ├── week3/extractions.jsonl             50 records  ← clean baseline
│   ├── week3/extractions_violated.jsonl    50 records  ← confidence × 100
│   ├── week4/lineage_snapshots.jsonl       3 snapshots
│   ├── week5/events.jsonl                  60 records
│   └── traces/runs.jsonl                   210 records
│
├── validation_reports/             Validation run outputs
│   ├── week*_clean.json            All passing — baselines established here
│   ├── week3_violated.json         2 FAILED — core violation evidence
│   ├── ai_extensions.json          Overall: PASS
│   └── schema_evolution_all.json
│
├── violation_log/
│   └── violations.jsonl            Attributed violations with blast radius + git blame
│
├── schema_snapshots/
│   ├── baselines.json              Statistical baselines (written from clean data only)
│   ├── embedding_baselines.npz     Embedding centroid baseline
│   └── <contract-id>/              2+ timestamped YAML snapshots per contract
│
├── enforcer_report/
│   └── report_data.json            Health score 70/100 · auto-generated
│
├── assets/                         Architecture diagrams
├── create_violation.py             Injects confidence × 100 scale change
└── DOMAIN_NOTES.md
```

---

## Setup

```bash
# Python 3.11+
pip install pandas pyyaml numpy scikit-learn openai python-dotenv gitpython

# .env
OPENAI_API_KEY=your_openrouter_key
OPENAI_BASE_URL=https://openrouter.ai/api/v1
```

---

## Running the Pipeline

### 1 — Generate contracts (all 6)

```bash
python contracts/generator.py \
    --source outputs/week3/extractions.jsonl \
    --contract-id week3-document-refinery-extractions \
    --lineage outputs/week4/lineage_snapshots.jsonl \
    --output generated_contracts/
# Repeat for week1, week2, week4, week5, langsmith-traces
# Use --no-llm to skip OpenRouter annotation
```

### 2 — Validate clean data (establishes baselines)

```bash
python contracts/runner.py \
    --contract generated_contracts/week3-document-refinery-extractions.yaml \
    --data    outputs/week3/extractions.jsonl \
    --output  validation_reports/week3_clean.json \
    --mode    AUDIT
# 30 passed · 0 failed · baselines saved
```

> Run clean data **before** violated data — baselines are written on the first run only.

### 3 — Detect the violation (ENFORCE mode)

```bash
python contracts/runner.py \
    --contract generated_contracts/week3-document-refinery-extractions.yaml \
    --data    outputs/week3/extractions_violated.jsonl \
    --output  validation_reports/week3_violated.json \
    --mode    ENFORCE
# ❌ confidence.range FAIL · ❌ confidence.statistical_drift FAIL
# 🚫 PIPELINE BLOCKED
```

| Mode | Behaviour |
|------|-----------|
| `AUDIT` | Log only, never block |
| `WARN` | Block on CRITICAL, quarantine data |
| `ENFORCE` | Block on CRITICAL + HIGH, exit code 1 |

### 4 — Attribute violations

```bash
python contracts/attributor.py \
    --violation validation_reports/week3_violated.json \
    --lineage   outputs/week4/lineage_snapshots.jsonl \
    --registry  contract_registry/subscriptions.yaml \
    --output    violation_log/violations.jsonl
# 2 subscribers · depth 3 · git blame score=1.0
```

### 5 — Schema evolution

```bash
python contracts/schema_analyzer.py --all \
    --output validation_reports/schema_evolution_all.json
```

### 6 — AI extensions

```bash
python contracts/ai_extensions.py \
    --extractions outputs/week3/extractions.jsonl \
    --verdicts    outputs/week2/verdicts.jsonl \
    --traces      outputs/traces/runs.jsonl \
    --output      validation_reports/ai_extensions.json \
    --violation-log violation_log/violations.jsonl
```

### 7 — Generate report

```bash
python contracts/report_generator.py --output enforcer_report/report_data.json
# Health score: 70/100
```

---

## Key Design Decisions

**Registry over lineage for blast radius.** At Tier 2+ (multi-team), external lineage graphs are inaccessible. The `contract_registry/subscriptions.yaml` is the primary source. Lineage enriches it with transitive depth — it does not replace it.

**Two checks, two defence lines.** The range check can be defeated by editing the contract. The statistical drift check reads only the stored baseline — editing the contract does nothing. Both must fire to block.

**Baselines written once.** Overwriting baselines on every run would let violated data become the new normal. Reset deliberately: `rm schema_snapshots/baselines.json`.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `failed = 0` on violated data | `rm schema_snapshots/baselines.json` → re-run clean first |
| Attributor shows 0 subscribers | Check `breaking_fields` in `contract_registry/subscriptions.yaml` |
| Embedding shape mismatch | `rm schema_snapshots/embedding_baselines.npz` → re-run |
| Schema analyzer finds no diff | Re-run generator on violated data to create a second snapshot |
| LLM annotation skipped | Set `OPENAI_API_KEY` in `.env` or pass `--no-llm` |