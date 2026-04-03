import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ── Utilities ──────────────────────────────────────────────────────────────────

def load_jsonl(path: str) -> list[dict]:
    """Load every non-empty line from a JSONL file into a list of dicts."""
    records = []
    with open(path, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"  ⚠  Skipping malformed line {lineno}: {exc}")
    return records


def infer_json_type(dtype_str: str) -> str:
    """Map a pandas dtype string to a JSON Schema type string."""
    return {
        "float64": "number",
        "float32": "number",
        "int64":   "integer",
        "int32":   "integer",
        "bool":    "boolean",
    }.get(dtype_str, "string")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _looks_like_uuid(sample_values: list) -> bool:
    """
    Return True only if the majority of sample values look like real UUIDs.
    Prevents false positives like 'user_0', 'user_1' getting format:uuid
    just because their column name ends in '_id'.
    """
    pat = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    if not sample_values:
        return False
    matches = sum(1 for v in sample_values if pat.match(str(v)))
    return matches >= len(sample_values) * 0.8


# ── Step 1 — Flatten nested JSONL to a flat DataFrame ─────────────────────────

def flatten_records(records: list[dict]) -> pd.DataFrame:
    """
    Explode the first list-valued field of each record into one row per item.

    Example
    -------
    Input : {"doc_id": "abc", "extracted_facts": [{"confidence": 0.9}, ...]}
    Output: rows  {"doc_id": "abc", "extracted_fact_confidence": 0.9, ...}

    Top-level dicts are flattened with underscore notation.
    Nested lists inside the exploded items are dropped (too deep).
    """
    rows: list[dict] = []

    for record in records:
        base:   dict = {}
        arrays: dict = {}

        for key, val in record.items():
            if isinstance(val, list):
                arrays[key] = val
            elif isinstance(val, dict):
                for sub_k, sub_v in val.items():
                    if not isinstance(sub_v, (list, dict)):
                        base[f"{key}_{sub_k}"] = sub_v
            else:
                base[key] = val

        if arrays:
            primary_key = next(iter(arrays))
            singular    = primary_key.rstrip("s")
            for item in arrays[primary_key]:
                if isinstance(item, dict):
                    row = dict(base)
                    for k, v in item.items():
                        if not isinstance(v, (list, dict)):
                            row[f"{singular}_{k}"] = v
                    rows.append(row)
                else:
                    rows.append(dict(base))
        else:
            rows.append(base)

    if not rows:
        raise ValueError("No rows extracted — check that the JSONL file is non-empty.")

    return pd.DataFrame(rows)


# ── Step 2 — Statistical profiling per column ─────────────────────────────────

def profile_column(series: pd.Series, col_name: str) -> dict:
    """
    Compute structural and statistical properties of a single column.
    Emits loud warnings for confidence columns that look wrong.
    """
    profile: dict = {
        "name":          col_name,
        "dtype":         str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality":   int(series.nunique()),
        "sample_values": [str(v) for v in series.dropna().unique()[:5].tolist()],
    }

    if pd.api.types.is_numeric_dtype(series):
        clean = series.dropna()
        if len(clean) > 0:
            profile["stats"] = {
                "min":    float(clean.min()),
                "max":    float(clean.max()),
                "mean":   float(clean.mean()),
                "p25":    float(clean.quantile(0.25)),
                "p50":    float(clean.quantile(0.50)),
                "p75":    float(clean.quantile(0.75)),
                "p95":    float(clean.quantile(0.95)),
                "p99":    float(clean.quantile(0.99)),
                "stddev": float(clean.std()),
            }
            if "confidence" in col_name:
                if clean.max() > 1.0:
                    print(
                        f"  ⚠  ALERT: '{col_name}' max={clean.max():.2f} — "
                        f"looks like 0-100 scale, not 0.0-1.0!"
                    )
                elif clean.mean() > 0.99:
                    print(f"  ⚠  '{col_name}' mean={clean.mean():.4f} — possibly clamped at 1.0")
                elif clean.mean() < 0.01:
                    print(f"  ⚠  '{col_name}' mean={clean.mean():.4f} — possibly all-zero / broken")

    return profile


# ── Step 3 — Translate a profile to a Bitol contract clause ───────────────────

# Columns that should NEVER be treated as enums even if low cardinality
_NEVER_ENUM = {"text", "excerpt", "path", "hash", "model", "service", "version"}


def profile_to_clause(profile: dict) -> dict:
    """
    Apply mapping rules to convert a column profile into a contract clause dict.

    Rules (in priority order)
    -------------------------
    1. confidence in name + numeric         ->  minimum:0.0, maximum:1.0
    2. name ends with _id + UUID values     ->  format:uuid
    3. name ends with _at                   ->  format:date-time
    4. low-cardinality string (safe cols)   ->  enum:[...]
    5. numeric with stats                   ->  description with observed range
    """
    json_type = infer_json_type(profile["dtype"])
    clause: dict = {
        "type":     json_type,
        "required": profile["null_fraction"] == 0.0,
    }

    # Rule 1 — confidence range (most important clause in the whole project)
    if "confidence" in profile["name"] and json_type == "number":
        clause["minimum"]     = 0.0
        clause["maximum"]     = 1.0
        clause["description"] = (
            "Confidence score. MUST remain in 0.0-1.0 float range. "
            "A value of 0.87 means 87% confidence. "
            "BREAKING CHANGE if converted to integer 0-100 percentage scale — "
            "all downstream threshold comparisons will silently produce wrong results."
        )

    # Rule 2 — UUID format (only if values actually look like UUIDs)
    elif (
        profile["name"].endswith("_id")
        and _looks_like_uuid(profile["sample_values"])
    ):
        clause["format"]      = "uuid"
        clause["description"] = (
            f"Unique identifier for "
            f"{profile['name'].replace('_id', '').replace('_', ' ')}. UUIDv4."
        )

    # Rule 3 — timestamp format
    elif profile["name"].endswith("_at"):
        clause["format"]      = "date-time"
        clause["description"] = "ISO 8601 timestamp in UTC (Z suffix required)."

    # Rule 4 — low-cardinality enum (exclude free-text, hashes, paths etc.)
    elif (
        json_type == "string"
        and 2 <= profile["cardinality"] <= 8
        and profile["cardinality"] == len(profile["sample_values"])
        and not any(skip in profile["name"] for skip in _NEVER_ENUM)
    ):
        clause["enum"]        = sorted(profile["sample_values"])
        clause["description"] = f"Enumerated value. Allowed: {clause['enum']}."

    # Rule 5 — numeric description with observed range
    if "stats" in profile and "description" not in clause:
        s = profile["stats"]
        clause["description"] = (
            f"Observed range [{s['min']:.3f}, {s['max']:.3f}], "
            f"mean={s['mean']:.3f}, stddev={s['stddev']:.3f}."
        )

    return clause


# ── Step 4 — LLM Annotation (Anthropic Claude) ────────────────────────────────

# Columns whose meaning is obvious from name alone — skip LLM for efficiency
_SKIP_LLM = {
    "doc_id", "source_path", "source_hash", "extraction_model",
    "extracted_at", "processing_time_ms", "token_count_input",
    "token_count_output", "event_id", "aggregate_id", "aggregate_type",
    "sequence_number", "schema_version", "occurred_at", "recorded_at",
    "verdict_id", "rubric_id", "rubric_version", "evaluated_at",
    "intent_id", "created_at", "snapshot_id", "codebase_root",
    "git_commit", "captured_at",
}


def _needs_llm_annotation(col_name: str, clause: dict) -> bool:
    """
    Returns True if the column needs LLM annotation.
    We annotate columns that:
    - Don't already have a clear description
    - Aren't in the skip list
    - Have ambiguous names (not obviously an id, timestamp, or confidence)
    """
    if col_name in _SKIP_LLM:
        return False
    if "description" in clause and len(clause["description"]) > 40:
        return False
    # Skip clearly named columns
    if col_name.endswith("_id") or col_name.endswith("_at"):
        return False
    if "confidence" in col_name:
        return False
    return True


def llm_annotate_columns(
    column_profiles: dict,
    clauses: dict,
    contract_id: str,
    table_name: str,
) -> dict:
    """
    Step 4 — LLM Annotation via OpenRouter.

    For any column whose business meaning is ambiguous from name and sample
    values alone, invoke Claude via OpenRouter with:
      - column name
      - table/contract name
      - 5 sample values
      - adjacent column names (context)

    Ask for:
      (a) plain-English description
      (b) a business rule as a validation expression
      (c) any cross-column relationship

    Falls back gracefully if API key not set or API unavailable.
    Uses OPENAI_API_KEY + OPENAI_BASE_URL (OpenRouter) — no Anthropic key needed.
    """
    api_key  = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1")

    if not api_key:
        print("  ℹ  No OPENAI_API_KEY — skipping LLM annotation (set key in .env to enable)")
        return clauses

    # Identify columns needing annotation
    to_annotate = [
        col for col, clause in clauses.items()
        if _needs_llm_annotation(col, clause)
    ]

    if not to_annotate:
        print("  ✅  All columns have sufficient descriptions — LLM annotation skipped")
        return clauses

    print(f"  🤖  LLM annotating {len(to_annotate)} ambiguous column(s) via OpenRouter: {to_annotate[:5]}")

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url=base_url)
        all_columns = list(column_profiles.keys())

        for col_name in to_annotate:
            profile = column_profiles[col_name]
            samples = profile.get("sample_values", [])

            prompt = f"""You are annotating a data contract for a data engineering system.

Table/contract: {contract_id}
Column name: {col_name}
Data type: {profile.get('dtype', 'unknown')}
Sample values (up to 5): {samples}
Adjacent columns: {[c for c in all_columns if c != col_name][:8]}

Provide a JSON object with exactly these three keys:
{{
  "description": "One sentence plain-English description of what this field contains and its business meaning.",
  "business_rule": "A validation expression like 'must be positive integer' or 'must be one of [X, Y, Z]' or 'must be >= 0.0 and <= 1.0'.",
  "cross_column": "Any cross-column relationship, e.g. 'must reference a valid doc_id in the same record' or 'none'."
}}

Respond with ONLY the JSON object, no markdown, no preamble."""

            try:
                response = client.chat.completions.create(
                    model="anthropic/claude-3-haiku",
                    max_tokens=256,
                    messages=[{"role": "user", "content": prompt}]
                )
                text = response.choices[0].message.content.strip()
                text = re.sub(r"```[a-z]*\n?", "", text).strip()
                annotation = json.loads(text)

                if "description" in annotation and annotation["description"]:
                    clauses[col_name]["description"] = annotation["description"]
                if "business_rule" in annotation and annotation["business_rule"] not in ("none", ""):
                    clauses[col_name]["llm_business_rule"] = annotation["business_rule"]
                if "cross_column" in annotation and annotation["cross_column"] not in ("none", ""):
                    clauses[col_name]["llm_cross_column"] = annotation["cross_column"]

                print(f"  🤖  Annotated '{col_name}': {annotation.get('description','')[:60]}...")

            except Exception as col_err:
                print(f"  ⚠  LLM annotation failed for '{col_name}': {str(col_err)[:60]}")
                continue

    except ImportError:
        print("  ⚠  openai package not installed — pip install openai")
    except Exception as e:
        print(f"  ⚠  LLM annotation skipped: {str(e)[:80]}")

    return clauses


# ── Step 5 — Load lineage context from Week 4 snapshot ────────────────────────

def load_downstream_consumers(lineage_path: str | None, contract_id: str) -> list[dict]:
    """
    Read the most recent Week 4 lineage snapshot and find nodes that
    consume data produced by the contract's source system.

    Returns a list of downstream consumer dicts for the contract's lineage section.
    """
    if not lineage_path or not Path(lineage_path).exists():
        print("  i  No lineage file found — downstream consumers left empty.")
        return []

    try:
        records = load_jsonl(lineage_path)
        if not records:
            print("  ⚠  Lineage file is empty.")
            return []

        snapshot = records[-1]
        nodes = {n["node_id"]: n for n in snapshot.get("nodes", [])}
        edges = snapshot.get("edges", [])

        print(f"  🗺   Our system nodes : {set()}", end="")

        # Map contract_id to known producer node patterns
        producer_patterns = {
            "week3-document-refinery-extractions": [
                "file::src/week3/extractor.py",
                "table::extractions",
            ],
            "week4-lineage-snapshots": [
                "file::src/week4/cartographer.py",
                "table::lineage_snapshots",
            ],
            "week5-event-records": [
                "table::events",
                "file::src/week5/",
            ],
            "week2-verdict-records": [
                "table::verdicts",
            ],
            "week1-intent-records": [
                "table::intent_records",
            ],
            "langsmith-traces": [
                "table::traces",
            ],
        }

        our_nodes = set()
        for pattern in producer_patterns.get(contract_id, []):
            for nid in nodes:
                if pattern in nid or nid.startswith(pattern):
                    our_nodes.add(nid)

        # Also find by contract_id prefix
        prefix = contract_id.split("-")[0]
        for nid in nodes:
            if prefix in nid:
                our_nodes.add(nid)

        print(f"\r  🗺   Our system nodes : {our_nodes}")

        # Find downstream nodes via multiple edge patterns
        consumers = set()
        relationship_types = ("PRODUCES", "WRITES", "READS", "CONSUMES")

        for edge in edges:
            src = edge.get("source", "")
            tgt = edge.get("target", "")
            rel = edge.get("relationship", "")

            # Pattern A: our node WRITES/PRODUCES to target
            if src in our_nodes and rel in ("WRITES", "PRODUCES"):
                consumers.add(tgt)

            # Pattern B: target READS from our node
            if tgt in our_nodes and rel in ("READS", "CONSUMES"):
                consumers.add(src)

            # Pattern C: our node is READS target (table reads by file)
            if src in our_nodes and rel == "READS":
                consumers.add(tgt)

            # Pattern D: reversed — target reads our table
            if tgt in our_nodes and rel == "READS":
                consumers.add(src)

        # Remove self-references
        consumers -= our_nodes

        # Build downstream list
        unique = []
        seen   = set()
        for nid in consumers:
            if nid not in seen and nid in nodes:
                seen.add(nid)
                node = nodes[nid]
                unique.append({
                    "id":          nid,
                    "description": f"Downstream: reads {contract_id} output via {next((e['relationship'] for e in edges if e.get('target') == nid or e.get('source') == nid), 'edge')}",
                    "fields_consumed":    ["doc_id", "extracted_facts"],
                    "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
                })

        print(f"  🔗  {len(unique)} downstream consumer(s) found in lineage.")
        return unique

    except Exception as exc:
        print(f"  ⚠  Could not read lineage file: {exc}")
        return []


# ── Step 6 — Assemble the full Bitol contract dict ────────────────────────────

def build_contract(
    contract_id:     str,
    source_path:     str,
    column_profiles: dict,
    downstream:      list[dict],
    llm_annotations: dict | None = None,
) -> dict:
    schema = {
        col: profile_to_clause(prof)
        for col, prof in column_profiles.items()
    }

    # Apply LLM annotations if provided
    if llm_annotations:
        for col, clause in schema.items():
            if col in llm_annotations:
                clause.update(llm_annotations[col])

    return {
        "kind":       "DataContract",
        "apiVersion": "v3.0.0",
        "id":         contract_id,
        "info": {
            "title":       f"Contract — {contract_id}",
            "version":     "1.0.0",
            "owner":       "data-engineering-team",
            "description": (
                f"Auto-generated contract for {source_path}. "
                f"Generated at {now_iso()}. "
                "Review and validate all clauses before use in production."
            ),
        },
        "servers": {
            "local": {
                "type":   "local",
                "path":   source_path,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage":       "Internal inter-system data contract. Do not publish externally.",
            "limitations": "confidence fields MUST remain in 0.0-1.0 float range.",
        },
        "schema": schema,
        "quality": {
            "type": "SodaChecks",
            "specification": {
                f"checks for {contract_id}": [
                    "row_count >= 1",
                    "missing_count(doc_id) = 0" if "doc_id" in schema else "row_count >= 1",
                ]
            },
        },
        "lineage": {
            "upstream":   [],
            "downstream": downstream,
        },
    }


# ── Step 6b — Generate dbt schema.yml counterpart ─────────────────────────────

def generate_dbt_yaml(contract: dict, contract_id: str, output_dir: str) -> None:
    """
    Produce a dbt-compatible schema.yml with equivalent test definitions.

    Mapping
    -------
    required: true        ->  not_null test
    enum: [...]           ->  accepted_values test
    format: uuid          ->  unique test
    minimum + maximum     ->  expression_is_true range test
    """
    columns: list[dict] = []

    for col_name, clause in contract["schema"].items():
        col:   dict = {"name": col_name}
        tests: list = []

        if clause.get("required"):
            tests.append("not_null")

        if "enum" in clause:
            tests.append({"accepted_values": {"values": clause["enum"]}})

        if clause.get("format") == "uuid":
            tests.append("unique")

        if clause.get("minimum") is not None and clause.get("maximum") is not None:
            tests.append({
                "dbt_utils.expression_is_true": {
                    "expression": (
                        f"{col_name} >= {clause['minimum']} "
                        f"and {col_name} <= {clause['maximum']}"
                    )
                }
            })
            if "confidence" in col_name:
                # Add explicit description for the critical clause
                col["description"] = (
                    f"BREAKING CHANGE if converted to 0-100 scale. "
                    f"Range: [{clause['minimum']}, {clause['maximum']}]."
                )

        if tests:
            col["tests"] = tests
        if "description" in clause and "description" not in col:
            col["description"] = clause["description"]

        columns.append(col)

    dbt_doc = {
        "version": 2,
        "models": [{
            "name":        contract_id.replace("-", "_"),
            "description": contract["info"].get("description", ""),
            "columns":     columns,
        }],
    }

    dbt_path = Path(output_dir) / f"{contract_id}_dbt.yml"
    with open(dbt_path, "w", encoding="utf-8") as fh:
        yaml.dump(dbt_doc, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"  ✅  dbt schema written  -> {dbt_path}")


# ── Step 7 — Save timestamped schema snapshot ─────────────────────────────────

def save_snapshot(contract: dict, contract_id: str) -> Path:
    """
    Write a timestamped copy of the contract to schema_snapshots/{contract_id}/.
    These snapshots are consumed by SchemaEvolutionAnalyzer to detect changes.
    """
    snap_dir  = Path("schema_snapshots") / contract_id
    snap_dir.mkdir(parents=True, exist_ok=True)
    snap_path = snap_dir / f"{now_stamp()}.yaml"

    with open(snap_path, "w", encoding="utf-8") as fh:
        yaml.dump(contract, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"  📸  Snapshot saved      -> {snap_path}")
    return snap_path


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a Bitol data contract from a JSONL file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source",      required=True,  help="Path to input JSONL file")
    parser.add_argument("--contract-id", required=True,  help="Unique contract identifier")
    parser.add_argument("--lineage",     default=None,   help="Path to Week 4 lineage JSONL")
    parser.add_argument("--output",      default="generated_contracts/", help="Output directory")
    parser.add_argument("--no-llm",      action="store_true",
                        help="Skip LLM annotation step (faster, no API call)")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  ContractGenerator")
    print(f"{'='*60}")
    print(f"  Source      : {args.source}")
    print(f"  Contract ID : {args.contract_id}")
    print(f"  Lineage     : {args.lineage or '(not provided)'}")
    print(f"  Output      : {args.output}")
    print(f"  LLM         : {'disabled' if args.no_llm else 'enabled (set --no-llm to skip)'}\n")

    # 1. Load
    print("Step 1 — Loading data ...")
    records = load_jsonl(args.source)
    if not records:
        print("ERROR: No records found in source file.")
        sys.exit(1)
    print(f"  ✅  Loaded {len(records)} records")

    # 2. Flatten
    print("Step 2 — Flattening nested records ...")
    df = flatten_records(records)
    print(f"  ✅  DataFrame: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"  📋  Columns  : {list(df.columns)}")

    # 3. Profile
    print("Step 3 — Profiling columns ...")
    column_profiles: dict = {}
    for col in df.columns:
        column_profiles[col] = profile_column(df[col], col)
        p    = column_profiles[col]
        line = (
            f"  📊  {col:<45} "
            f"type={p['dtype']:<10} "
            f"nulls={p['null_fraction']*100:5.1f}%  "
            f"cardinality={p['cardinality']}"
        )
        if "stats" in p:
            s     = p["stats"]
            line += f"  range=[{s['min']:.3f}, {s['max']:.3f}]"
        print(line)

    # 4. Build initial clauses
    initial_clauses = {
        col: profile_to_clause(prof)
        for col, prof in column_profiles.items()
    }

    # 4b. LLM annotation step
    print("Step 4 — LLM annotation of ambiguous columns ...")
    if args.no_llm:
        print("  ℹ  LLM annotation disabled via --no-llm flag")
        annotated_clauses = initial_clauses
    else:
        annotated_clauses = llm_annotate_columns(
            column_profiles=column_profiles,
            clauses=initial_clauses,
            contract_id=args.contract_id,
            table_name=args.source,
        )

    # 5. Lineage
    print("Step 5 — Loading lineage context ...")
    downstream = load_downstream_consumers(args.lineage, args.contract_id)

    # 6. Build contract
    print("Step 6 — Building contract ...")
    contract = {
        "kind":       "DataContract",
        "apiVersion": "v3.0.0",
        "id":         args.contract_id,
        "info": {
            "title":       f"Contract — {args.contract_id}",
            "version":     "1.0.0",
            "owner":       "data-engineering-team",
            "description": (
                f"Auto-generated contract for {args.source}. "
                f"Generated at {now_iso()}. "
                "Review and validate all clauses before use in production."
            ),
        },
        "servers": {
            "local": {
                "type":   "local",
                "path":   args.source,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage":       "Internal inter-system data contract. Do not publish externally.",
            "limitations": "confidence fields MUST remain in 0.0-1.0 float range.",
        },
        "schema": annotated_clauses,
        "quality": {
            "type": "SodaChecks",
            "specification": {
                f"checks for {args.contract_id}": [
                    "row_count >= 1",
                    "missing_count(doc_id) = 0" if "doc_id" in annotated_clauses else "row_count >= 1",
                ]
            },
        },
        "lineage": {
            "upstream":   [],
            "downstream": downstream,
        },
    }

    num_clauses = len(contract["schema"])
    print(f"  ✅  {num_clauses} schema clause(s) generated")

    if num_clauses < 8:
        print(f"  ⚠  Only {num_clauses} clauses — minimum is 8.")

    # Write YAML
    Path(args.output).mkdir(parents=True, exist_ok=True)
    out_path = Path(args.output) / f"{args.contract_id}.yaml"
    with open(out_path, "w", encoding="utf-8") as fh:
        yaml.dump(contract, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"  ✅  Contract written    -> {out_path}")

    # 7. Write dbt YAML
    print("Step 7 — Generating dbt schema.yml ...")
    generate_dbt_yaml(contract, args.contract_id, args.output)

    # 8. Save snapshot
    print("Step 8 — Saving schema snapshot ...")
    save_snapshot(contract, args.contract_id)

    print(f"\n{'='*60}")
    print(f"  Done.  Contract has {num_clauses} clauses.")
    print(f"  Open {out_path} and verify each clause makes sense.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()