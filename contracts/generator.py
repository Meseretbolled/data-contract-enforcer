"""
contracts/generator.py — ContractGenerator
==========================================
Reads a JSONL data file and auto-generates a Bitol-compatible data
contract YAML plus a dbt-compatible schema.yml counterpart.

Steps
-----
1.  Load JSONL → list of dicts
2.  Flatten nested records → flat DataFrame
3.  Statistical profiling per column (with distribution_warning detection)
3b. Save statistical baselines (mean/stddev per numeric column)
4b. LLM annotation for ambiguous columns (OpenRouter / any OpenAI-compatible endpoint)
5.  Load lineage context from Week 4 snapshot
6.  Build Bitol contract YAML (threads LLM annotations into clauses)
7.  Generate dbt schema.yml counterpart
8.  Save timestamped schema snapshot

Usage
-----
python contracts/generator.py \
    --source      outputs/week3/extractions.jsonl \
    --contract-id week3-document-refinery-extractions \
    --lineage     outputs/week4/lineage_snapshots.jsonl \
    --output      generated_contracts/

# Skip LLM annotation:
python contracts/generator.py --source ... --contract-id ... --no-llm
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ── Utilities ──────────────────────────────────────────────────────────────────

def load_jsonl(path: str) -> list[dict]:
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
    pat = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    if not sample_values:
        return False
    matches = sum(1 for v in sample_values if pat.match(str(v)))
    return matches >= len(sample_values) * 0.8


# ── Step 1 — Flatten nested JSONL ─────────────────────────────────────────────

def flatten_records(records: list[dict]) -> pd.DataFrame:
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


# ── Step 2 — Statistical profiling ────────────────────────────────────────────

def profile_column(series: pd.Series, col_name: str) -> dict:
    """
    Compute structural and statistical properties of a column.
    Detects suspicious distributions and writes them into the profile
    so profile_to_clause can embed distribution_warning into the clause.
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

            # Detect suspicious distributions — write into profile dict
            # so profile_to_clause can embed distribution_warning into the YAML clause
            warnings = []

            if "confidence" in col_name or (0.0 <= float(clean.mean()) <= 1.0 and float(clean.max()) <= 1.0):
                if float(clean.max()) > 1.0:
                    msg = (
                        f"SCALE_VIOLATION: max={clean.max():.2f} exceeds expected 0.0-1.0 range. "
                        f"Likely converted to 0-100 percentage scale — silently corrupts downstream."
                    )
                    warnings.append(msg)
                    print(f"  ⚠  ALERT: '{col_name}' {msg}")
                elif float(clean.mean()) > 0.99:
                    msg = f"CLAMPED_HIGH: mean={clean.mean():.4f} — possibly clamped at 1.0, check extractor."
                    warnings.append(msg)
                    print(f"  ⚠  '{col_name}' {msg}")
                elif float(clean.mean()) < 0.01:
                    msg = f"CLAMPED_LOW: mean={clean.mean():.4f} — possibly all-zero or broken extractor."
                    warnings.append(msg)
                    print(f"  ⚠  '{col_name}' {msg}")

            if float(clean.std()) == 0.0 and len(clean) > 1:
                msg = (
                    f"ZERO_VARIANCE: stddev=0.0 — all values identical ({clean.iloc[0]}). "
                    f"Possible constant injection."
                )
                warnings.append(msg)
                print(f"  ⚠  '{col_name}' {msg}")

            if warnings:
                profile["distribution_warnings"] = warnings

    return profile


# ── Step 3b — Save statistical baselines from profiling ───────────────────────

def save_baselines_from_profiles(
    column_profiles: dict,
    source_file: str,
    contract_id: str,
    baselines_path: str = "schema_snapshots/baselines.json",
) -> None:
    """
    Persist mean/stddev per numeric column to baselines.json.

    Called by ContractGenerator (not just ValidationRunner) so baselines
    exist before the first validation run. Only written if file does not
    already exist — violated data can never overwrite a clean baseline.
    """
    path = Path(baselines_path)
    if path.exists():
        print(f"  ℹ   Baselines already exist → {baselines_path} (delete to reset)")
        return

    Path(baselines_path).parent.mkdir(parents=True, exist_ok=True)
    baseline_cols = {}
    for col, prof in column_profiles.items():
        if "stats" in prof:
            s = prof["stats"]
            baseline_cols[col] = {
                "mean":   s["mean"],
                "stddev": s["stddev"],
                "min":    s["min"],
                "max":    s["max"],
            }

    payload = {
        "written_at":  now_iso(),
        "written_by":  "ContractGenerator",
        "source_file": source_file,
        "contract_id": contract_id,
        "columns":     baseline_cols,
    }
    with open(baselines_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    print(f"  📐  Baselines saved → {baselines_path} ({len(baseline_cols)} numeric columns)")


# ── Step 3 — Translate profile to Bitol contract clause ───────────────────────

_NEVER_ENUM = {"text", "excerpt", "path", "hash", "model", "service", "version"}


def profile_to_clause(profile: dict) -> dict:
    """
    Rules (in priority order)
    1. confidence in name + numeric         ->  minimum:0.0, maximum:1.0
    2. name ends with _id + UUID values     ->  format:uuid
    3. name ends with _at                   ->  format:date-time
    4. low-cardinality string (safe cols)   ->  enum:[...]
    5. numeric with stats                   ->  description with observed range
    6. distribution_warnings from profiling ->  distribution_warning field in clause
    """
    json_type = infer_json_type(profile["dtype"])
    clause: dict = {
        "type":     json_type,
        "required": profile["null_fraction"] == 0.0,
    }

    if "confidence" in profile["name"] and json_type == "number":
        clause["minimum"]     = 0.0
        clause["maximum"]     = 1.0
        clause["description"] = (
            "Confidence score. MUST remain in 0.0-1.0 float range. "
            "A value of 0.87 means 87% confidence. "
            "BREAKING CHANGE if converted to integer 0-100 percentage scale — "
            "all downstream threshold comparisons will silently produce wrong results."
        )

    elif (
        profile["name"].endswith("_id")
        and _looks_like_uuid(profile["sample_values"])
    ):
        clause["format"]      = "uuid"
        clause["description"] = (
            f"Unique identifier for "
            f"{profile['name'].replace('_id', '').replace('_', ' ')}. UUIDv4."
        )

    elif profile["name"].endswith("_at"):
        clause["format"]      = "date-time"
        clause["description"] = "ISO 8601 timestamp in UTC (Z suffix required)."

    elif (
        json_type == "string"
        and 2 <= profile["cardinality"] <= 8
        and profile["cardinality"] == len(profile["sample_values"])
        and not any(skip in profile["name"] for skip in _NEVER_ENUM)
    ):
        clause["enum"]        = sorted(profile["sample_values"])
        clause["description"] = f"Enumerated value. Allowed: {clause['enum']}."

    if "stats" in profile and "description" not in clause:
        s = profile["stats"]
        clause["description"] = (
            f"Observed range [{s['min']:.3f}, {s['max']:.3f}], "
            f"mean={s['mean']:.3f}, stddev={s['stddev']:.3f}."
        )

    # Rule 6 — embed distribution_warning into the clause for downstream programmatic use
    if profile.get("distribution_warnings"):
        clause["distribution_warning"] = " | ".join(profile["distribution_warnings"])

    return clause


# ── Step 4b — LLM Annotation (OpenRouter) ─────────────────────────────────────

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
    if col_name in _SKIP_LLM:
        return False
    if "description" in clause and len(clause["description"]) > 40:
        return False
    if col_name.endswith("_id") or col_name.endswith("_at"):
        return False
    if "confidence" in col_name:
        return False
    return True


def llm_annotate_columns(
    column_profiles: dict,
    clauses: dict,
    contract_id: str,
) -> dict:
    """
    Step 4b — LLM Annotation via OpenRouter.

    For any column whose business meaning is ambiguous, invoke Claude via
    OpenRouter and thread the annotation directly into the contract clause:
        description       — plain-English business meaning
        llm_business_rule — machine-readable validation hint
        llm_cross_column  — cross-field dependency, if any

    This surfaces LLM-derived metadata in Week 3 and Week 5 contract clauses
    so downstream tools can consume it programmatically.

    Uses OPENAI_API_KEY + OPENAI_BASE_URL from .env.
    Falls back gracefully if key is absent or API call fails.
    """
    api_key  = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1")

    if not api_key:
        print("  ℹ  No OPENAI_API_KEY — skipping LLM annotation (set key in .env to enable)")
        return clauses

    to_annotate = [
        col for col, clause in clauses.items()
        if _needs_llm_annotation(col, clause)
    ]

    if not to_annotate:
        print("  ✅  All columns have sufficient descriptions — LLM annotation skipped")
        return clauses

    print(f"  🤖  LLM annotating {len(to_annotate)} ambiguous column(s) via OpenRouter: "
          f"{to_annotate[:5]}")

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url=base_url)
        all_columns = list(column_profiles.keys())

        for col_name in to_annotate:
            profile = column_profiles[col_name]
            samples = profile.get("sample_values", [])

            prompt = (
                f"You are annotating a data contract for a data engineering system.\n\n"
                f"Table/contract: {contract_id}\n"
                f"Column name: {col_name}\n"
                f"Data type: {profile.get('dtype', 'unknown')}\n"
                f"Sample values (up to 5): {samples}\n"
                f"Adjacent columns: {[c for c in all_columns if c != col_name][:8]}\n\n"
                f"Provide a JSON object with exactly these three keys:\n"
                f"{{\n"
                f'  "description": "One sentence plain-English description of what this field contains and its business meaning.",\n'
                f'  "business_rule": "A validation expression like \'must be positive integer\' or \'must be one of [X, Y, Z]\' or \'must be >= 0.0 and <= 1.0\'.",\n'
                f'  "cross_column": "Any cross-column relationship, e.g. \'must reference a valid doc_id\' or \'none\'."\n'
                f"}}\n\n"
                f"Respond with ONLY the JSON object, no markdown, no preamble."
            )

            try:
                response = client.chat.completions.create(
                    model="anthropic/claude-3-haiku",
                    max_tokens=256,
                    messages=[{"role": "user", "content": prompt}]
                )
                text = response.choices[0].message.content.strip()
                text = re.sub(r"```[a-z]*\n?", "", text).strip()
                annotation = json.loads(text)

                if annotation.get("description"):
                    clauses[col_name]["description"] = annotation["description"]
                if annotation.get("business_rule") not in (None, "", "none"):
                    clauses[col_name]["llm_business_rule"] = annotation["business_rule"]
                if annotation.get("cross_column") not in (None, "", "none"):
                    clauses[col_name]["llm_cross_column"] = annotation["cross_column"]

                print(f"  🤖  Annotated '{col_name}': "
                      f"{annotation.get('description', '')[:60]}...")

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
    if not lineage_path or not Path(lineage_path).exists():
        print("  i  No lineage file found — downstream consumers left empty.")
        return []

    try:
        records = load_jsonl(lineage_path)
        if not records:
            return []

        snapshot = records[-1]
        system   = contract_id.split("-")[0]
        edges    = snapshot.get("edges", [])
        nodes    = snapshot.get("nodes", [])

        our_nodes: set = set()
        for node in nodes:
            nid  = node.get("node_id", "")
            path = node.get("metadata", {}).get("path", "")
            if system in nid or system in path:
                our_nodes.add(nid)

        table_map = {
            "week3": "table::extractions",
            "week5": "table::events",
        }
        if system in table_map:
            our_nodes.add(table_map[system])

        print(f"  🗺   Our system nodes : {our_nodes}")

        consumers: list[dict] = []
        for edge in edges:
            src = edge.get("source", "")
            tgt = edge.get("target", "")
            rel = edge.get("relationship", "")

            if tgt in our_nodes and rel in ("READS", "CONSUMES"):
                consumers.append({
                    "id": src, "description": f"Downstream: reads {system} output via {rel}",
                    "fields_consumed": ["doc_id", "extracted_facts"],
                    "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
                })
            if src in our_nodes and rel in ("WRITES", "PRODUCES") and tgt not in our_nodes:
                consumers.append({
                    "id": tgt, "description": f"Downstream: receives {system} data via {rel}",
                    "fields_consumed": ["doc_id", "extracted_facts"],
                    "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
                })
            if src in our_nodes and rel == "READS":
                consumers.append({
                    "id": tgt, "description": f"Downstream: {tgt} reads from {system} table",
                    "fields_consumed": ["doc_id", "extracted_facts"],
                    "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
                })
            if tgt in our_nodes and rel == "PRODUCES":
                consumers.append({
                    "id": src, "description": f"Downstream: {src} produces into {system} table",
                    "fields_consumed": ["doc_id", "extracted_facts"],
                    "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
                })

        seen:   set  = set()
        unique: list = []
        for c in consumers:
            if c["id"] not in seen:
                seen.add(c["id"])
                unique.append(c)

        print(f"  🔗  {len(unique)} downstream consumer(s) found in lineage.")
        return unique

    except Exception as exc:
        print(f"  ⚠  Could not read lineage file: {exc}")
        return []


# ── Step 6 — Assemble the full Bitol contract dict ────────────────────────────

def build_contract(
    contract_id:       str,
    source_path:       str,
    column_profiles:   dict,
    downstream:        list[dict],
    annotated_clauses: dict | None = None,
) -> dict:
    """
    Assemble the full Bitol contract dict.

    If annotated_clauses is provided (from LLM annotation step), those are
    used directly — they already contain llm_business_rule and llm_cross_column
    fields threaded in by llm_annotate_columns(). Otherwise falls back to
    building clauses from profiling alone.
    """
    schema = annotated_clauses if annotated_clauses is not None else {
        col: profile_to_clause(prof)
        for col, prof in column_profiles.items()
    }

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


# ── Step 7 — Generate dbt schema.yml counterpart ─────────────────────────────

def generate_dbt_yaml(contract: dict, contract_id: str, output_dir: str) -> None:
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

        if tests:
            col["tests"] = tests
        if "description" in clause:
            col["description"] = clause["description"]
        if "llm_business_rule" in clause:
            col["llm_business_rule"] = clause["llm_business_rule"]

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


# ── Step 8 — Save timestamped schema snapshot ─────────────────────────────────

def save_snapshot(contract: dict, contract_id: str) -> Path:
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
                        help="Skip LLM annotation step (faster, no API call needed)")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  ContractGenerator")
    print(f"{'='*60}")
    print(f"  Source      : {args.source}")
    print(f"  Contract ID : {args.contract_id}")
    print(f"  Lineage     : {args.lineage or '(not provided)'}")
    print(f"  Output      : {args.output}")
    print(f"  LLM         : {'disabled (--no-llm)' if args.no_llm else 'enabled (set --no-llm to skip)'}\n")

    # Step 1 — Load
    print("Step 1 — Loading data ...")
    records = load_jsonl(args.source)
    if not records:
        print("ERROR: No records found in source file.")
        sys.exit(1)
    print(f"  ✅  Loaded {len(records)} records")

    # Step 2 — Flatten
    print("Step 2 — Flattening nested records ...")
    df = flatten_records(records)
    print(f"  ✅  DataFrame: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"  📋  Columns  : {list(df.columns)}")

    # Step 3 — Profile
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

    # Step 3b — Save statistical baselines from profiling
    # Ensures baselines exist before ValidationRunner is first invoked.
    # Only written if file does not exist — violated data never corrupts baseline.
    print("Step 3b — Saving statistical baselines ...")
    save_baselines_from_profiles(
        column_profiles = column_profiles,
        source_file     = args.source,
        contract_id     = args.contract_id,
    )

    # Step 4 — Build initial clauses from profiling
    initial_clauses = {
        col: profile_to_clause(prof)
        for col, prof in column_profiles.items()
    }

    # Step 4b — LLM annotation of ambiguous columns
    # Threads llm_business_rule + llm_cross_column + description into clauses.
    # Week 3 and Week 5 paths both go through this step.
    print("Step 4b — LLM annotation of ambiguous columns ...")
    if args.no_llm:
        print("  ℹ  LLM annotation disabled via --no-llm flag")
        annotated_clauses = initial_clauses
    else:
        annotated_clauses = llm_annotate_columns(
            column_profiles = column_profiles,
            clauses         = initial_clauses,
            contract_id     = args.contract_id,
        )

    # Step 5 — Lineage
    print("Step 5 — Loading lineage context ...")
    downstream = load_downstream_consumers(args.lineage, args.contract_id)

    # Step 6 — Build contract (uses annotated clauses)
    print("Step 6 — Building contract ...")
    contract = build_contract(
        contract_id       = args.contract_id,
        source_path       = args.source,
        column_profiles   = column_profiles,
        downstream        = downstream,
        annotated_clauses = annotated_clauses,
    )
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

    # Step 7 — dbt YAML
    print("Step 7 — Generating dbt schema.yml ...")
    generate_dbt_yaml(contract, args.contract_id, args.output)

    # Step 8 — Snapshot
    print("Step 8 — Saving schema snapshot ...")
    save_snapshot(contract, args.contract_id)

    print(f"\n{'='*60}")
    print(f"  Done.  Contract has {num_clauses} clauses.")
    print(f"  Open {out_path} and verify each clause makes sense.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()