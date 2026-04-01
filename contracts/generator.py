"""
contracts/generator.py — ContractGenerator
==========================================
Reads a JSONL data file and auto-generates a Bitol-compatible data
contract YAML plus a dbt-compatible schema.yml counterpart.

Usage
-----
python contracts/generator.py \
    --source outputs/week3/extractions.jsonl \
    --contract-id week3-document-refinery-extractions \
    --lineage outputs/week4/lineage_snapshots.jsonl \
    --output generated_contracts/

python contracts/generator.py \
    --source outputs/week5/events.jsonl \
    --contract-id week5-event-records \
    --lineage outputs/week4/lineage_snapshots.jsonl \
    --output generated_contracts/
"""

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml


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
        # Split scalars / dicts from list fields
        base: dict = {}
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
            # Explode the first array found
            primary_key = next(iter(arrays))
            singular = primary_key.rstrip("s")          # extracted_facts → extracted_fact
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
            # Confidence-specific sanity checks
            if "confidence" in col_name:
                if clean.max() > 1.0:
                    print(
                        f"  ⚠  ALERT: '{col_name}' max={clean.max():.2f} — "
                        f"looks like 0–100 scale, not 0.0–1.0!"
                    )
                elif clean.mean() > 0.99:
                    print(f"  ⚠  '{col_name}' mean={clean.mean():.4f} — possibly clamped at 1.0")
                elif clean.mean() < 0.01:
                    print(f"  ⚠  '{col_name}' mean={clean.mean():.4f} — possibly all-zero / broken")

    return profile


# ── Step 3 — Translate a profile to a Bitol contract clause ───────────────────

def profile_to_clause(profile: dict) -> dict:
    """
    Apply mapping rules to convert a column profile into a contract clause dict.

    Rules (in priority order)
    -------------------------
    1. confidence in name + numeric  →  minimum:0.0, maximum:1.0
    2. name ends with _id            →  format:uuid
    3. name ends with _at            →  format:date-time
    4. low-cardinality string        →  enum:[…]
    5. numeric with stats            →  description with observed range
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
            "Confidence score. MUST remain in 0.0–1.0 float range. "
            "A value of 0.87 means 87 % confidence. "
            "BREAKING CHANGE if converted to integer 0–100 percentage scale — "
            "all downstream threshold comparisons will silently produce wrong results."
        )

    elif profile["name"].endswith("_id"):
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
    ):
        clause["enum"]        = sorted(profile["sample_values"])
        clause["description"] = f"Enumerated value. Allowed: {clause['enum']}."

    if "stats" in profile and "description" not in clause:
        s = profile["stats"]
        clause["description"] = (
            f"Observed range [{s['min']:.3f}, {s['max']:.3f}], "
            f"mean={s['mean']:.3f}, stddev={s['stddev']:.3f}."
        )

    return clause


# ── Step 4 — Load lineage context from Week 4 snapshot ────────────────────────

def load_downstream_consumers(lineage_path: str | None, contract_id: str) -> list[dict]:
    """
    Read the most recent Week 4 lineage snapshot and find nodes that
    consume the table produced by *this* contract's system.

    Returns a list of downstream consumer dicts for the contract lineage section.
    """
    if not lineage_path or not Path(lineage_path).exists():
        print("  ℹ  No lineage file found — downstream consumers left empty.")
        return []

    try:
        records = load_jsonl(lineage_path)
        if not records:
            return []

        snapshot = records[-1]                      # most recent snapshot
        system   = contract_id.split("-")[0]        # "week3" from "week3-document-refinery-…"

        consumers: list[dict] = []
        for edge in snapshot.get("edges", []):
            src = edge.get("source", "")
            tgt = edge.get("target", "")
            rel = edge.get("relationship", "")

            # An edge FROM our system TO somewhere → "somewhere" consumes our output
            if system in src and rel in ("WRITES", "PRODUCES", "CALLS"):
                consumers.append({
                    "id":                 tgt,
                    "description":        "Downstream consumer identified via Week 4 lineage graph",
                    "fields_consumed":    ["doc_id", "extracted_facts"],
                    "breaking_if_changed": ["extracted_facts.confidence", "doc_id"],
                })

        # Deduplicate by id
        seen: set = set()
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


# ── Step 5 — Assemble the full Bitol contract dict ────────────────────────────

def build_contract(
    contract_id:  str,
    source_path:  str,
    column_profiles: dict,
    downstream:   list[dict],
) -> dict:
    schema = {
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
                f"Generated at {datetime.utcnow().isoformat()}Z. "
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
            "limitations": "confidence fields MUST remain in 0.0–1.0 float range.",
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


# ── Step 5b — Generate dbt schema.yml counterpart ─────────────────────────────

def generate_dbt_yaml(contract: dict, contract_id: str, output_dir: str) -> None:
    """
    Produce a dbt-compatible schema.yml with equivalent test definitions.

    Mapping
    -------
    required: true          →  not_null test
    enum: [...]             →  accepted_values test
    format: uuid            →  unique test  (IDs must be unique)
    """
    columns: list[dict] = []

    for col_name, clause in contract["schema"].items():
        col: dict = {"name": col_name}
        tests: list = []

        if clause.get("required"):
            tests.append("not_null")
        if "enum" in clause:
            tests.append({"accepted_values": {"values": clause["enum"]}})
        if clause.get("format") == "uuid":
            tests.append("unique")

        if tests:
            col["tests"] = tests
        if "description" in clause:
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
    print(f"  ✅  dbt schema written  → {dbt_path}")


# ── Step 6 — Save timestamped schema snapshot ─────────────────────────────────

def save_snapshot(contract: dict, contract_id: str) -> Path:
    """
    Write a timestamped copy of the contract to schema_snapshots/{contract_id}/.
    These snapshots are consumed by SchemaEvolutionAnalyzer to detect changes.
    """
    snap_dir = Path("schema_snapshots") / contract_id
    snap_dir.mkdir(parents=True, exist_ok=True)

    ts      = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    snap_path = snap_dir / f"{ts}.yaml"

    with open(snap_path, "w", encoding="utf-8") as fh:
        yaml.dump(contract, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"  📸  Snapshot saved      → {snap_path}")
    return snap_path


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a Bitol data contract from a JSONL file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source",      required=True,  help="Path to input JSONL file")
    parser.add_argument("--contract-id", required=True,  help="Unique contract identifier")
    parser.add_argument("--lineage",     default=None,   help="Path to Week 4 lineage JSONL (optional)")
    parser.add_argument("--output",      default="generated_contracts/", help="Output directory")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  ContractGenerator")
    print(f"{'='*60}")
    print(f"  Source      : {args.source}")
    print(f"  Contract ID : {args.contract_id}")
    print(f"  Lineage     : {args.lineage or '(not provided)'}")
    print(f"  Output      : {args.output}\n")

    # ── 1. Load ──────────────────────────────────────────────────────
    print("Step 1 — Loading data …")
    records = load_jsonl(args.source)
    if not records:
        print("ERROR: No records found in source file.")
        sys.exit(1)
    print(f"  ✅  Loaded {len(records)} records")

    # ── 2. Flatten ───────────────────────────────────────────────────
    print("Step 2 — Flattening nested records …")
    df = flatten_records(records)
    print(f"  ✅  DataFrame: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"  📋  Columns  : {list(df.columns)}")

    # ── 3. Profile ───────────────────────────────────────────────────
    print("Step 3 — Profiling columns …")
    column_profiles: dict = {}
    for col in df.columns:
        column_profiles[col] = profile_column(df[col], col)
        p = column_profiles[col]
        line = (
            f"  📊  {col:<45} "
            f"type={p['dtype']:<10} "
            f"nulls={p['null_fraction']*100:5.1f}%  "
            f"cardinality={p['cardinality']}"
        )
        if "stats" in p:
            s = p["stats"]
            line += f"  range=[{s['min']:.3f}, {s['max']:.3f}]"
        print(line)

    # ── 4. Lineage ───────────────────────────────────────────────────
    print("Step 4 — Loading lineage context …")
    downstream = load_downstream_consumers(args.lineage, args.contract_id)

    # ── 5. Build contract ────────────────────────────────────────────
    print("Step 5 — Building contract …")
    contract = build_contract(
        contract_id      = args.contract_id,
        source_path      = args.source,
        column_profiles  = column_profiles,
        downstream       = downstream,
    )
    num_clauses = len(contract["schema"])
    print(f"  ✅  {num_clauses} schema clause(s) generated")

    if num_clauses < 8:
        print(
            f"  ⚠  Only {num_clauses} clauses. Minimum is 8. "
            "Consider whether the source data has enough columns."
        )

    # ── 6. Write YAML ────────────────────────────────────────────────
    Path(args.output).mkdir(parents=True, exist_ok=True)
    out_path = Path(args.output) / f"{args.contract_id}.yaml"
    with open(out_path, "w", encoding="utf-8") as fh:
        yaml.dump(contract, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"  ✅  Contract written    → {out_path}")

    # ── 7. Write dbt YAML ────────────────────────────────────────────
    print("Step 6 — Generating dbt schema.yml …")
    generate_dbt_yaml(contract, args.contract_id, args.output)

    # ── 8. Save snapshot ─────────────────────────────────────────────
    print("Step 7 — Saving schema snapshot …")
    save_snapshot(contract, args.contract_id)

    print(f"\n{'='*60}")
    print(f"  Done.  Contract has {num_clauses} clauses.")
    print(f"  Open {out_path} and verify each clause makes sense.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()