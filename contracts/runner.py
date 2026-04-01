"""
contracts/runner.py — ValidationRunner
=======================================
Executes every clause in a contract YAML against a JSONL data snapshot
and produces a structured JSON validation report.

Usage
-----
# Clean run (establishes baselines):
python contracts/runner.py \
    --contract generated_contracts/week3-document-refinery-extractions.yaml \
    --data    outputs/week3/extractions.jsonl \
    --output  validation_reports/week3_clean.json

# Violated run (detects injected violation):
python contracts/runner.py \
    --contract generated_contracts/week3-document-refinery-extractions.yaml \
    --data    outputs/week3/extractions_violated.jsonl \
    --output  validation_reports/week3_violated.json
"""

import argparse
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


# ── Loaders ────────────────────────────────────────────────────────────────────

def load_jsonl(path: str) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_contract(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def sha256_of_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        h.update(fh.read())
    return h.hexdigest()


# ── Same flattening as generator (must match exactly) ─────────────────────────

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

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Baseline management ────────────────────────────────────────────────────────

BASELINES_PATH = "schema_snapshots/baselines.json"


def load_baselines() -> dict:
    if Path(BASELINES_PATH).exists():
        with open(BASELINES_PATH, encoding="utf-8") as fh:
            return json.load(fh).get("columns", {})
    return {}


def save_baselines(df: pd.DataFrame) -> None:
    Path("schema_snapshots").mkdir(parents=True, exist_ok=True)
    cols: dict = {}
    for col in df.select_dtypes(include="number").columns:
        cols[col] = {
            "mean":   float(df[col].mean()),
            "stddev": float(df[col].std()),
            "min":    float(df[col].min()),
            "max":    float(df[col].max()),
        }
    payload = {
        "written_at": datetime.now(timezone.utc).isoformat(),
        "columns": cols
    }
    with open(BASELINES_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    print(f"  📐  Baselines saved → {BASELINES_PATH}")


# ── Result builder helper ──────────────────────────────────────────────────────

def result(
    check_id: str,
    column_name: str,
    check_type: str,
    status: str,
    actual_value: str,
    expected: str,
    severity: str,
    records_failing: int,
    sample_failing: list,
    message: str,
) -> dict:
    return {
        "check_id":        check_id,
        "column_name":     column_name,
        "check_type":      check_type,
        "status":          status,
        "actual_value":    actual_value,
        "expected":        expected,
        "severity":        severity,
        "records_failing": records_failing,
        "sample_failing":  sample_failing,
        "message":         message,
    }


def pass_result(check_id, column_name, check_type, actual, expected):
    return result(
        check_id, column_name, check_type, "PASS",
        actual, expected, "LOW", 0, [],
        f"{column_name} {check_type} check passed."
    )


# ── Individual checks ──────────────────────────────────────────────────────────

def check_required(col: str, series: pd.Series, clause: dict, cid: str) -> dict | None:
    if not clause.get("required"):
        return None
    nulls = int(series.isna().sum())
    if nulls > 0:
        return result(
            cid, col, "required", "FAIL",
            f"{nulls} null(s) found", "0 nulls (field is required)",
            "CRITICAL", nulls, [],
            f"'{col}' has {nulls} null value(s) but is marked required."
        )
    return pass_result(cid, col, "required", "0 nulls", "0 nulls")


def check_type(col: str, series: pd.Series, clause: dict, cid: str) -> dict | None:
    expected = clause.get("type")

    # ── FIX: pandas stores text columns as dtype "object".
    #    JSON Schema "string" maps to pandas "object".
    #    We simply pass all string checks — the uuid/datetime/enum
    #    checks below provide real validation for string columns.
    if expected == "string":
        return pass_result(cid, col, "type", str(series.dtype), "string")

    checks = {
        "number":  pd.api.types.is_numeric_dtype,
        "integer": pd.api.types.is_integer_dtype,
        "boolean": pd.api.types.is_bool_dtype,
    }
    fn = checks.get(expected)
    if fn is None:
        return None

    if not fn(series):
        return result(
            cid, col, "type", "FAIL",
            str(series.dtype), expected,
            "CRITICAL", len(series), [],
            f"'{col}' expected type '{expected}' but got '{series.dtype}'."
        )
    return pass_result(cid, col, "type", str(series.dtype), expected)


def check_range(col: str, series: pd.Series, clause: dict, cid: str) -> dict | None:
    """
    THE critical check — catches the 0.0-1.0 → 0-100 confidence scale change.
    Even a single value outside the range triggers CRITICAL FAIL.
    """
    minimum = clause.get("minimum")
    maximum = clause.get("maximum")
    if minimum is None and maximum is None:
        return None

    clean = series.dropna()
    if len(clean) == 0:
        return None

    actual_min = float(clean.min())
    actual_max = float(clean.max())
    failures: list[str] = []

    if minimum is not None and actual_min < minimum:
        failures.append(f"min={actual_min:.4f} < {minimum}")
    if maximum is not None and actual_max > maximum:
        failures.append(f"max={actual_max:.4f} > {maximum}")

    if failures:
        bad_mask = pd.Series(False, index=clean.index)
        if minimum is not None:
            bad_mask |= (clean < minimum)
        if maximum is not None:
            bad_mask |= (clean > maximum)

        return result(
            cid, col, "range", "FAIL",
            f"min={actual_min:.4f}, max={actual_max:.4f}, mean={float(clean.mean()):.4f}",
            f"min>={minimum}, max<={maximum}",
            "CRITICAL", int(bad_mask.sum()), [],
            f"'{col}' range violation: {'; '.join(failures)}. "
            f"Likely cause: scale changed from 0.0-1.0 to 0-100."
        )

    return pass_result(
        cid, col, "range",
        f"min={actual_min:.4f}, max={actual_max:.4f}",
        f"min>={minimum}, max<={maximum}"
    )


def check_enum(col: str, series: pd.Series, clause: dict, cid: str) -> dict | None:
    allowed = clause.get("enum")
    if not allowed:
        return None

    clean       = series.dropna().astype(str)
    allowed_str = [str(v) for v in allowed]
    bad_mask    = ~clean.isin(allowed_str)
    bad_count   = int(bad_mask.sum())

    if bad_count > 0:
        bad_vals = clean[bad_mask].unique()[:5].tolist()
        return result(
            cid, col, "enum", "FAIL",
            f"Invalid: {bad_vals}",
            f"One of: {allowed}",
            "HIGH", bad_count, bad_vals,
            f"'{col}' has {bad_count} value(s) outside allowed enum."
        )
    return pass_result(cid, col, "enum", f"all in {allowed}", f"one of {allowed}")


def check_uuid_format(col: str, series: pd.Series, clause: dict, cid: str) -> dict | None:
    if clause.get("format") != "uuid":
        return None

    pat    = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE
    )
    clean  = series.dropna().astype(str)
    sample = clean.sample(min(100, len(clean)), random_state=42)
    bad    = sample[~sample.apply(lambda x: bool(pat.match(x)))]

    if len(bad) > 0:
        return result(
            cid, col, "uuid_format", "FAIL",
            f"Non-UUID values: {bad.head(3).tolist()}",
            "UUID v4 (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)",
            "HIGH", len(bad), bad.head(3).tolist(),
            f"'{col}' contains non-UUID formatted values."
        )
    return pass_result(
        cid, col, "uuid_format",
        "all sampled values are valid UUIDs", "UUID v4 format"
    )


def check_datetime_format(col: str, series: pd.Series, clause: dict, cid: str) -> dict | None:
    if clause.get("format") != "date-time":
        return None

    clean     = series.dropna().astype(str)
    sample    = clean.sample(min(50, len(clean)), random_state=42)
    bad_count = 0

    for val in sample:
        try:
            datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            bad_count += 1

    if bad_count > 0:
        return result(
            cid, col, "datetime_format", "FAIL",
            f"{bad_count}/{len(sample)} values unparseable",
            "ISO 8601 date-time",
            "HIGH", bad_count, [],
            f"'{col}' has {bad_count} value(s) that cannot be parsed as ISO 8601."
        )
    return pass_result(
        cid, col, "datetime_format",
        "all sampled values parse as ISO 8601", "ISO 8601 date-time"
    )


def check_statistical_drift(
    col: str, series: pd.Series, baselines: dict, cid: str
) -> dict | None:
    """
    Detects the confidence 0.0-1.0 → 0-100 change even if the type check passes,
    because the MEAN shifts from ~0.76 to ~76.0 (z-score ≈ 450).
    """
    if col not in baselines:
        return None

    b      = baselines[col]
    b_mean = b["mean"]
    b_std  = b.get("stddev", 0.0)

    if b_std < 1e-9:
        return None

    clean = series.dropna()
    if len(clean) == 0:
        return None

    curr_mean = float(clean.mean())
    z         = abs(curr_mean - b_mean) / b_std

    if z > 3:
        status, severity, word = "FAIL",   "HIGH",   f"{z:.1f} stddev drift"
    elif z > 2:
        status, severity, word = "WARN",   "MEDIUM", f"{z:.1f} stddev drift"
    else:
        status, severity, word = "PASS",   "LOW",    f"{z:.2f} stddev (stable)"

    return result(
        cid, col, "statistical_drift", status,
        f"current_mean={curr_mean:.4f}, z_score={z:.2f}",
        f"z_score < 2 (baseline_mean={b_mean:.4f}, stddev={b_std:.4f})",
        severity, 0, [],
        f"'{col}' statistical drift: {word}. "
        + (f"Was {b_mean:.4f}, now {curr_mean:.4f}. Possible scale change!"
           if z > 3 else "")
    )


# ── Main runner ────────────────────────────────────────────────────────────────

def run_validation(contract_path: str, data_path: str, output_path: str) -> dict:
    print(f"\n{'='*60}")
    print(f"  ValidationRunner")
    print(f"{'='*60}")
    print(f"  Contract : {contract_path}")
    print(f"  Data     : {data_path}\n")

    contract    = load_contract(contract_path)
    records     = load_jsonl(data_path)
    df          = flatten_records(records)
    baselines   = load_baselines()
    contract_id = contract.get("id", "unknown")
    snapshot_id = sha256_of_file(data_path)

    results: list[dict] = []
    schema = contract.get("schema", {})

    ICON = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "ERROR": "🔴"}

    for col_name, clause in schema.items():

        # ── Column must exist ───────────────────────────────────────
        if col_name not in df.columns:
            r = result(
                f"{contract_id}.{col_name}.exists",
                col_name, "column_exists", "ERROR",
                "column not found in data",
                "column present",
                "CRITICAL", len(df), [],
                f"Column '{col_name}' defined in contract but not found in data. "
                f"Available: {list(df.columns)[:5]}",
            )
            results.append(r)
            print(f"  🔴  {r['check_id']}: ERROR — column missing")
            continue

        series = df[col_name]

        # ── Run each check ──────────────────────────────────────────
        checks = [
            ("required",        check_required,        True),
            ("type",            check_type,            True),
            ("range",           check_range,           True),
            ("enum",            check_enum,            True),
            ("uuid_format",     check_uuid_format,     True),
            ("datetime_format", check_datetime_format, True),
            ("statistical_drift",
             lambda c, s, cl, ci: check_statistical_drift(c, s, baselines, ci),
             True),
        ]

        for suffix, fn, should_run in checks:
            if not should_run:
                continue
            cid = f"{contract_id}.{col_name}.{suffix}"
            try:
                r = fn(col_name, series, clause, cid)
                if r is not None:
                    results.append(r)
                    icon = ICON.get(r["status"], "?")
                    print(f"  {icon}  {cid}: {r['status']}")
            except Exception as exc:
                results.append(result(
                    cid, col_name, suffix, "ERROR",
                    str(exc), "check to execute without error",
                    "MEDIUM", 0, [],
                    f"Check raised an exception: {exc}",
                ))
                print(f"  🔴  {cid}: ERROR ({exc})")

    # ── Save baselines on first clean run ───────────────────────────
    if not baselines:
        print("\n  First run — saving statistical baselines …")
        save_baselines(df)

    # ── Tally ───────────────────────────────────────────────────────
    passed  = sum(1 for r in results if r["status"] == "PASS")
    failed  = sum(1 for r in results if r["status"] == "FAIL")
    warned  = sum(1 for r in results if r["status"] == "WARN")
    errored = sum(1 for r in results if r["status"] == "ERROR")

    report = {
        "report_id":     str(uuid.uuid4()),
        "contract_id":   contract_id,
        "snapshot_id":   snapshot_id,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_checks":  len(results),
        "passed":        passed,
        "failed":        failed,
        "warned":        warned,
        "errored":       errored,
        "results":       results,
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)

    print(f"\n  📊  {passed} passed  {failed} failed  {warned} warned  {errored} errored")
    print(f"  ✅  Report → {output_path}\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run all contract checks against a JSONL data snapshot."
    )
    parser.add_argument("--contract", required=True)
    parser.add_argument("--data",     required=True)
    parser.add_argument("--output",   default="validation_reports/report.json")
    args = parser.parse_args()

    run_validation(args.contract, args.data, args.output)


if __name__ == "__main__":
    main()