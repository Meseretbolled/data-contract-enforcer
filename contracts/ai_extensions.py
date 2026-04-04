import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

# Load .env file automatically
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ──────────────────────────────────────────────
# Extension 1 — Embedding Drift Detection
# ──────────────────────────────────────────────

def simple_text_embedding(text: str) -> np.ndarray:
    """
    Lightweight deterministic text embedding using character n-gram frequencies.
    Used when OpenAI API is not available.
    Produces a 256-dim vector from character bigram frequencies.
    """
    text = (text or "").lower()[:500]
    vec = np.zeros(256, dtype=np.float32)
    for i in range(len(text) - 1):
        a, b = ord(text[i]) % 16, ord(text[i+1]) % 16
        vec[a * 16 + b] += 1.0
    # Add unigram features in second half
    for ch in text:
        vec[128 + ord(ch) % 128] += 1.0
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec


def embed_sample(texts: list, n: int = 200) -> np.ndarray:
    """
    Embed a sample of texts.
    Uses OpenAI-compatible API (OpenRouter supported via OPENAI_BASE_URL env var).
    Falls back to local n-gram embedder if API unavailable.
    """
    sample = [t for t in texts if t and len(t.strip()) > 5][:n]
    if not sample:
        return np.zeros((1, 256))

    # Try OpenAI-compatible API (works with OpenRouter via OPENAI_BASE_URL)
    api_key  = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1")
    model    = os.getenv("OPENAI_MODEL", "openai/text-embedding-3-small")

    if not api_key:
        print(f"    ⚠  No OPENAI_API_KEY — falling back to local n-gram embedder")
        return np.array([simple_text_embedding(t) for t in sample])

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url=base_url)
        print(f"    🤖  Calling {model} via OpenRouter ({len(sample)} texts in small batches) ...")

        # Send in batches of 10 to avoid timeout on large requests
        all_vecs = []
        batch_size = 10
        for i in range(0, len(sample), batch_size):
            batch = sample[i:i + batch_size]
            resp  = client.embeddings.create(input=batch, model=model)
            all_vecs.extend([e.embedding for e in resp.data])
            print(f"    ✅  Batch {i//batch_size + 1}/{(len(sample)-1)//batch_size + 1} — {len(batch)} texts")

        vecs = np.array(all_vecs)
        print(f"    ✅  All done — {vecs.shape[1]}-dimensional vectors ({len(all_vecs)} total)")
        return vecs

    except Exception as e:
        print(f"    ⚠  Embedding API failed: {str(e)[:80]}")
        print(f"    Falling back to local n-gram embedder")
        return np.array([simple_text_embedding(t) for t in sample])


def check_embedding_drift(
    texts: list,
    baseline_path: str = "schema_snapshots/embedding_baselines.npz",
    threshold: float = 0.15,
) -> dict:
    """
    Detect semantic drift in text data by comparing current centroid
    to stored baseline centroid using cosine distance.

    threshold=0.15: conservative starting point
      < 0.08: stable (normal variance)
      0.08-0.15: mild drift (monitor)
      > 0.15: significant drift (investigate)
      > 0.25: model-breaking drift (alert)
    """
    if not texts:
        return {
            "status": "ERROR",
            "message": "No texts provided for embedding drift check",
            "drift_score": None,
        }

    vecs    = embed_sample(texts, n=200)
    centroid = vecs.mean(axis=0)
    baseline_path_obj = Path(baseline_path)

    if not baseline_path_obj.exists():
        baseline_path_obj.parent.mkdir(parents=True, exist_ok=True)
        np.savez(str(baseline_path_obj), centroid=centroid)
        return {
            "status":       "BASELINE_SET",
            "drift_score":  0.0,
            "threshold":    threshold,
            "sample_size":  len(vecs),
            "message":      "Baseline established from current data. Run again to detect drift.",
            "interpretation": "First run — baseline saved.",
        }

    baseline = np.load(str(baseline_path_obj))["centroid"]

    # Cosine similarity → cosine distance
    sim   = np.dot(centroid, baseline) / (
        np.linalg.norm(centroid) * np.linalg.norm(baseline) + 1e-9
    )
    drift = float(1.0 - sim)

    if drift > threshold:
        status = "FAIL"
        interpretation = (
            f"Semantic content has shifted significantly (drift={drift:.4f} > threshold={threshold}). "
            "This may indicate a domain shift, data quality issue, or model change."
        )
    elif drift > threshold * 0.6:
        status = "WARN"
        interpretation = (
            f"Drift approaching threshold (drift={drift:.4f}). Monitor closely."
        )
    else:
        status = "PASS"
        interpretation = f"Semantic content is stable (drift={drift:.4f} < threshold={threshold})."

    return {
        "status":         status,
        "drift_score":    round(drift, 4),
        "threshold":      threshold,
        "cosine_similarity": round(float(sim), 4),
        "sample_size":    len(vecs),
        "interpretation": interpretation,
        "baseline_path":  str(baseline_path_obj),
    }


# ──────────────────────────────────────────────
# Extension 2 — Prompt Input Schema Validation
# ──────────────────────────────────────────────

# JSON Schema for Week 3 extraction prompt inputs
WEEK3_PROMPT_SCHEMA = {
    "required": ["doc_id", "source_path", "source_hash", "extraction_model"],
    "types": {
        "doc_id":           str,
        "source_path":      str,
        "source_hash":      str,
        "extraction_model": str,
    },
    "min_length": {
        "doc_id":           10,
        "source_path":      3,
        "source_hash":      16,
        "extraction_model": 3,
    },
    "patterns": {
        "extraction_model": lambda v: v.startswith("claude") or v.startswith("gpt"),
    }
}

# JSON Schema for Week 2 verdict prompt inputs
WEEK2_PROMPT_SCHEMA = {
    "required": ["verdict_id", "target_ref", "rubric_id", "overall_verdict"],
    "types": {
        "verdict_id":      str,
        "target_ref":      str,
        "rubric_id":       str,
        "overall_verdict": str,
    },
    "enum": {
        "overall_verdict": {"PASS", "FAIL", "WARN"},
    }
}


def validate_record_against_schema(record: dict, schema: dict) -> list:
    """Validate a single record against a prompt input schema. Returns list of errors."""
    errors = []

    # Required fields
    for field in schema.get("required", []):
        if field not in record or record[field] is None:
            errors.append(f"Missing required field: '{field}'")

    # Type checks
    for field, expected_type in schema.get("types", {}).items():
        if field in record and record[field] is not None:
            if not isinstance(record[field], expected_type):
                errors.append(f"Type error: '{field}' expected {expected_type.__name__}, "
                              f"got {type(record[field]).__name__}")

    # Min length checks
    for field, min_len in schema.get("min_length", {}).items():
        if field in record and isinstance(record[field], str):
            if len(record[field]) < min_len:
                errors.append(f"Too short: '{field}' min length {min_len}, "
                              f"got {len(record[field])}")

    # Enum checks
    for field, allowed in schema.get("enum", {}).items():
        if field in record and record[field] not in allowed:
            errors.append(f"Invalid enum: '{field}' must be one of {allowed}, "
                          f"got '{record[field]}'")

    # Pattern checks
    for field, pattern_fn in schema.get("patterns", {}).items():
        if field in record and record[field] is not None:
            if not pattern_fn(record[field]):
                errors.append(f"Pattern violation: '{field}' value '{record[field]}' "
                              f"failed pattern check")

    return errors


def validate_prompt_inputs(
    records: list,
    schema: dict,
    quarantine_path: str = "outputs/quarantine/quarantine.jsonl",
) -> dict:
    """
    Validate all records against a prompt input schema.
    Non-conforming records go to quarantine — never silently dropped.
    """
    valid       = []
    quarantined = []

    for record in records:
        errors = validate_record_against_schema(record, schema)
        if errors:
            quarantined.append({
                "record": {k: str(v)[:100] for k, v in record.items()},
                "errors": errors,
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
            })
        else:
            valid.append(record)

    if quarantined:
        qpath = Path(quarantine_path)
        qpath.parent.mkdir(parents=True, exist_ok=True)
        with open(qpath, "a") as f:
            for q in quarantined:
                f.write(json.dumps(q) + "\n")

    quarantine_rate = len(quarantined) / max(len(records), 1)

    return {
        "status":           "FAIL" if quarantine_rate > 0.05 else "PASS",
        "total_records":    len(records),
        "valid":            len(valid),
        "quarantined":      len(quarantined),
        "quarantine_rate":  round(quarantine_rate, 4),
        "quarantine_path":  quarantine_path if quarantined else None,
        "interpretation": (
            f"⚠  {len(quarantined)} records quarantined ({quarantine_rate:.1%}) — "
            "check outputs/quarantine/ for details."
        ) if quarantined else "✅ All records passed prompt input validation.",
    }


# ──────────────────────────────────────────────
# Extension 3 — LLM Output Schema Violation Rate
# ──────────────────────────────────────────────

def check_output_violation_rate(
    outputs: list,
    expected_field: str,
    expected_values: set,
    baseline_rate: float = None,
    warn_threshold: float = 0.02,
) -> dict:
    """
    Track the schema violation rate for LLM-generated structured outputs.
    A rising rate signals prompt degradation or model behaviour change.
    """
    total      = len(outputs)
    violations = sum(
        1 for o in outputs
        if o.get(expected_field) not in expected_values
    )
    rate = violations / max(total, 1)

    trend = "unknown"
    if baseline_rate is not None:
        if rate > baseline_rate * 1.5:
            trend = "rising"
        elif rate < baseline_rate * 0.5:
            trend = "falling"
        else:
            trend = "stable"

    if trend == "rising" or rate > warn_threshold:
        status = "WARN"
    else:
        status = "PASS"

    return {
        "status":           status,
        "total_outputs":    total,
        "schema_violations":violations,
        "violation_rate":   round(rate, 4),
        "baseline_rate":    baseline_rate,
        "trend":            trend,
        "warn_threshold":   warn_threshold,
        "interpretation": (
            f"⚠  Violation rate {rate:.1%} is {'rising' if trend=='rising' else 'above threshold'}. "
            "Check prompt template or model version."
        ) if status == "WARN" else
        f"✅ Output schema violation rate {rate:.1%} is within acceptable bounds.",
    }


def check_trace_schema(traces: list) -> dict:
    """
    Validate LangSmith traces against the trace_record schema.
    Checks: end_time > start_time, run_type enum, required fields.
    """
    valid_run_types = {"llm", "chain", "tool", "retriever", "embedding"}
    required_fields = ["id", "name", "run_type", "start_time", "end_time"]

    total       = len(traces)
    violations  = []

    for t in traces:
        errs = []

        # Required fields
        for field in required_fields:
            if not t.get(field):
                errs.append(f"missing_{field}")

        # run_type enum
        rt = t.get("run_type", "")
        if rt and rt not in valid_run_types:
            errs.append(f"invalid_run_type:{rt}")

        # end_time >= start_time
        st = t.get("start_time")
        et = t.get("end_time")
        if st and et:
            try:
                if et < st:
                    errs.append("end_time_before_start_time")
            except Exception:
                pass

        if errs:
            violations.append({"trace_id": t.get("id", "unknown"), "errors": errs})

    rate = len(violations) / max(total, 1)

    return {
        "status":          "FAIL" if rate > 0.1 else ("WARN" if rate > 0.02 else "PASS"),
        "total_traces":    total,
        "violations":      len(violations),
        "violation_rate":  round(rate, 4),
        "sample_violations": violations[:3],
        "interpretation":  (
            f"✅ {total} traces checked — {len(violations)} schema violations ({rate:.1%})."
        ),
    }


# ──────────────────────────────────────────────
# Main runner
# ──────────────────────────────────────────────

def load_jsonl(path: str) -> list:
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except Exception:
                    pass
    return records


def main():
    parser = argparse.ArgumentParser(description="AI Contract Extensions")
    parser.add_argument("--extractions", required=True,
                        help="Path to Week 3 extractions JSONL")
    parser.add_argument("--verdicts",    required=True,
                        help="Path to Week 2 verdicts JSONL")
    parser.add_argument("--traces",      default="outputs/traces/runs.jsonl",
                        help="Path to LangSmith traces JSONL")
    parser.add_argument("--output",      required=True,
                        help="Output path for AI extensions report JSON")
    parser.add_argument("--baseline",    default="schema_snapshots/embedding_baselines.npz",
                        help="Path to embedding baseline file")
    args = parser.parse_args()

    print("=" * 60)
    print("  AI Contract Extensions")
    print("=" * 60)

    results = {
        "run_id":     str(uuid.uuid4()),
        "run_at":     datetime.now(timezone.utc).isoformat(),
        "extensions": {},
    }

    # ── Extension 1: Embedding Drift ──
    print("\n  Extension 1 — Embedding Drift Detection")
    extractions = load_jsonl(args.extractions)
    texts = []
    for rec in extractions:
        for fact in rec.get("extracted_facts", []):
            t = fact.get("text", "")
            if t:
                texts.append(t)

    print(f"    Loaded {len(texts)} fact texts from {len(extractions)} extraction records")
    drift_result = check_embedding_drift(texts, baseline_path=args.baseline)
    results["extensions"]["embedding_drift"] = drift_result
    icon = "✅" if drift_result["status"] in ("PASS", "BASELINE_SET") else (
           "⚠️" if drift_result["status"] == "WARN" else "❌")
    print(f"    {icon}  Status: {drift_result['status']} | "
          f"Drift score: {drift_result.get('drift_score', 'N/A')} | "
          f"{drift_result.get('interpretation', '')[:80]}")

    # ── Extension 2: Prompt Input Schema Validation ──
    print("\n  Extension 2 — Prompt Input Schema Validation")

    # Week 3 extractions
    print(f"    Validating {len(extractions)} Week 3 extraction records...")
    prompt_result_w3 = validate_prompt_inputs(
        extractions, WEEK3_PROMPT_SCHEMA,
        quarantine_path="outputs/quarantine/week3_quarantine.jsonl"
    )
    results["extensions"]["prompt_input_validation_week3"] = prompt_result_w3
    icon = "✅" if prompt_result_w3["status"] == "PASS" else "❌"
    print(f"    {icon}  Week 3: {prompt_result_w3['valid']} valid, "
          f"{prompt_result_w3['quarantined']} quarantined "
          f"({prompt_result_w3['quarantine_rate']:.1%} rate)")

    # Week 2 verdicts
    verdicts = load_jsonl(args.verdicts)
    print(f"    Validating {len(verdicts)} Week 2 verdict records...")
    prompt_result_w2 = validate_prompt_inputs(
        verdicts, WEEK2_PROMPT_SCHEMA,
        quarantine_path="outputs/quarantine/week2_quarantine.jsonl"
    )
    results["extensions"]["prompt_input_validation_week2"] = prompt_result_w2
    icon = "✅" if prompt_result_w2["status"] == "PASS" else "❌"
    print(f"    {icon}  Week 2: {prompt_result_w2['valid']} valid, "
          f"{prompt_result_w2['quarantined']} quarantined "
          f"({prompt_result_w2['quarantine_rate']:.1%} rate)")

    # ── Extension 3: LLM Output Schema Violation Rate ──
    print("\n  Extension 3 — LLM Output Schema Violation Rate")

    # Week 2 verdict enum check
    print(f"    Checking overall_verdict enum in {len(verdicts)} verdict records...")
    verdict_violation = check_output_violation_rate(
        verdicts,
        expected_field="overall_verdict",
        expected_values={"PASS", "FAIL", "WARN"},
        baseline_rate=0.0,
        warn_threshold=0.02,
    )
    results["extensions"]["output_violation_rate_verdicts"] = verdict_violation
    icon = "✅" if verdict_violation["status"] == "PASS" else "⚠️"
    print(f"    {icon}  Verdict enum violation rate: "
          f"{verdict_violation['violation_rate']:.1%} | "
          f"Trend: {verdict_violation['trend']} | "
          f"{verdict_violation['interpretation'][:70]}")

    # Trace schema check
    if Path(args.traces).exists():
        traces = load_jsonl(args.traces)
        print(f"\n    Checking trace schema for {len(traces)} LangSmith traces...")
        trace_result = check_trace_schema(traces)
        results["extensions"]["trace_schema_check"] = trace_result
        icon = "✅" if trace_result["status"] == "PASS" else "⚠️"
        print(f"    {icon}  Trace schema: {trace_result['interpretation'][:80]}")
    else:
        print(f"\n    ⚠  Traces file not found: {args.traces}")
        results["extensions"]["trace_schema_check"] = {
            "status": "SKIPPED",
            "message": f"Traces file not found: {args.traces}"
        }

    # ── Summary ──
    statuses = [
        v.get("status") for v in results["extensions"].values()
    ]
    overall = (
        "FAIL" if "FAIL" in statuses else
        "WARN" if "WARN" in statuses else
        "PASS"
    )
    results["overall_status"] = overall

    print("\n  " + "─" * 40)
    print(f"  Overall AI Contract Status: {overall}")
    for ext_name, ext_result in results["extensions"].items():
        icon = ("✅" if ext_result.get("status") in ("PASS", "BASELINE_SET", "SKIPPED")
                else "⚠️" if ext_result.get("status") == "WARN"
                else "❌")
        print(f"  {icon}  {ext_name}: {ext_result.get('status', 'UNKNOWN')}")

    # Write output
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n  ✅  Report → {args.output}")
    print("=" * 60)


if __name__ == "__main__":
    main()