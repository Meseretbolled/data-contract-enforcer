"""
contracts/report_generator.py — EnforcerReport Generator
=========================================================
Auto-generates the Enforcer Report from live validation data.
Reads from validation_reports/, violation_log/, and ai_metrics.

Usage
-----
python contracts/report_generator.py \
    --output enforcer_report/report_data.json
"""

import argparse
import json
import glob
from datetime import datetime, timezone, timedelta
from pathlib import Path

import yaml


# ──────────────────────────────────────────────
# Severity deductions for health score
# ──────────────────────────────────────────────
SEVERITY_DEDUCTIONS = {
    "CRITICAL": 20,
    "HIGH":     10,
    "MEDIUM":    5,
    "LOW":       1,
    "WARNING":   2,
}


# ──────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────

def load_all_validation_reports(reports_dir: str = "validation_reports") -> list:
    """Load all validation report JSONs from the reports directory."""
    reports = []
    for path in glob.glob(f"{reports_dir}/*.json"):
        # Skip schema evolution and ai_extensions reports
        if any(x in path for x in ["schema_evolution", "ai_extensions"]):
            continue
        try:
            with open(path) as f:
                reports.append(json.load(f))
        except Exception:
            pass
    return reports


def load_violation_log(violation_path: str = "violation_log/violations.jsonl") -> list:
    """Load all violation log entries."""
    violations = []
    vpath = Path(violation_path)
    if vpath.exists():
        with open(vpath) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        violations.append(json.loads(line))
                    except Exception:
                        pass
    return violations


def load_ai_extensions(ai_path: str = "validation_reports/ai_extensions.json") -> dict:
    """Load AI extensions report."""
    if Path(ai_path).exists():
        with open(ai_path) as f:
            return json.load(f)
    return {}


def load_registry(registry_path: str = "contract_registry/subscriptions.yaml") -> dict:
    """Load contract registry."""
    if Path(registry_path).exists():
        with open(registry_path) as f:
            return yaml.safe_load(f)
    return {}


def load_schema_evolution(evo_path: str = "validation_reports/schema_evolution_all.json") -> dict:
    """Load schema evolution report."""
    if Path(evo_path).exists():
        with open(evo_path) as f:
            return json.load(f)
    return {}


# ──────────────────────────────────────────────
# Health score computation
# ──────────────────────────────────────────────

def compute_health_score(reports: list) -> tuple:
    """
    Compute data health score 0-100.
    Formula: 100 - sum(deductions per FAIL/ERROR by severity)
    Returns (score, all_failures)
    """
    all_failures = []
    for report in reports:
        for result in report.get("results", []):
            if result.get("status") in ("FAIL", "ERROR"):
                all_failures.append({
                    **result,
                    "contract_id": report.get("contract_id", "unknown"),
                    "report_file": report.get("report_id", "unknown"),
                })

    score = 100
    for failure in all_failures:
        severity  = failure.get("severity", "LOW")
        deduction = SEVERITY_DEDUCTIONS.get(severity, 1)
        score    -= deduction

    return max(0, min(100, score)), all_failures


# ──────────────────────────────────────────────
# Plain language violation descriptions
# ──────────────────────────────────────────────

def plain_language_violation(result: dict, registry: dict) -> str:
    """
    Convert a technical violation result into plain English.
    Non-engineers must be able to read this and know what to do.
    """
    contract_id  = result.get("contract_id", "unknown")
    column_name  = result.get("column_name", result.get("check_id", "unknown"))
    check_type   = result.get("check_type", "check")
    actual       = result.get("actual_value", "unknown")
    expected     = result.get("expected", "unknown")
    records_fail = result.get("records_failing", "unknown")
    severity     = result.get("severity", "UNKNOWN")

    # Find affected subscribers from registry
    subscribers = []
    for sub in registry.get("subscriptions", []):
        if sub.get("contract_id") == contract_id:
            for bf in sub.get("breaking_fields", []):
                if bf.get("field", "").replace(".", "_") in column_name.replace(".", "_"):
                    subscribers.append(sub["subscriber_id"])
                    break

    sub_str = (", ".join(subscribers) if subscribers
               else "check contract_registry/subscriptions.yaml for affected systems")

    return (
        f"[{severity}] The '{column_name}' field in '{contract_id}' failed its "
        f"{check_type} check. "
        f"Found: {actual}. Expected: {expected}. "
        f"Affecting {records_fail} record(s). "
        f"Downstream systems at risk: {sub_str}."
    )


# ──────────────────────────────────────────────
# Recommended actions
# ──────────────────────────────────────────────

def generate_recommendations(all_failures: list, ai_report: dict, evo_report: dict) -> list:
    """
    Generate 3 prioritised, specific recommended actions.
    Each action must be specific enough to open a ticket without follow-up questions.
    """
    recommendations = []

    # Priority 1: Critical failures
    critical = [f for f in all_failures if f.get("severity") == "CRITICAL"]
    if critical:
        top = critical[0]
        contract_id = top.get("contract_id", "unknown")
        column      = top.get("column_name", "unknown")
        actual      = top.get("actual_value", "unknown")

        # Map contract to source file
        source_files = {
            "week3-document-refinery-extractions": "outputs/week3/extractions.jsonl (produced by src/week3/extractor.py)",
            "week5-event-records":                 "outputs/week5/events.jsonl (produced by apex-ledger event simulator)",
            "week2-verdict-records":               "outputs/week2/verdicts.jsonl (produced by automaton-auditor)",
            "week4-lineage-snapshots":             "outputs/week4/lineage_snapshots.jsonl (produced by brownfield-cartographer)",
        }
        source = source_files.get(contract_id, f"outputs/{contract_id.split('-')[0]}/")

        recommendations.append(
            f"URGENT: Fix CRITICAL violation in {contract_id}. "
            f"Field '{column}' has value {actual} but contract requires {top.get('expected', 'see contract')}. "
            f"Source: {source}. "
            f"Run: python contracts/runner.py --contract generated_contracts/{contract_id}.yaml "
            f"--data outputs/ --mode ENFORCE after fix."
        )

    # Priority 2: Add CI enforcement
    if all_failures:
        recommendations.append(
            "Add contracts/runner.py as a required CI step before any data pipeline deployment. "
            "Insert: 'python contracts/runner.py --contract generated_contracts/<contract>.yaml "
            "--data outputs/<week>/ --mode ENFORCE --output validation_reports/ci_check.json' "
            "into your GitHub Actions workflow. This prevents violations from reaching production."
        )
    else:
        recommendations.append(
            "All contracts currently passing. Schedule monthly baseline refresh: "
            "run 'python contracts/generator.py' on fresh data for each contract "
            "to update statistical thresholds. Set calendar reminder for first of each month."
        )

    # Priority 3: Schema evolution or AI risk
    ai_extensions = ai_report.get("extensions", {})
    drift = ai_extensions.get("embedding_drift", {})
    if drift.get("status") == "FAIL":
        recommendations.append(
            f"URGENT: Embedding drift detected (score={drift.get('drift_score')}). "
            "The semantic content of extracted facts has shifted significantly. "
            "Investigate: (1) check if extraction model changed, "
            "(2) compare recent source documents to baseline corpus, "
            "(3) run python contracts/ai_extensions.py to get current drift score, "
            "(4) if intentional domain shift, re-establish baseline by deleting "
            "schema_snapshots/embedding_baselines.npz and re-running ai_extensions.py."
        )
    elif drift.get("status") == "BASELINE_SET":
        recommendations.append(
            "Embedding drift baseline has been established. Run "
            "'python contracts/ai_extensions.py --extractions outputs/week3/extractions.jsonl "
            "--verdicts outputs/week2/verdicts.jsonl --output validation_reports/ai_extensions.json' "
            "again on next data batch to detect any semantic drift from this baseline."
        )
    else:
        evo_breaking = evo_report.get("total_breaking", 0)
        if evo_breaking > 0:
            recommendations.append(
                f"Schema evolution detected {evo_breaking} breaking change(s). "
                "Review validation_reports/schema_evolution_all.json for migration checklists. "
                "Notify all registry subscribers listed in contract_registry/subscriptions.yaml "
                "before deploying any schema changes to production."
            )
        else:
            recommendations.append(
                "All AI contract checks passing. Consider expanding embedding drift monitoring "
                "to Week 5 event payloads by adding event payload text fields to ai_extensions.py. "
                "This will catch semantic drift in loan decision rationale text."
            )

    return recommendations[:3]


# ──────────────────────────────────────────────
# AI risk assessment
# ──────────────────────────────────────────────

def ai_risk_assessment(ai_report: dict) -> dict:
    """Generate plain-language AI risk assessment from extension results."""
    extensions = ai_report.get("extensions", {})

    drift        = extensions.get("embedding_drift", {})
    verdict_viol = extensions.get("output_violation_rate_verdicts", {})
    trace_check  = extensions.get("trace_schema_check", {})

    drift_score    = drift.get("drift_score", "N/A")
    drift_status   = drift.get("status", "UNKNOWN")
    violation_rate = verdict_viol.get("violation_rate", "N/A")
    viol_trend     = verdict_viol.get("trend", "unknown")
    trace_status   = trace_check.get("status", "UNKNOWN")

    overall_ai_status = ai_report.get("overall_status", "UNKNOWN")

    narrative = []

    if drift_status == "BASELINE_SET":
        narrative.append(
            "Embedding drift baseline established on current data. "
            "First drift measurement will be available on the next run."
        )
    elif drift_status == "PASS":
        narrative.append(
            f"Semantic content of extracted facts is stable (drift score: {drift_score}). "
            "No evidence of domain shift or model behaviour change."
        )
    elif drift_status == "FAIL":
        narrative.append(
            f"⚠ ALERT: Significant embedding drift detected (score: {drift_score}). "
            "AI system may be processing data from a different domain than the baseline."
        )

    if isinstance(violation_rate, float):
        if violation_rate == 0.0:
            narrative.append(
                "LLM output schema violation rate is 0.0% — "
                "all verdict records conform to expected PASS/FAIL/WARN enum."
            )
        else:
            narrative.append(
                f"LLM output schema violation rate: {violation_rate:.1%} (trend: {viol_trend}). "
                "Monitor for rising trend which may indicate prompt or model degradation."
            )

    if trace_status == "PASS":
        total_t = trace_check.get("total_traces", 0)
        narrative.append(
            f"LangSmith trace schema check passed: {total_t} traces validated, "
            "all required fields present and timestamps valid."
        )

    return {
        "overall_status":       overall_ai_status,
        "embedding_drift":      {"score": drift_score, "status": drift_status},
        "output_violation_rate":{"rate": violation_rate, "trend": viol_trend},
        "trace_schema":         {"status": trace_status},
        "narrative":            " ".join(narrative),
    }


# ──────────────────────────────────────────────
# Schema changes summary
# ──────────────────────────────────────────────

def schema_changes_summary(evo_report: dict) -> list:
    """Summarise schema changes in plain language."""
    summaries = []
    for report in evo_report.get("reports", []):
        contract_id = report.get("contract_id", "unknown")
        verdict     = report.get("compatibility_verdict", "UNKNOWN")
        breaking    = report.get("breaking_changes", 0)
        total       = report.get("total_changes", 0)

        if verdict == "INSUFFICIENT_SNAPSHOTS":
            continue
        if total == 0:
            continue

        action_needed = "No action required." if breaking == 0 else (
            f"BLOCK DEPLOY until migration checklist complete. "
            f"See validation_reports/schema_evolution_all.json for details."
        )

        summaries.append({
            "contract_id":         contract_id,
            "compatibility":       verdict,
            "total_changes":       total,
            "breaking_changes":    breaking,
            "action_required":     action_needed,
            "plain_summary": (
                f"Contract '{contract_id}': {total} schema change(s) detected "
                f"({breaking} breaking). Verdict: {verdict}. {action_needed}"
            ),
        })

    return summaries


# ──────────────────────────────────────────────
# Main report generator
# ──────────────────────────────────────────────

def generate_report(
    reports_dir:    str = "validation_reports",
    violation_path: str = "violation_log/violations.jsonl",
    ai_path:        str = "validation_reports/ai_extensions.json",
    registry_path:  str = "contract_registry/subscriptions.yaml",
    evo_path:       str = "validation_reports/schema_evolution_all.json",
    output_path:    str = "enforcer_report/report_data.json",
):
    print("=" * 60)
    print("  EnforcerReport Generator")
    print("=" * 60)

    # Load all data sources
    reports    = load_all_validation_reports(reports_dir)
    violations = load_violation_log(violation_path)
    ai_report  = load_ai_extensions(ai_path)
    registry   = load_registry(registry_path)
    evo_report = load_schema_evolution(evo_path)

    print(f"  Loaded {len(reports)} validation report(s)")
    print(f"  Loaded {len(violations)} violation log entry(s)")
    print(f"  AI extensions: {ai_report.get('overall_status', 'not found')}")

    # Compute health score
    score, all_failures = compute_health_score(reports)
    print(f"  Data health score: {score}/100")

    # Severity counts
    sev_counts = {}
    for f in all_failures:
        sev = f.get("severity", "UNKNOWN")
        sev_counts[sev] = sev_counts.get(sev, 0) + 1

    # Top 3 violations in plain language
    severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "WARNING", "UNKNOWN", "ERROR"]
    sorted_failures = sorted(
        all_failures,
        key=lambda x: severity_order.index(x.get("severity", "UNKNOWN"))
        if x.get("severity", "UNKNOWN") in severity_order else 99
    )
    top3_violations = [
        plain_language_violation(f, registry)
        for f in sorted_failures[:3]
    ]

    if not top3_violations:
        top3_violations = [
            "No violations detected in current validation run. "
            "All contracts passing. System operating within defined parameters."
        ]

    # Health narrative
    if score >= 90:
        health_narrative = (
            f"Score {score}/100 — All monitored data systems are operating within "
            "contract parameters. No critical issues detected."
        )
    elif score >= 70:
        health_narrative = (
            f"Score {score}/100 — System is generally healthy but has "
            f"{sev_counts.get('HIGH', 0)} high-severity and "
            f"{sev_counts.get('MEDIUM', 0)} medium-severity issues requiring attention."
        )
    else:
        health_narrative = (
            f"Score {score}/100 — ATTENTION REQUIRED. "
            f"{sev_counts.get('CRITICAL', 0)} critical violation(s) detected. "
            "Downstream AI systems may be consuming corrupted data. Immediate action needed."
        )

    # Generate all sections
    recommendations = generate_recommendations(all_failures, ai_report, evo_report)
    ai_risk         = ai_risk_assessment(ai_report)
    schema_changes  = schema_changes_summary(evo_report)

    # Period
    now   = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    period = f"{week_ago.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}"

    # Contracts summary
    contracts_summary = []
    for report in reports:
        contracts_summary.append({
            "contract_id":   report.get("contract_id", "unknown"),
            "total_checks":  report.get("total_checks", 0),
            "passed":        report.get("passed", 0),
            "failed":        report.get("failed", 0),
            "warned":        report.get("warned", 0),
            "errored":       report.get("errored", 0),
        })

    # Full report
    report_data = {
        "report_metadata": {
            "generated_at":     now.isoformat(),
            "generated_by":     "contracts/report_generator.py",
            "period":           period,
            "auto_generated":   True,
            "note": "This report is machine-generated from live validation data. Do not edit manually.",
        },

        # Section 1: Data Health Score
        "data_health_score":  score,
        "health_narrative":   health_narrative,
        "violations_by_severity": sev_counts,
        "total_violations":   len(all_failures),
        "total_violations_logged": len(violations),

        # Section 2: Violations this week
        "top_violations":     top3_violations,
        "contracts_summary":  contracts_summary,

        # Section 3: Schema changes
        "schema_changes_detected": schema_changes,
        "total_breaking_changes":  evo_report.get("total_breaking", 0),

        # Section 4: AI system risk
        "ai_risk_assessment": ai_risk,

        # Section 5: Recommended actions
        "recommended_actions": recommendations,

        # Raw data for downstream use (Week 8 Sentinel)
        "raw": {
            "all_failures":    all_failures,
            "violation_log":   violations,
            "registry_subscriptions": len(registry.get("subscriptions", [])),
            "contracts_monitored":    len(reports),
        }
    }

    # Write output
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report_data, f, indent=2, default=str)

    print(f"\n  ✅  Report generated → {output_path}")
    print(f"  📊  Health score     : {score}/100")
    print(f"  ⚠   Violations       : {len(all_failures)} ({sev_counts})")
    print(f"  🔀  Schema changes   : {evo_report.get('total_changes', 0)} "
          f"({evo_report.get('total_breaking', 0)} breaking)")
    print(f"  🤖  AI status        : {ai_report.get('overall_status', 'N/A')}")
    print("=" * 60)

    return report_data


def main():
    parser = argparse.ArgumentParser(description="EnforcerReport Generator")
    parser.add_argument("--reports-dir",    default="validation_reports")
    parser.add_argument("--violations",     default="violation_log/violations.jsonl")
    parser.add_argument("--ai-report",      default="validation_reports/ai_extensions.json")
    parser.add_argument("--registry",       default="contract_registry/subscriptions.yaml")
    parser.add_argument("--evolution",      default="validation_reports/schema_evolution_all.json")
    parser.add_argument("--output",         default="enforcer_report/report_data.json")
    args = parser.parse_args()

    generate_report(
        reports_dir    = args.reports_dir,
        violation_path = args.violations,
        ai_path        = args.ai_report,
        registry_path  = args.registry,
        evo_path       = args.evolution,
        output_path    = args.output,
    )


if __name__ == "__main__":
    main()
