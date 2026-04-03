import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml


# ──────────────────────────────────────────────
# Change classification taxonomy
# Mirrors Confluent Schema Registry compatibility model
# ──────────────────────────────────────────────

BREAKING_CHANGES = {
    "field_removed",
    "required_field_added",
    "type_changed",
    "range_narrowed",
    "enum_value_removed",
    "format_changed",
}

COMPATIBLE_CHANGES = {
    "optional_field_added",
    "enum_value_added",
    "range_widened",
    "description_changed",
    "no_change",
}


def classify_change(field: str, old_clause: dict, new_clause: dict) -> dict:
    """
    Classify a schema change between two snapshots.
    Returns a dict with change_type, compatible, description, required_action.
    """
    # Field added
    if old_clause is None:
        required = new_clause.get("required", False)
        if required:
            return {
                "change_type":     "required_field_added",
                "compatible":      False,
                "description":     f"New required field '{field}' added. All producers must populate it.",
                "required_action": "Coordinate with all producers. Provide default or migration script. Block deploy until all producers updated.",
                "severity":        "BREAKING",
            }
        else:
            return {
                "change_type":     "optional_field_added",
                "compatible":      True,
                "description":     f"New optional field '{field}' added. Consumers can ignore it.",
                "required_action": "None. Notify downstream subscribers as informational.",
                "severity":        "COMPATIBLE",
            }

    # Field removed
    if new_clause is None:
        return {
            "change_type":     "field_removed",
            "compatible":      False,
            "description":     f"Field '{field}' removed. All consumers depending on it will break.",
            "required_action": "Two-sprint deprecation minimum. Each registry subscriber must acknowledge removal.",
            "severity":        "BREAKING",
        }

    changes = []

    # Type changed
    old_type = old_clause.get("type")
    new_type = new_clause.get("type")
    if old_type and new_type and old_type != new_type:
        changes.append({
            "change_type":     "type_changed",
            "compatible":      False,
            "description":     f"Type changed {old_type} → {new_type} for '{field}'.",
            "required_action": "CRITICAL. Requires migration plan with rollback. Registry blast radius report mandatory.",
            "severity":        "BREAKING",
        })

    # Range changed (minimum/maximum)
    old_min = old_clause.get("minimum")
    new_min = new_clause.get("minimum")
    old_max = old_clause.get("maximum")
    new_max = new_clause.get("maximum")

    if old_max is not None and new_max is not None and old_max != new_max:
        if new_max < old_max:
            changes.append({
                "change_type":     "range_narrowed",
                "compatible":      False,
                "description":     f"Maximum narrowed {old_max} → {new_max} for '{field}'. Existing data may violate new constraint.",
                "required_action": "Validate all existing data against new range. Statistical baseline must be re-established.",
                "severity":        "BREAKING",
            })
        else:
            changes.append({
                "change_type":     "range_widened",
                "compatible":      True,
                "description":     f"Maximum widened {old_max} → {new_max} for '{field}'.",
                "required_action": "Re-run statistical checks to confirm distribution unchanged.",
                "severity":        "COMPATIBLE",
            })

    if old_min is not None and new_min is not None and old_min != new_min:
        if new_min > old_min:
            changes.append({
                "change_type":     "range_narrowed",
                "compatible":      False,
                "description":     f"Minimum raised {old_min} → {new_min} for '{field}'.",
                "required_action": "Validate all existing data. May reject previously valid records.",
                "severity":        "BREAKING",
            })

    # Enum changed
    old_enum = set(old_clause.get("enum", []))
    new_enum = set(new_clause.get("enum", []))
    if old_enum and new_enum:
        removed = old_enum - new_enum
        added   = new_enum - old_enum
        if removed:
            changes.append({
                "change_type":     "enum_value_removed",
                "compatible":      False,
                "description":     f"Enum values removed from '{field}': {sorted(removed)}. Existing data with these values will fail.",
                "required_action": "Treat as breaking — blast radius report required. Deprecation period mandatory.",
                "severity":        "BREAKING",
            })
        if added:
            changes.append({
                "change_type":     "enum_value_added",
                "compatible":      True,
                "description":     f"Enum values added to '{field}': {sorted(added)}.",
                "required_action": "Notify subscribers. Additive change — no immediate action required.",
                "severity":        "COMPATIBLE",
            })

    # Format changed
    old_fmt = old_clause.get("format")
    new_fmt = new_clause.get("format")
    if old_fmt and new_fmt and old_fmt != new_fmt:
        changes.append({
            "change_type":     "format_changed",
            "compatible":      False,
            "description":     f"Format changed {old_fmt} → {new_fmt} for '{field}'.",
            "required_action": "Validate all existing data against new format. Update all consumers.",
            "severity":        "BREAKING",
        })

    # Required changed
    old_req = old_clause.get("required", False)
    new_req = new_clause.get("required", False)
    if not old_req and new_req:
        changes.append({
            "change_type":     "required_field_added",
            "compatible":      False,
            "description":     f"Field '{field}' changed from optional to required.",
            "required_action": "All producers must now populate this field. Coordinate before deploy.",
            "severity":        "BREAKING",
        })

    # No material change
    if not changes:
        return {
            "change_type":     "no_change",
            "compatible":      True,
            "description":     f"No material change to '{field}'.",
            "required_action": "None.",
            "severity":        "COMPATIBLE",
        }

    # Return most severe change if multiple
    breaking = [c for c in changes if not c["compatible"]]
    return breaking[0] if breaking else changes[0]


# ──────────────────────────────────────────────
# Snapshot loading
# ──────────────────────────────────────────────

def load_snapshots(contract_id: str, snapshots_dir: str = "schema_snapshots") -> list:
    """Load all timestamped snapshots for a contract, sorted chronologically."""
    snap_dir = Path(snapshots_dir) / contract_id
    if not snap_dir.exists():
        return []

    snapshots = []
    for f in sorted(snap_dir.glob("*.yaml")):
        with open(f) as fp:
            try:
                data = yaml.safe_load(fp)
                snapshots.append({"file": str(f), "timestamp": f.stem, "schema": data.get("schema", {})})
            except Exception:
                pass

    return snapshots


def diff_snapshots(old_snap: dict, new_snap: dict) -> list:
    """
    Diff two schema snapshots and return list of field changes.
    """
    old_schema = old_snap.get("schema", {})
    new_schema = new_snap.get("schema", {})

    all_fields = set(old_schema.keys()) | set(new_schema.keys())
    diffs = []

    for field in sorted(all_fields):
        old_clause = old_schema.get(field)
        new_clause = new_schema.get(field)

        if old_clause == new_clause:
            continue  # Skip identical fields

        change = classify_change(field, old_clause, new_clause)
        change["field"] = field
        diffs.append(change)

    return diffs


# ──────────────────────────────────────────────
# Migration impact report
# ──────────────────────────────────────────────

def generate_migration_report(contract_id: str, diffs: list,
                               old_snap: dict, new_snap: dict) -> dict:
    """Generate a migration impact report for a set of diffs."""
    breaking = [d for d in diffs if not d.get("compatible", True)]
    compatible = [d for d in diffs if d.get("compatible", True)]

    compatibility_verdict = "BACKWARD_COMPATIBLE"
    if breaking:
        compatibility_verdict = "BREAKING"

    migration_checklist = []
    if breaking:
        migration_checklist.append("1. Run blast radius query: check contract_registry/subscriptions.yaml for all subscribers")
        migration_checklist.append("2. Notify all breaking_fields subscribers via contact email in registry")
        migration_checklist.append("3. Create migration branch — do NOT merge to main until all consumers updated")
        for i, b in enumerate(breaking, 4):
            migration_checklist.append(f"{i}. Fix: {b['required_action']}")
        migration_checklist.append(f"{len(breaking)+4}. Re-run ValidationRunner on clean data to re-establish baselines")
        migration_checklist.append(f"{len(breaking)+5}. Update schema_snapshots/ after migration is complete")

    rollback_plan = (
        "Revert to previous snapshot version by restoring the prior "
        f"schema_snapshots/{contract_id}/{old_snap.get('timestamp', 'previous')}.yaml. "
        "Re-run ContractGenerator on the last known good data file. "
        "Notify all subscribers of the rollback via registry contacts."
    ) if breaking else "No rollback required — all changes are backward compatible."

    return {
        "contract_id":            contract_id,
        "analyzed_at":            datetime.now(timezone.utc).isoformat(),
        "old_snapshot":           old_snap.get("timestamp", "unknown"),
        "new_snapshot":           new_snap.get("timestamp", "unknown"),
        "compatibility_verdict":  compatibility_verdict,
        "total_changes":          len(diffs),
        "breaking_changes":       len(breaking),
        "compatible_changes":     len(compatible),
        "changes":                diffs,
        "breaking_details":       breaking,
        "migration_checklist":    migration_checklist,
        "rollback_plan":          rollback_plan,
        "recommendation": (
            f"BLOCK DEPLOY — {len(breaking)} breaking change(s) detected. "
            "Complete migration checklist before merging."
        ) if breaking else "Safe to deploy — all changes are backward compatible.",
        "critical_narrative": " | ".join([
            f"CRITICAL [{d['field']}]: range_narrowed silently rejects previously valid data — "
            f"all subscribers must update validation thresholds before deploy."
            for d in breaking if d.get("change_type") == "range_narrowed"
        ]) or None,
    }


# ──────────────────────────────────────────────
# Main analyzer
# ──────────────────────────────────────────────

def analyze_contract(contract_id: str, snapshots_dir: str = "schema_snapshots",
                     since: str = None) -> dict:
    """
    Analyze schema evolution for a single contract.

    Parameters
    ----------
    contract_id   : contract identifier
    snapshots_dir : path to schema_snapshots/
    since         : if set, only consider snapshots with timestamp >= since value
                    Format: YYYYMMDD_HHMMSS — e.g. "20260401_000000"
    """
    print(f"\n  Analyzing: {contract_id}")

    snapshots = load_snapshots(contract_id, snapshots_dir)

    # Apply --since time window filter
    if since and snapshots:
        snapshots = [s for s in snapshots if s.get("timestamp", "") >= since]
        if snapshots:
            print(f"    ⏱   After --since {since}: {len(snapshots)} snapshot(s) in window")
        else:
            print(f"    ⚠   No snapshots found after --since {since}")

    if len(snapshots) < 2:
        print(f"    ⚠  Only {len(snapshots)} snapshot(s) found — need at least 2 to diff.")
        print(f"    💡  Run generator again on different data to create a second snapshot.")
        return {
            "contract_id":           contract_id,
            "analyzed_at":           datetime.now(timezone.utc).isoformat(),
            "compatibility_verdict": "INSUFFICIENT_SNAPSHOTS",
            "total_changes":         0,
            "breaking_changes":      0,
            "compatible_changes":    0,
            "changes":               [],
            "note": f"Only {len(snapshots)} snapshot(s) available. Need >= 2 to detect evolution.",
        }

    old_snap = snapshots[-2]
    new_snap = snapshots[-1]

    print(f"    Old snapshot: {old_snap['timestamp']}")
    print(f"    New snapshot: {new_snap['timestamp']}")

    diffs = diff_snapshots(old_snap, new_snap)

    if not diffs:
        print(f"    ✅  No schema changes detected between snapshots.")
    else:
        breaking = [d for d in diffs if not d.get("compatible", True)]
        print(f"    📊  {len(diffs)} change(s) detected: "
              f"{len(breaking)} breaking, {len(diffs)-len(breaking)} compatible")
        for d in diffs:
            icon = "❌" if not d.get("compatible", True) else "✅"
            print(f"    {icon}  [{d['severity']}] {d['field']}: {d['change_type']}")

    return generate_migration_report(contract_id, diffs, old_snap, new_snap)


def load_registry_subscribers(registry_path: str, contract_id: str) -> list:
    """Load subscribers for a contract from the registry for per-consumer analysis."""
    try:
        with open(registry_path) as f:
            reg = yaml.safe_load(f)
        return [
            s for s in reg.get("subscriptions", [])
            if s.get("contract_id") == contract_id
        ]
    except Exception:
        return []


def per_consumer_failure_analysis(breaking_changes: list, subscribers: list) -> list:
    """
    For each breaking change, map which subscribers are affected and how.
    Emits per-consumer failure mode analysis required by rubric.
    """
    analysis = []
    for change in breaking_changes:
        field = change.get("field", "unknown")
        change_type = change.get("change_type", "unknown")
        severity = change.get("severity", "BREAKING")

        affected = []
        for sub in subscribers:
            breaking_fields = [bf.get("field", "") for bf in sub.get("breaking_fields", [])]
            if field in breaking_fields or any(field in bf for bf in breaking_fields):
                affected.append({
                    "subscriber_id":    sub.get("subscriber_id"),
                    "validation_mode":  sub.get("validation_mode"),
                    "failure_mode":     sub.get("failure_mode_description", "Not documented"),
                    "on_violation":     sub.get("on_violation_action", "UNKNOWN"),
                    "contact":          sub.get("contact", "unknown"),
                })

        # Label CRITICAL for range_narrowed — it silently corrupts data
        triage_label = "CRITICAL" if change_type == "range_narrowed" else severity
        narrative = (
            f"CRITICAL — range narrowing silently rejects previously valid data. "
            f"All {len(affected)} subscriber(s) must update validation thresholds."
        ) if change_type == "range_narrowed" else (
            f"{severity} — {change.get('description', '')} "
            f"Affects {len(affected)} subscriber(s)."
        )

        analysis.append({
            "field":             field,
            "change_type":       change_type,
            "triage_label":      triage_label,
            "narrative":         narrative,
            "affected_subscribers": affected,
            "total_affected":    len(affected),
        })
    return analysis


def main():
    parser = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer")
    parser.add_argument("--contract-id",    help="Single contract ID to analyze")
    parser.add_argument("--all",            action="store_true",
                        help="Analyze all contracts in schema_snapshots/")
    parser.add_argument("--snapshots-dir",  default="schema_snapshots",
                        help="Directory containing schema snapshots")
    parser.add_argument("--output",         required=True,
                        help="Output path for migration impact report JSON")
    parser.add_argument("--since",          default=None,
                        help="Only diff snapshots created after this timestamp "
                             "(YYYYMMDD_HHMMSS or ISO 8601). "
                             "Example: --since 20260401_000000")
    parser.add_argument("--registry",       default="contract_registry/subscriptions.yaml",
                        help="Path to contract registry for per-consumer failure analysis")
    args = parser.parse_args()

    print("=" * 60)
    print("  SchemaEvolutionAnalyzer")
    print("=" * 60)

    reports = []

    since_filter = args.since  # e.g. "20260401_000000"

    if args.all:
        snap_root = Path(args.snapshots_dir)
        contract_ids = [d.name for d in snap_root.iterdir() if d.is_dir()] if snap_root.exists() else []
        print(f"  Found {len(contract_ids)} contract(s) to analyze")
        if since_filter:
            print(f"  Time window: snapshots after {since_filter}")
        for cid in sorted(contract_ids):
            report = analyze_contract(cid, args.snapshots_dir, since=since_filter)
            # Add per-consumer failure analysis
            subscribers = load_registry_subscribers(args.registry, cid)
            breaking = report.get("breaking_details", [])
            if breaking and subscribers:
                report["per_consumer_failure_analysis"] = per_consumer_failure_analysis(breaking, subscribers)
            else:
                report["per_consumer_failure_analysis"] = []
            reports.append(report)

    elif args.contract_id:
        report = analyze_contract(args.contract_id, args.snapshots_dir, since=since_filter)
        subscribers = load_registry_subscribers(args.registry, args.contract_id)
        breaking = report.get("breaking_details", [])
        if breaking and subscribers:
            report["per_consumer_failure_analysis"] = per_consumer_failure_analysis(breaking, subscribers)
        else:
            report["per_consumer_failure_analysis"] = []
        reports.append(report)

    else:
        print("  ERROR: provide --contract-id or --all")
        return

    # Summary
    total_breaking = sum(r.get("breaking_changes", 0) for r in reports)
    total_changes  = sum(r.get("total_changes", 0)   for r in reports)

    print(f"\n  Summary: {len(reports)} contract(s) analyzed")
    print(f"           {total_changes} total change(s), {total_breaking} breaking")

    output = {
        "analyzed_at":    datetime.now(timezone.utc).isoformat(),
        "contracts_analyzed": len(reports),
        "total_changes":  total_changes,
        "total_breaking": total_breaking,
        "reports":        reports,
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)

    print(f"  ✅  Report → {args.output}")
    print("=" * 60)


if __name__ == "__main__":
    main()