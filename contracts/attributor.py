
import argparse
import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml


# ──────────────────────────────────────────────
# 1. Registry blast radius query (PRIMARY source)
# ──────────────────────────────────────────────

def registry_blast_radius(contract_id: str, failing_field: str, registry_path: str) -> list:
    """
    Query the ContractRegistry for all subscribers affected by a failing field.
    This is the PRIMARY blast radius source — not the lineage graph.
    At Tier 2, this becomes a REST API call to DataHub/OpenMetadata.
    """
    with open(registry_path) as f:
        registry = yaml.safe_load(f)

    affected = []
    for sub in registry.get("subscriptions", []):
        if sub["contract_id"] != contract_id:
            continue
        for bf in sub.get("breaking_fields", []):
            field = bf["field"]
            def norm(s):
                return s.replace("_","").replace(".","").replace("[*]","").lower()
            keywords_failing = set(failing_field.replace(".","_").split("_"))
            keywords_field   = set(field.replace(".","_").split("_"))
            overlap = keywords_failing & keywords_field - {"", "id"}
            if norm(field) in norm(failing_field) or norm(failing_field) in norm(field) or len(overlap) >= 1:
                affected.append({
                    "subscriber_id":   sub["subscriber_id"],
                    "subscriber_team": sub.get("subscriber_team", "unknown"),
                    "contact":         sub.get("contact", "unknown"),
                    "validation_mode": sub.get("validation_mode", "AUDIT"),
                    "breaking_reason": bf["reason"],
                    "fields_consumed": sub.get("fields_consumed", []),
                })
                break

    return affected


# ──────────────────────────────────────────────
# 2. Lineage transitive depth (ENRICHMENT only)
# ──────────────────────────────────────────────

def load_latest_snapshot(lineage_path: str) -> dict:
    """Load the most recent lineage snapshot."""
    with open(lineage_path) as f:
        lines = [l.strip() for l in f if l.strip()]
    return json.loads(lines[-1])


def compute_transitive_depth(producer_node_ids: list, snapshot: dict, max_depth: int = 3) -> dict:
    """
    BFS traversal of the lineage graph to find downstream nodes.
    Returns direct and transitive consumers with contamination depth.
    This ENRICHES the registry blast radius — it does not replace it.
    """
    edges    = snapshot.get("edges", [])
    visited  = set(producer_node_ids)
    frontier = set(producer_node_ids)
    depth_map = {}

    for depth in range(1, max_depth + 1):
        next_frontier = set()
        for node in frontier:
            for edge in edges:
                if (edge["source"] == node and
                        edge.get("relationship") in ("PRODUCES", "WRITES", "CONSUMES", "READS")):
                    target = edge["target"]
                    if target not in visited:
                        depth_map[target] = depth
                        next_frontier.add(target)
                        visited.add(target)
        frontier = next_frontier
        if not frontier:
            break

    return {
        "direct":     [n for n, d in depth_map.items() if d == 1],
        "transitive": [n for n, d in depth_map.items() if d > 1],
        "max_depth":  max(depth_map.values()) if depth_map else 0,
        "all_nodes":  list(depth_map.keys()),
    }


def find_producer_nodes(contract_id: str, snapshot: dict) -> list:
    """
    Find nodes in the lineage graph that correspond to this contract's producer.
    Matches on node_id containing week identifier from contract_id.
    """
    nodes = snapshot.get("nodes", [])
    week_hint = contract_id.split("-")[0]  # e.g. "week3"

    producer_nodes = []
    for node in nodes:
        nid = node.get("node_id", "")
        if week_hint in nid or contract_id.split("-")[1] in nid if len(contract_id.split("-")) > 1 else False:
            producer_nodes.append(nid)

    # Also check for known producer patterns
    known_producers = {
        "week3-document-refinery-extractions": ["file::src/week3/extractor.py", "table::extractions"],
        "week4-lineage-snapshots":             ["file::src/week4/cartographer.py", "table::lineage_snapshots"],
        "week5-event-records":                 ["table::events"],
        "week2-verdict-records":               ["table::verdicts"],
        "week1-intent-records":                ["table::intent_records"],
        "langsmith-traces":                    ["table::traces"],
    }
    node_ids = {n.get("node_id") for n in nodes}
    for candidate in known_producers.get(contract_id, []):
        if candidate in node_ids and candidate not in producer_nodes:
            producer_nodes.append(candidate)

    return producer_nodes if producer_nodes else [f"table::{contract_id.split('-')[0]}"]


# ──────────────────────────────────────────────
# 3. Git blame for cause attribution
# ──────────────────────────────────────────────

def get_recent_commits(file_path: str, repo_root: str, days: int = 30) -> list:
    """
    Run git log on a file to find recent commits that may have caused the violation.
    Falls back gracefully if git is unavailable or file not tracked.
    """
    try:
        cmd = [
            "git", "log", "--follow",
            f"--since={days} days ago",
            "--format=%H|%ae|%ai|%s",
            "--", file_path
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            cwd=repo_root, timeout=10
        )
        commits = []
        for line in result.stdout.strip().split("\n"):
            if "|" not in line:
                continue
            parts = line.split("|", 3)
            if len(parts) == 4:
                h, ae, ai, s = parts
                commits.append({
                    "commit_hash":      h.strip(),
                    "author":           ae.strip(),
                    "commit_timestamp": ai.strip(),
                    "commit_message":   s.strip(),
                })
        return commits
    except Exception as e:
        return [{
            "commit_hash":      "git-unavailable-" + str(uuid.uuid4())[:8],
            "author":           "unknown",
            "commit_timestamp": datetime.now(timezone.utc).isoformat(),
            "commit_message":   f"Git log unavailable: {str(e)[:80]}",
        }]


def score_blame_candidates(commits: list, violation_ts: str, lineage_distance: int) -> list:
    """
    Score each commit candidate by temporal proximity and lineage distance.
    Formula: base = 1.0 - (days_since_commit * 0.1) - (lineage_distance * 0.2)
    Returns top 5 ranked candidates.
    """
    scored = []
    try:
        vt = datetime.fromisoformat(violation_ts.replace("Z", "+00:00"))
    except Exception:
        vt = datetime.now(timezone.utc)

    for rank, commit in enumerate(commits[:5], 1):
        try:
            ct_str = commit["commit_timestamp"]
            # Handle various git timestamp formats
            if " +" in ct_str or " -" in ct_str:
                ct_str = ct_str.replace(" +", "+").replace(" -", "-")
                ct_str = ct_str[:19] + ct_str[19:].replace(" ", "")
            ct = datetime.fromisoformat(ct_str)
            if ct.tzinfo is None:
                ct = ct.replace(tzinfo=timezone.utc)
            days = abs((vt - ct).days)
        except Exception:
            days = 7  # default assumption

        score = max(0.0, round(1.0 - (days * 0.1) - (lineage_distance * 0.2), 3))
        scored.append({
            **commit,
            "rank":             rank,
            "confidence_score": score,
            "days_from_violation": days,
            "lineage_distance": lineage_distance,
        })

    return sorted(scored, key=lambda x: x["confidence_score"], reverse=True)


def build_blame_chain(contract_id: str, snapshot: dict, violation_ts: str, repo_root: str) -> list:
    """
    Build the blame chain by finding producer files and running git log on them.
    """
    # Known source files per contract
    source_files = {
        "week3-document-refinery-extractions": [
            "contracts/generator.py",
            "outputs/week3/extractions.jsonl",
            "src/week3/extractor.py",
        ],
        "week4-lineage-snapshots": [
            "outputs/week4/lineage_snapshots.jsonl",
            "src/week4/cartographer.py",
        ],
        "week5-event-records": [
            "outputs/week5/events.jsonl",
        ],
        "week2-verdict-records": [
            "outputs/week2/verdicts.jsonl",
        ],
        "week1-intent-records": [
            "outputs/week1/intent_records.jsonl",
        ],
        "langsmith-traces": [
            "outputs/traces/runs.jsonl",
        ],
    }

    files_to_check = source_files.get(contract_id, ["outputs/"])
    all_commits = []

    for file_path in files_to_check:
        commits = get_recent_commits(file_path, repo_root, days=60)
        distance = files_to_check.index(file_path)  # closer files = lower distance
        scored = score_blame_candidates(commits, violation_ts, lineage_distance=distance)
        all_commits.extend(scored)

    # Deduplicate by commit hash and keep top 5
    seen = set()
    deduped = []
    for c in sorted(all_commits, key=lambda x: x["confidence_score"], reverse=True):
        key = c["commit_hash"]
        if key not in seen:
            seen.add(key)
            deduped.append(c)
        if len(deduped) >= 5:
            break

    # Re-rank after dedup
    for i, c in enumerate(deduped, 1):
        c["rank"] = i

    # Ensure at least one candidate
    if not deduped:
        deduped = [{
            "rank":             1,
            "commit_hash":      "no-commits-found-" + str(uuid.uuid4())[:8],
            "author":           "unknown",
            "commit_timestamp": datetime.now(timezone.utc).isoformat(),
            "commit_message":   "No recent commits found in git log",
            "confidence_score": 0.1,
            "days_from_violation": 0,
            "lineage_distance": 0,
        }]

    return deduped


# ──────────────────────────────────────────────
# 4. Write violation log entry
# ──────────────────────────────────────────────

def write_violation_log(entry: dict, output_path: str):
    """Append violation record to violations.jsonl."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"  📝  Violation logged → {output_path}")


# ──────────────────────────────────────────────
# 5. Main attribution pipeline
# ──────────────────────────────────────────────

def attribute_violations(
    report_path: str,
    lineage_path: str,
    registry_path: str,
    output_path: str,
    repo_root: str = ".",
):
    print("=" * 60)
    print("  ViolationAttributor")
    print("=" * 60)

    # Load validation report
    with open(report_path) as f:
        report = json.load(f)

    contract_id  = report.get("contract_id", "unknown")
    run_ts       = report.get("run_timestamp", datetime.now(timezone.utc).isoformat())
    results      = report.get("results", [])
    failed       = [r for r in results if r.get("status") in ("FAIL", "ERROR")]

    print(f"  Contract  : {contract_id}")
    print(f"  Report    : {report_path}")
    print(f"  Failures  : {len(failed)}")

    if not failed:
        print("  ✅  No violations to attribute.")
        return

    # Load lineage snapshot
    snapshot = load_latest_snapshot(lineage_path)
    print(f"  Snapshot  : {snapshot.get('snapshot_id', 'unknown')[:16]}...")

    attributed_count = 0

    for result in failed:
        check_id     = result.get("check_id", "unknown")
        column_name  = result.get("column_name", "unknown")
        severity     = result.get("severity", "UNKNOWN")

        print(f"\n  Attributing: {check_id} [{severity}]")

        # Step 1 — Registry blast radius (PRIMARY)
        print(f"  Step 1 — Registry blast radius query...")
        registry_affected = registry_blast_radius(contract_id, column_name, registry_path)
        print(f"    Found {len(registry_affected)} registry subscriber(s) affected")
        for sub in registry_affected:
            print(f"    → {sub['subscriber_id']} [{sub['validation_mode']}]: {sub['breaking_reason'][:60]}")

        # Step 2 — Lineage transitive depth (ENRICHMENT)
        print(f"  Step 2 — Lineage transitive depth enrichment...")
        producer_nodes = find_producer_nodes(contract_id, snapshot)
        lineage_result = compute_transitive_depth(producer_nodes, snapshot)
        print(f"    Producer nodes : {producer_nodes}")
        print(f"    Direct consumers (lineage)    : {lineage_result['direct']}")
        print(f"    Transitive consumers (lineage): {lineage_result['transitive']}")
        print(f"    Max contamination depth       : {lineage_result['max_depth']}")

        # Step 3 — Git blame
        print(f"  Step 3 — Git blame attribution...")
        blame_chain = build_blame_chain(contract_id, snapshot, run_ts, repo_root)
        print(f"    Top candidate: {blame_chain[0]['commit_hash'][:12]}... "
              f"by {blame_chain[0]['author']} "
              f"(score={blame_chain[0]['confidence_score']})")

        # Step 4 — Write violation log
        entry = {
            "violation_id": str(uuid.uuid4()),
            "check_id":     check_id,
            "contract_id":  contract_id,
            "column_name":  column_name,
            "severity":     severity,
            "detected_at":  datetime.now(timezone.utc).isoformat(),
            "actual_value": result.get("actual_value", "unknown"),
            "expected":     result.get("expected", "unknown"),
            "records_failing": result.get("records_failing", 0),
            "blast_radius": {
                "source": "registry",
                "note": "direct_subscribers from registry; transitive_nodes from lineage graph enrichment",
                "direct_subscribers": registry_affected,
                "lineage_direct_nodes":     lineage_result["direct"],
                "lineage_transitive_nodes": lineage_result["transitive"],
                "contamination_depth":      lineage_result["max_depth"],
                "total_affected_systems":   len(registry_affected) + len(lineage_result["transitive"]),
            },
            "blame_chain": blame_chain,
            "source_report": report_path,
        }

        write_violation_log(entry, output_path)
        attributed_count += 1

    print(f"\n  ✅  Attributed {attributed_count} violation(s)")
    print(f"  📄  Violation log → {output_path}")
    print("=" * 60)


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ViolationAttributor")
    parser.add_argument("--violation", required=True,
                        help="Path to validation report JSON (from runner.py)")
    parser.add_argument("--lineage",   required=True,
                        help="Path to lineage_snapshots.jsonl (Week 4 output)")
    parser.add_argument("--registry",  required=True,
                        help="Path to contract_registry/subscriptions.yaml")
    parser.add_argument("--output",    required=True,
                        help="Path to violation_log/violations.jsonl (append)")
    parser.add_argument("--repo-root", default=".",
                        help="Root of the git repository for blame (default: .)")
    args = parser.parse_args()

    attribute_violations(
        report_path   = args.violation,
        lineage_path  = args.lineage,
        registry_path = args.registry,
        output_path   = args.output,
        repo_root     = args.repo_root,
    )


if __name__ == "__main__":
    main()