

import json
from pathlib import Path

SOURCE  = "outputs/week3/extractions.jsonl"
DEST    = "outputs/week3/extractions_violated.jsonl"

records = []
with open(SOURCE, encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        # INJECTION: scale confidence from 0.0–1.0 to 0–100
        for fact in r.get("extracted_facts", []):
            if "confidence" in fact:
                fact["confidence"] = round(fact["confidence"] * 100, 1)
        records.append(r)

with open(DEST, "w", encoding="utf-8") as fh:
    for r in records:
        fh.write(json.dumps(r) + "\n")

print(f"✅  Violated dataset written : {DEST}")
print(f"   {len(records)} records modified")
print(f"   Injection: confidence × 100  (0.0–1.0 → 0–100)")
print(f"\nNow run the ValidationRunner against this file:")
print(f"  python contracts/runner.py \\")
print(f"    --contract generated_contracts/week3-document-refinery-extractions.yaml \\")
print(f"    --data {DEST} \\")
print(f"    --output validation_reports/week3_violated.json")