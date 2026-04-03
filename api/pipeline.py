import subprocess
import os
import json
import sys
from pathlib import Path
from datetime import datetime, timezone


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_command(cmd: list, cwd: str = None) -> dict:
    """Run a shell command and return output."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=cwd or BASE_DIR,
            timeout=120
        )
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode
        }
    except subprocess.TimeoutExpired:
        return {'success': False, 'stdout': '', 'stderr': 'Command timed out after 120s', 'returncode': -1}
    except Exception as e:
        return {'success': False, 'stdout': '', 'stderr': str(e), 'returncode': -1}


def get_python():
    """Get the Python executable path."""
    venv_python = os.path.join(BASE_DIR, 'venv', 'bin', 'python')
    if os.path.exists(venv_python):
        return venv_python
    return sys.executable


def run_generator(source: str, contract_id: str) -> dict:
    """Run ContractGenerator on a data file."""
    python = get_python()
    cmd = [
        python, 'contracts/generator.py',
        '--source', source,
        '--contract-id', contract_id,
        '--lineage', 'outputs/week4/lineage_snapshots.jsonl',
        '--output', 'generated_contracts/'
    ]
    return run_command(cmd)


def run_validator(contract_path: str, data_path: str, output_path: str = None) -> dict:
    """Run ValidationRunner on a data file."""
    python = get_python()
    if not output_path:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'validation_reports/run_{ts}.json'
    cmd = [
        python, 'contracts/runner.py',
        '--contract', contract_path,
        '--data', data_path,
        '--output', output_path
    ]
    result = run_command(cmd)
    result['report_path'] = output_path
    if os.path.exists(os.path.join(BASE_DIR, output_path)):
        with open(os.path.join(BASE_DIR, output_path)) as f:
            result['report'] = json.load(f)
    return result


def run_attributor(violation_report: str) -> dict:
    """Run ViolationAttributor on a validation report."""
    python = get_python()
    cmd = [
        python, 'contracts/attributor.py',
        '--violation', violation_report,
        '--lineage', 'outputs/week4/lineage_snapshots.jsonl',
        '--registry', 'contract_registry/subscriptions.yaml',
        '--output', 'violation_log/violations.jsonl'
    ]
    return run_command(cmd)


def run_schema_analyzer() -> dict:
    """Run SchemaEvolutionAnalyzer on all contracts."""
    python = get_python()
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    output = f'validation_reports/schema_evolution_{ts}.json'
    cmd = [
        python, 'contracts/schema_analyzer.py',
        '--all',
        '--output', output
    ]
    result = run_command(cmd)
    result['report_path'] = output
    if os.path.exists(os.path.join(BASE_DIR, output)):
        with open(os.path.join(BASE_DIR, output)) as f:
            result['report'] = json.load(f)
    return result


def run_ai_extensions() -> dict:
    """Run AI Contract Extensions."""
    python = get_python()
    cmd = [
        python, 'contracts/ai_extensions.py',
        '--extractions', 'outputs/week3/extractions.jsonl',
        '--verdicts', 'outputs/week2/verdicts.jsonl',
        '--traces', 'outputs/traces/runs.jsonl',
        '--output', 'validation_reports/ai_extensions.json'
    ]
    result = run_command(cmd)
    ai_path = os.path.join(BASE_DIR, 'validation_reports/ai_extensions.json')
    if os.path.exists(ai_path):
        with open(ai_path) as f:
            result['report'] = json.load(f)
    return result


def run_report_generator() -> dict:
    """Run ReportGenerator."""
    python = get_python()
    cmd = [
        python, 'contracts/report_generator.py',
        '--output', 'enforcer_report/report_data.json'
    ]
    result = run_command(cmd)
    report_path = os.path.join(BASE_DIR, 'enforcer_report/report_data.json')
    if os.path.exists(report_path):
        with open(report_path) as f:
            result['report'] = json.load(f)
    return result


def run_full_pipeline() -> dict:
    """Run the complete enforcement pipeline."""
    steps = []

    steps.append({'step': 'schema_evolution', 'label': 'Schema Evolution Analysis', **run_schema_analyzer()})
    steps.append({'step': 'ai_extensions', 'label': 'AI Contract Extensions', **run_ai_extensions()})
    steps.append({'step': 'report', 'label': 'Report Generation', **run_report_generator()})

    overall_success = all(s.get('success', False) for s in steps)
    return {
        'success': overall_success,
        'steps': steps,
        'completed_at': datetime.now(timezone.utc).isoformat()
    }