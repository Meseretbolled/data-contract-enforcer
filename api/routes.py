import os
import json
import glob
import uuid
from pathlib import Path
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, current_app
from werkzeug.utils import secure_filename

from api.pipeline import (
    run_generator, run_validator, run_attributor,
    run_schema_analyzer, run_ai_extensions,
    run_report_generator, run_full_pipeline
)

api_bp = Blueprint('api', __name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ALLOWED_EXTENSIONS = {'jsonl', 'json', 'yaml', 'yml'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def base_path(rel):
    return os.path.join(BASE_DIR, rel)


def safe_load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def safe_load_jsonl(path):
    records = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
    except Exception:
        pass
    return records


# ── Health ──────────────────────────────────────────────────────────────

@api_bp.route('/health')
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.now(timezone.utc).isoformat()})


# ── Report ──────────────────────────────────────────────────────────────

@api_bp.route('/report')
def get_report():
    """Load the latest enforcer report."""
    path = base_path('enforcer_report/report_data.json')
    data = safe_load_json(path)
    if not data:
        return jsonify({'error': 'No report found. Run the pipeline first.'}), 404
    return jsonify(data)


# ── Contracts ────────────────────────────────────────────────────────────

@api_bp.route('/contracts')
def list_contracts():
    """List all generated contracts with their clause counts."""
    contracts = []
    pattern = base_path('generated_contracts/*.yaml')
    for path in sorted(glob.glob(pattern)):
        if '_dbt' in path:
            continue
        try:
            import yaml
            with open(path) as f:
                data = yaml.safe_load(f)
            schema = data.get('schema', {})
            contracts.append({
                'contract_id': data.get('id', Path(path).stem),
                'title': data.get('info', {}).get('title', ''),
                'version': data.get('info', {}).get('version', '1.0.0'),
                'clauses': len(schema),
                'path': path,
                'filename': Path(path).name,
                'has_dbt': os.path.exists(path.replace('.yaml', '_dbt.yml')),
            })
        except Exception as e:
            contracts.append({'path': path, 'error': str(e)})
    return jsonify({'contracts': contracts, 'total': len(contracts)})


@api_bp.route('/contracts/<contract_id>')
def get_contract(contract_id):
    """Get a specific contract's full content."""
    path = base_path(f'generated_contracts/{contract_id}.yaml')
    if not os.path.exists(path):
        return jsonify({'error': f'Contract {contract_id} not found'}), 404
    try:
        import yaml
        with open(path) as f:
            data = yaml.safe_load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Validation Reports ───────────────────────────────────────────────────

@api_bp.route('/reports')
def list_reports():
    """List all validation reports."""
    reports = []
    pattern = base_path('validation_reports/*.json')
    for path in sorted(glob.glob(pattern), reverse=True):
        if any(x in path for x in ['schema_evolution', 'ai_extensions']):
            continue
        data = safe_load_json(path)
        if data:
            reports.append({
                'report_id': data.get('report_id', Path(path).stem),
                'contract_id': data.get('contract_id', ''),
                'run_timestamp': data.get('run_timestamp', ''),
                'total_checks': data.get('total_checks', 0),
                'passed': data.get('passed', 0),
                'failed': data.get('failed', 0),
                'warned': data.get('warned', 0),
                'errored': data.get('errored', 0),
                'filename': Path(path).name,
            })
    return jsonify({'reports': reports, 'total': len(reports)})


@api_bp.route('/reports/<filename>')
def get_report_detail(filename):
    """Get a specific validation report."""
    path = base_path(f'validation_reports/{filename}')
    data = safe_load_json(path)
    if not data:
        return jsonify({'error': 'Report not found'}), 404
    return jsonify(data)


# ── Violations ───────────────────────────────────────────────────────────

@api_bp.route('/violations')
def list_violations():
    """List all violations from the violation log."""
    records = safe_load_jsonl(base_path('violation_log/violations.jsonl'))
    violations = [r for r in records if not r.get('injection_note')]
    injections = [r for r in records if r.get('injection_note')]
    return jsonify({
        'violations': violations,
        'total': len(violations),
        'injected': len(injections)
    })


# ── Registry ─────────────────────────────────────────────────────────────

@api_bp.route('/registry')
def get_registry():
    """Get all contract registry subscriptions."""
    try:
        import yaml
        path = base_path('contract_registry/subscriptions.yaml')
        with open(path) as f:
            data = yaml.safe_load(f)
        subs = data.get('subscriptions', [])
        return jsonify({'subscriptions': subs, 'total': len(subs)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Schema Evolution ──────────────────────────────────────────────────────

@api_bp.route('/schema-evolution')
def get_schema_evolution():
    """Get latest schema evolution report."""
    reports = sorted(glob.glob(base_path('validation_reports/schema_evolution*.json')), reverse=True)
    if not reports:
        return jsonify({'error': 'No schema evolution report found. Run analyzer first.'}), 404
    data = safe_load_json(reports[0])
    return jsonify(data or {'error': 'Could not parse report'})


# ── AI Extensions ─────────────────────────────────────────────────────────

@api_bp.route('/ai-extensions')
def get_ai_extensions():
    """Get AI extensions report."""
    path = base_path('validation_reports/ai_extensions.json')
    data = safe_load_json(path)
    if not data:
        return jsonify({'error': 'No AI extensions report found. Run extensions first.'}), 404
    return jsonify(data)


# ── Pipeline Commands ─────────────────────────────────────────────────────

@api_bp.route('/run/validate', methods=['POST'])
def run_validate():
    """Run ValidationRunner on a contract + data file."""
    body = request.get_json() or {}
    contract_id = body.get('contract_id', 'week3-document-refinery-extractions')
    data_file = body.get('data_file', 'outputs/week3/extractions.jsonl')
    violated = body.get('violated', False)

    if violated:
        data_file = data_file.replace('.jsonl', '_violated.jsonl')

    contract_path = f'generated_contracts/{contract_id}.yaml'
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = f'validation_reports/{contract_id}_{ts}.json'

    result = run_validator(contract_path, data_file, output_path)
    return jsonify(result)


@api_bp.route('/run/attribute', methods=['POST'])
def run_attribute():
    """Run ViolationAttributor on the latest violated report."""
    reports = sorted(glob.glob(base_path('validation_reports/*violated*.json')), reverse=True)
    if not reports:
        return jsonify({'error': 'No violated report found. Run validation with violated data first.'}), 404
    rel_path = os.path.relpath(reports[0], BASE_DIR)
    result = run_attributor(rel_path)
    return jsonify(result)


@api_bp.route('/run/schema-analyzer', methods=['POST'])
def run_schema():
    """Run SchemaEvolutionAnalyzer."""
    result = run_schema_analyzer()
    return jsonify(result)


@api_bp.route('/run/ai-extensions', methods=['POST'])
def run_ai():
    """Run AI Contract Extensions."""
    result = run_ai_extensions()
    return jsonify(result)


@api_bp.route('/run/report', methods=['POST'])
def run_report():
    """Run ReportGenerator."""
    result = run_report_generator()
    return jsonify(result)


@api_bp.route('/run/full-pipeline', methods=['POST'])
def run_pipeline():
    """Run the complete enforcement pipeline."""
    result = run_full_pipeline()
    return jsonify(result)


# ── File Upload + Validate ────────────────────────────────────────────────

@api_bp.route('/upload', methods=['POST'])
def upload_and_validate():
    """
    Upload a JSONL file and validate it against a selected contract.
    Returns the full validation report.
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    contract_id = request.form.get('contract_id', 'week3-document-refinery-extractions')

    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if not allowed_file(file.filename):
        return jsonify({'error': 'File must be .jsonl or .json'}), 400

    filename = secure_filename(file.filename)
    upload_id = str(uuid.uuid4())[:8]
    save_name = f'{upload_id}_{filename}'
    save_path = os.path.join(BASE_DIR, 'uploads', save_name)
    file.save(save_path)

    contract_path = f'generated_contracts/{contract_id}.yaml'
    if not os.path.exists(base_path(contract_path)):
        return jsonify({'error': f'Contract {contract_id} not found'}), 404

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = f'validation_reports/upload_{upload_id}_{ts}.json'
    rel_save = os.path.relpath(save_path, BASE_DIR)

    result = run_validator(contract_path, rel_save, output_path)
    result['uploaded_file'] = filename
    result['contract_id'] = contract_id
    result['upload_id'] = upload_id

    return jsonify(result)


# ── Stats ─────────────────────────────────────────────────────────────────

@api_bp.route('/stats')
def get_stats():
    """Get quick system stats for the dashboard header."""
    contracts = len([f for f in glob.glob(base_path('generated_contracts/*.yaml')) if '_dbt' not in f])
    reports = len(glob.glob(base_path('validation_reports/*.json')))
    violations = len([r for r in safe_load_jsonl(base_path('violation_log/violations.jsonl')) if not r.get('injection_note')])

    report_data = safe_load_json(base_path('enforcer_report/report_data.json'))
    health_score = report_data.get('data_health_score', 0) if report_data else 0

    import yaml
    try:
        with open(base_path('contract_registry/subscriptions.yaml')) as f:
            reg = yaml.safe_load(f)
        subscriptions = len(reg.get('subscriptions', []))
    except Exception:
        subscriptions = 0

    return jsonify({
        'contracts': contracts,
        'reports': reports,
        'violations': violations,
        'subscriptions': subscriptions,
        'health_score': health_score,
        'last_updated': datetime.now(timezone.utc).isoformat()
    })