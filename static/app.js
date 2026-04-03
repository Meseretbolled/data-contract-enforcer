// Data Contract Enforcer — Frontend JavaScript

const API = '';
let allContracts = [];
let allViolations = [];
let allSubscriptions = [];
let charts = {};
let selectedFile = null;

// ── Init ──────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  refreshAll();
});

async function refreshAll() {
  await loadStats();
  await loadReport();
  loadContracts();
  loadViolations();
  loadRegistry();
  loadSchemaEvolution();
  loadAIExtensions();
}

// ── Tab switching ─────────────────────────────────────────────────────

const tabTitles = {
  overview: 'Overview',
  contracts: 'Contracts',
  violations: 'Violations',
  schema: 'Schema Evolution',
  ai: 'AI Extensions',
  registry: 'Registry',
  pipeline: 'Run Pipeline',
  upload: 'Upload & Validate'
};

function showTab(name) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  document.getElementById('nav-' + name).classList.add('active');
  document.getElementById('page-title').textContent = tabTitles[name] || name;
  document.getElementById('breadcrumb').textContent = 'Dashboard / ' + (tabTitles[name] || name);
}

// ── Stats ─────────────────────────────────────────────────────────────

async function loadStats() {
  try {
    const data = await fetch(`${API}/api/stats`).then(r => r.json());
    document.getElementById('stat-contracts').textContent = data.contracts || 0;
    document.getElementById('stat-violations').textContent = data.violations || 0;
    document.getElementById('stat-subs').textContent = data.subscriptions || 0;
    document.getElementById('stat-reports').textContent = data.reports || 0;

    const score = data.health_score || 0;
    const scoreEl = document.getElementById('stat-health');
    scoreEl.textContent = score;
    scoreEl.className = 'stat-val ' + (score >= 90 ? 'green' : score >= 60 ? 'amber' : 'red');

    document.getElementById('sidebar-score').textContent = score;
    document.getElementById('sidebar-score').style.color =
      score >= 90 ? 'var(--green)' : score >= 60 ? 'var(--amber)' : 'var(--red)';

    const pill = document.getElementById('status-pill');
    const txt = document.getElementById('status-text');
    if (score >= 90) { pill.className = 'status-pill pass'; txt.textContent = 'All systems healthy'; }
    else if (score >= 60) { pill.className = 'status-pill warn'; txt.textContent = 'Issues detected'; }
    else { pill.className = 'status-pill fail'; txt.textContent = 'Critical violations'; }

  } catch (e) {
    document.getElementById('status-text').textContent = 'API offline';
    document.getElementById('status-pill').className = 'status-pill fail';
  }
}

// ── Report ────────────────────────────────────────────────────────────

async function loadReport() {
  try {
    const data = await fetch(`${API}/api/report`).then(r => r.json());
    if (data.error) return;

    document.getElementById('health-narrative').textContent = data.health_narrative || '';

    const sev = data.violations_by_severity || {};
    renderSeverityChart(sev);

    const contracts = data.contracts_summary || [];
    renderPassRateChart(contracts);

    const topViol = data.top_violations || [];
    const topEl = document.getElementById('top-violations-list');
    if (topViol.length) {
      topEl.innerHTML = topViol.map(v => `<div class="top-viol-item">${v}</div>`).join('');
    } else {
      topEl.innerHTML = '<div class="empty-state">No violations detected</div>';
    }

    const recs = data.recommended_actions || [];
    document.getElementById('recommendations-list').innerHTML = recs.map((r, i) => `
      <div class="rec-item">
        <div class="rec-num">${String(i+1).padStart(2,'0')}</div>
        <div class="rec-text">${r}</div>
      </div>
    `).join('') || '<div class="empty-state">No recommendations</div>';

  } catch (e) {}
}

function renderPassRateChart(contracts) {
  const ctx = document.getElementById('passRateChart');
  if (!ctx) return;
  if (charts.passRate) charts.passRate.destroy();
  charts.passRate = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: contracts.map(c => c.contract_id.replace('week','w').replace(/-document-refinery-extractions|--records|--snapshots/g,'')),
      datasets: [{
        data: contracts.map(c => c.total_checks > 0 ? Math.round((c.passed / c.total_checks) * 100) : 0),
        backgroundColor: contracts.map(c => c.failed > 0 ? '#f05454' : '#3ecf7a'),
        borderRadius: 6, borderSkipped: false
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { display: false }, ticks: { color: '#7a7f94', font: { size: 10 }, maxRotation: 30 } },
        y: { max: 100, grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#7a7f94', callback: v => v + '%', font: { size: 11 } } }
      }
    }
  });
}

function renderSeverityChart(sev) {
  const ctx = document.getElementById('severityChart');
  if (!ctx) return;
  if (charts.severity) charts.severity.destroy();
  const labels = Object.keys(sev);
  const values = Object.values(sev);
  if (!labels.length) { labels.push('No violations'); values.push(0); }
  charts.severity = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: ['#f05454','#f5a623','#4f8ef7','#7a7f94'],
        borderWidth: 0
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { color: '#7a7f94', font: { size: 11 }, padding: 12 } }
      }
    }
  });
}

// ── Contracts ─────────────────────────────────────────────────────────

async function loadContracts() {
  try {
    const data = await fetch(`${API}/api/contracts`).then(r => r.json());
    allContracts = data.contracts || [];
    renderContractTable(allContracts);
  } catch (e) {
    document.getElementById('contracts-table').innerHTML = '<div class="empty-state">Failed to load contracts</div>';
  }
}

function renderContractTable(contracts) {
  const el = document.getElementById('contracts-table');
  if (!contracts.length) { el.innerHTML = '<div class="empty-state">No contracts found</div>'; return; }

  el.innerHTML = `
    <table class="data-table">
      <thead>
        <tr>
          <th>Contract ID</th>
          <th>Version</th>
          <th>Clauses</th>
          <th>dbt</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody>
        ${contracts.map(c => `
          <tr onclick="viewContract('${c.contract_id}')">
            <td><span style="font-family:var(--font);font-size:11px;color:var(--accent)">${c.contract_id}</span></td>
            <td><span class="badge badge-info">${c.version || '1.0.0'}</span></td>
            <td><span class="badge badge-info">${c.clauses} clauses</span></td>
            <td>${c.has_dbt ? '<span class="badge badge-pass">yes</span>' : '<span class="badge badge-warn">no</span>'}</td>
            <td><button class="btn-run" onclick="event.stopPropagation();viewContract('${c.contract_id}')">View</button></td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;
}

function filterContracts() {
  const search = document.getElementById('contract-search').value.toLowerCase();
  const status = document.getElementById('contract-status-filter').value;
  let filtered = allContracts.filter(c => c.contract_id.toLowerCase().includes(search));
  renderContractTable(filtered);
}

async function viewContract(contractId) {
  try {
    const data = await fetch(`${API}/api/contracts/${contractId}`).then(r => r.json());
    const card = document.getElementById('contract-detail-card');
    const title = document.getElementById('contract-detail-title');
    const content = document.getElementById('contract-detail-content');
    card.style.display = 'block';
    title.textContent = `Contract: ${contractId}`;

    const schema = data.schema || {};
    const clauses = Object.entries(schema);

    content.innerHTML = `
      <div style="margin-bottom:12px;font-size:12px;color:var(--muted)">${data.info?.description || ''}</div>
      ${clauses.map(([name, clause]) => `
        <div class="clause-row">
          <div class="clause-name">${name}</div>
          <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:4px">
            ${clause.type ? `<span class="badge badge-info">${clause.type}</span>` : ''}
            ${clause.required ? '<span class="badge badge-pass">required</span>' : '<span class="badge badge-warn">optional</span>'}
            ${clause.format ? `<span class="badge badge-info">${clause.format}</span>` : ''}
            ${clause.minimum !== undefined ? `<span class="badge badge-high">min: ${clause.minimum}</span>` : ''}
            ${clause.maximum !== undefined ? `<span class="badge badge-high">max: ${clause.maximum}</span>` : ''}
            ${clause.enum ? `<span class="badge badge-info">enum: ${clause.enum.slice(0,3).join(', ')}${clause.enum.length > 3 ? '...' : ''}</span>` : ''}
          </div>
          ${clause.description ? `<div class="clause-desc">${clause.description.slice(0,120)}${clause.description.length > 120 ? '...' : ''}</div>` : ''}
        </div>
      `).join('')}
    `;
    card.scrollIntoView({ behavior: 'smooth', block: 'start' });
  } catch (e) {}
}

// ── Violations ────────────────────────────────────────────────────────

async function loadViolations() {
  try {
    const data = await fetch(`${API}/api/violations`).then(r => r.json());
    allViolations = data.violations || [];
    renderViolations(allViolations);
  } catch (e) {}
}

function renderViolations(violations) {
  const el = document.getElementById('violations-list');
  if (!violations.length) {
    el.innerHTML = '<div class="empty-state">No violations found</div>';
    return;
  }
  el.innerHTML = violations.map((v, i) => `
    <div class="violation-card" onclick="selectViolation(${i})" id="vcard-${i}">
      <div class="violation-header">
        <span class="badge badge-${(v.severity||'').toLowerCase()}">${v.severity || 'UNKNOWN'}</span>
        <span style="font-family:var(--font);font-size:10px;color:var(--muted)">${v.violation_id?.slice(0,16) || ''}</span>
      </div>
      <div class="violation-check">${v.check_id || v.column_name || 'Unknown check'}</div>
      <div class="violation-vals">actual: ${v.actual_value || 'N/A'} → expected: ${v.expected || 'N/A'}</div>
      <div class="violation-meta">${v.records_failing || 0} records failing · ${(v.detected_at||'').slice(0,16)}</div>
    </div>
  `).join('');
}

function filterViolations() {
  const sev = document.getElementById('sev-filter').value;
  let filtered = allViolations;
  if (sev !== 'all') filtered = filtered.filter(v => v.severity === sev);
  renderViolations(filtered);
}

function selectViolation(idx) {
  document.querySelectorAll('.violation-card').forEach(c => c.classList.remove('selected'));
  const card = document.getElementById('vcard-' + idx);
  if (card) card.classList.add('selected');

  const v = allViolations[idx];
  if (!v) return;

  const br = v.blast_radius || {};
  const subs = br.direct_subscribers || [];
  const trans = br.lineage_transitive_nodes || [];

  document.getElementById('blast-content').innerHTML = `
    <div style="font-size:11px;color:var(--muted);margin-bottom:10px">
      ${subs.length} direct subscribers · depth ${br.contamination_depth || 0}
    </div>
    ${subs.map(s => `
      <div class="blast-item">
        <span class="blast-arrow">→</span>
        <div>
          <div style="display:flex;align-items:center;gap:8px">
            <span class="blast-sys">${s.subscriber_id}</span>
            <span class="badge badge-${(s.validation_mode||'').toLowerCase()}">${s.validation_mode || ''}</span>
          </div>
          <div class="blast-reason">${s.breaking_reason || ''}</div>
        </div>
      </div>
    `).join('')}
    ${trans.length ? `
      <div style="font-size:11px;color:var(--muted);margin-top:10px">
        Transitive: ${trans.join(', ')}
      </div>
    ` : ''}
  ` || '<div class="empty-state">No blast radius data</div>';

  const chain = v.blame_chain || [];
  document.getElementById('blame-content').innerHTML = chain.map(b => `
    <div class="blame-item">
      <div style="display:flex;align-items:center;gap:8px">
        <span class="blame-hash">${(b.commit_hash||'').slice(0,12)}</span>
        <span class="badge badge-info">score: ${(b.confidence_score||0).toFixed(2)}</span>
      </div>
      <div class="blame-msg">${b.commit_message || ''}</div>
      <div class="blame-meta">${b.author || ''} · ${(b.commit_timestamp||'').slice(0,16)}</div>
    </div>
  `).join('') || '<div class="empty-state">No blame chain data</div>';
}

// ── Schema Evolution ──────────────────────────────────────────────────

async function loadSchemaEvolution() {
  try {
    const data = await fetch(`${API}/api/schema-evolution`).then(r => r.json());
    if (data.error) { document.getElementById('schema-status').textContent = data.error; return; }

    const reports = data.reports || [];
    renderSchemaChangesChart(reports);
    renderSchemaSummary(reports);
    renderSchemaChangesList(reports);
    document.getElementById('schema-status').textContent =
      `${data.total_changes || 0} changes · ${data.total_breaking || 0} breaking`;
  } catch (e) {}
}

function renderSchemaChangesChart(reports) {
  const ctx = document.getElementById('schemaChangesChart');
  if (!ctx) return;
  if (charts.schema) charts.schema.destroy();
  const valid = reports.filter(r => r.total_changes > 0);
  charts.schema = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: valid.map(r => r.contract_id.replace('week','w').replace(/-[a-z]+-[a-z]+$/,'')),
      datasets: [
        { label: 'Breaking', data: valid.map(r => r.breaking_changes||0), backgroundColor: '#f05454', borderRadius: 4 },
        { label: 'Compatible', data: valid.map(r => (r.total_changes||0)-(r.breaking_changes||0)), backgroundColor: '#3ecf7a', borderRadius: 4 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#7a7f94', font: { size: 11 } } } },
      scales: {
        x: { stacked: true, grid: { display: false }, ticks: { color: '#7a7f94', font: { size: 10 } } },
        y: { stacked: true, grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#7a7f94', stepSize: 1 } }
      }
    }
  });
}

function renderSchemaSummary(reports) {
  const el = document.getElementById('schema-summary');
  const active = reports.filter(r => r.compatibility_verdict !== 'INSUFFICIENT_SNAPSHOTS');
  el.innerHTML = active.map(r => `
    <div style="padding:10px 0;border-bottom:1px solid var(--border)">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
        <span style="font-family:var(--font);font-size:11px;color:var(--accent)">${r.contract_id}</span>
        <span class="badge ${r.compatibility_verdict === 'BREAKING' ? 'badge-critical' : 'badge-pass'}">
          ${r.compatibility_verdict}
        </span>
      </div>
      <div style="font-size:11px;color:var(--muted)">${r.total_changes||0} changes · ${r.breaking_changes||0} breaking</div>
      ${r.breaking_changes > 0 ? `<div style="font-size:11px;color:var(--red);margin-top:4px">${r.action_required||''}</div>` : ''}
    </div>
  `).join('') || '<div class="empty-state">No schema evolution data</div>';
}

function renderSchemaChangesList(reports) {
  const el = document.getElementById('schema-changes-list');
  const changes = reports.flatMap(r =>
    (r.changes||[]).filter(c => c.change_type !== 'no_change').map(c => ({...c, contract_id: r.contract_id}))
  );
  if (!changes.length) { el.innerHTML = '<div class="empty-state">No material changes detected</div>'; return; }
  el.innerHTML = changes.map(c => `
    <div class="change-item">
      <div class="change-icon" style="color:${c.compatible ? 'var(--green)' : 'var(--red)'}">
        ${c.compatible ? '✓' : '✗'}
      </div>
      <div style="flex:1">
        <div style="display:flex;align-items:center;gap:8px">
          <span class="change-field">${c.field || ''}</span>
          <span class="badge ${c.compatible ? 'badge-pass' : 'badge-critical'}">${c.change_type||''}</span>
        </div>
        <div class="change-type" style="margin-top:2px">${c.contract_id} · ${c.description||''}</div>
      </div>
    </div>
  `).join('');
}

// ── AI Extensions ─────────────────────────────────────────────────────

async function loadAIExtensions() {
  try {
    const data = await fetch(`${API}/api/ai-extensions`).then(r => r.json());
    if (data.error) { document.getElementById('ai-status').textContent = data.error; return; }

    const ext = data.extensions || {};
    const drift = ext.embedding_drift || {};
    const viol = ext.output_violation_rate_verdicts || {};
    const trace = ext.trace_schema_check || {};
    const overall = data.overall_status || 'UNKNOWN';

    document.getElementById('drift-score').textContent = drift.drift_score !== undefined ? drift.drift_score.toFixed(4) : '--';
    document.getElementById('drift-status').textContent = drift.status || '';
    document.getElementById('drift-score').className = 'stat-val ' + (drift.status === 'FAIL' ? 'red' : drift.status === 'WARN' ? 'amber' : 'green');

    document.getElementById('viol-rate').textContent = viol.violation_rate !== undefined ? (viol.violation_rate * 100).toFixed(1) + '%' : '--';
    document.getElementById('viol-trend').textContent = 'Trend: ' + (viol.trend || 'unknown');

    document.getElementById('trace-status-val').textContent = trace.status || '--';
    document.getElementById('trace-count').textContent = trace.total_traces ? `${trace.total_traces} traces` : '';

    const overallEl = document.getElementById('ai-overall');
    overallEl.textContent = overall;
    overallEl.className = 'stat-val ' + (overall === 'PASS' ? 'green' : overall === 'WARN' ? 'amber' : 'red');

    document.getElementById('ai-status').textContent = `Overall: ${overall}`;

    document.getElementById('drift-detail').innerHTML = `
      <div style="font-size:12px;color:var(--text);line-height:1.7;margin-bottom:12px">${drift.interpretation || ''}</div>
      <div style="font-size:11px;color:var(--muted)">
        Threshold: ${drift.threshold || 0.15} · Sample size: ${drift.sample_size || 0} texts<br>
        Method: cosine distance from centroid baseline (OpenRouter text-embedding-3-small)
      </div>
    `;

    const w3 = ext.prompt_input_validation_week3 || {};
    const w2 = ext.prompt_input_validation_week2 || {};
    document.getElementById('prompt-detail').innerHTML = `
      <div style="font-size:12px;margin-bottom:10px">
        <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border)">
          <span>Week 3 extractions</span>
          <span>
            <span class="badge badge-pass">${w3.valid || 0} valid</span>
            ${w3.quarantined > 0 ? `<span class="badge badge-critical">${w3.quarantined} quarantined</span>` : ''}
          </span>
        </div>
        <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border)">
          <span>Week 2 verdicts</span>
          <span>
            <span class="badge badge-pass">${w2.valid || 0} valid</span>
            ${w2.quarantined > 0 ? `<span class="badge badge-critical">${w2.quarantined} quarantined</span>` : ''}
          </span>
        </div>
        <div style="display:flex;justify-content:space-between;padding:8px 0">
          <span>Output violation rate</span>
          <span class="badge ${viol.status === 'PASS' ? 'badge-pass' : 'badge-warn'}">${(viol.violation_rate||0)*100}% ${viol.status||''}</span>
        </div>
      </div>
    `;
  } catch (e) {}
}

// ── Registry ──────────────────────────────────────────────────────────

async function loadRegistry() {
  try {
    const data = await fetch(`${API}/api/registry`).then(r => r.json());
    allSubscriptions = data.subscriptions || [];
    renderRegistry(allSubscriptions);
  } catch (e) {}
}

function renderRegistry(subs) {
  const el = document.getElementById('registry-list');
  if (!subs.length) { el.innerHTML = '<div class="empty-state">No subscriptions found</div>'; return; }
  el.innerHTML = `
    <div style="font-size:11px;color:var(--muted);margin-bottom:12px">${subs.length} subscriptions registered</div>
    ${subs.map(s => `
      <div class="sub-item">
        <div class="sub-contracts">
          <span class="sub-contract">${s.contract_id}</span>
          <span class="sub-arrow">→</span>
          <span class="sub-subscriber">${s.subscriber_id}</span>
          <span class="badge badge-${(s.validation_mode||'').toLowerCase()}">${s.validation_mode||''}</span>
        </div>
        <div class="sub-reason">
          Breaking fields: ${(s.breaking_fields||[]).map(f => f.field || f).join(', ')}
        </div>
      </div>
    `).join('')}
  `;
}

function filterRegistry() {
  const search = document.getElementById('registry-search').value.toLowerCase();
  const filtered = allSubscriptions.filter(s =>
    s.contract_id.toLowerCase().includes(search) ||
    s.subscriber_id.toLowerCase().includes(search)
  );
  renderRegistry(filtered);
}

// ── Pipeline commands ─────────────────────────────────────────────────

function showLoading(text = 'Running...') {
  document.getElementById('loading-text').textContent = text;
  document.getElementById('loading-overlay').classList.add('show');
}

function hideLoading() {
  document.getElementById('loading-overlay').classList.remove('show');
}

function appendTerminal(text, type = '') {
  const t = document.getElementById('terminal');
  const line = document.createElement('span');
  if (type) line.className = type;
  line.innerHTML = text + '\n';
  t.appendChild(line);
  t.scrollTop = t.scrollHeight;
}

function clearTerminal() {
  document.getElementById('terminal').innerHTML = '';
}

function terminalFromOutput(result) {
  if (result.stdout) {
    result.stdout.split('\n').forEach(line => {
      if (line.includes('PASS') || line.includes('✅')) appendTerminal(line, 'ok');
      else if (line.includes('FAIL') || line.includes('❌')) appendTerminal(line, 'fail');
      else if (line.includes('WARN') || line.includes('⚠')) appendTerminal(line, 'warn');
      else if (line.includes('Step') || line.includes('Running') || line.includes('===')) appendTerminal(line, 'info');
      else appendTerminal(line);
    });
  }
  if (result.stderr) {
    result.stderr.split('\n').filter(l => l.trim()).forEach(line => appendTerminal(line, 'fail'));
  }
}

async function runValidate() {
  const contractId = document.getElementById('validate-contract').value;
  const violated = document.getElementById('use-violated').checked;
  showLoading('Validating contracts...');
  clearTerminal();
  appendTerminal(`Running ValidationRunner on ${contractId}...`, 'info');
  try {
    const result = await fetch(`${API}/api/run/validate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ contract_id: contractId, violated })
    }).then(r => r.json());

    terminalFromOutput(result);
    if (result.report) {
      const r = result.report;
      appendTerminal(`\n📊  ${r.passed} passed  ${r.failed} failed  ${r.warned} warned  ${r.errored} errored`, r.failed > 0 ? 'fail' : 'ok');
      showLastRun(r);
    }
    await loadStats();
    await loadReport();
    await loadViolations();
  } catch (e) {
    appendTerminal('Error: ' + e.message, 'fail');
  }
  hideLoading();
}

async function runAttribute() {
  showLoading('Attributing violations...');
  clearTerminal();
  appendTerminal('Running ViolationAttributor...', 'info');
  try {
    const result = await fetch(`${API}/api/run/attribute`, { method: 'POST' }).then(r => r.json());
    terminalFromOutput(result);
    await loadViolations();
  } catch (e) {
    appendTerminal('Error: ' + e.message, 'fail');
  }
  hideLoading();
}

async function runSchemaAnalyzer() {
  showLoading('Analyzing schema evolution...');
  clearTerminal();
  appendTerminal('Running SchemaEvolutionAnalyzer...', 'info');
  try {
    const result = await fetch(`${API}/api/run/schema-analyzer`, { method: 'POST' }).then(r => r.json());
    terminalFromOutput(result);
    document.getElementById('schema-status').textContent = result.success ? 'Done' : 'Error';
    await loadSchemaEvolution();
  } catch (e) {
    appendTerminal('Error: ' + e.message, 'fail');
  }
  hideLoading();
}

async function runAIExtensions() {
  showLoading('Running AI extensions...');
  clearTerminal();
  appendTerminal('Running AI Contract Extensions...', 'info');
  try {
    const result = await fetch(`${API}/api/run/ai-extensions`, { method: 'POST' }).then(r => r.json());
    terminalFromOutput(result);
    document.getElementById('ai-status').textContent = result.success ? 'Done' : 'Error';
    await loadAIExtensions();
  } catch (e) {
    appendTerminal('Error: ' + e.message, 'fail');
  }
  hideLoading();
}

async function runReport() {
  showLoading('Generating report...');
  clearTerminal();
  appendTerminal('Running ReportGenerator...', 'info');
  try {
    const result = await fetch(`${API}/api/run/report`, { method: 'POST' }).then(r => r.json());
    terminalFromOutput(result);
    if (result.report) {
      appendTerminal(`\n📊  Health score: ${result.report.data_health_score}/100`, 'ok');
    }
    await loadStats();
    await loadReport();
  } catch (e) {
    appendTerminal('Error: ' + e.message, 'fail');
  }
  hideLoading();
}

async function runFullPipeline() {
  showLoading('Running full pipeline...');
  clearTerminal();
  appendTerminal('Running full enforcement pipeline...', 'head');
  try {
    const result = await fetch(`${API}/api/run/full-pipeline`, { method: 'POST' }).then(r => r.json());
    (result.steps || []).forEach(step => {
      appendTerminal(`\n── ${step.label} ──`, 'info');
      terminalFromOutput(step);
    });
    appendTerminal(`\nPipeline ${result.success ? 'completed successfully' : 'completed with errors'}`, result.success ? 'ok' : 'fail');
    await refreshAll();
  } catch (e) {
    appendTerminal('Error: ' + e.message, 'fail');
  }
  hideLoading();
}

function showLastRun(report) {
  const card = document.getElementById('last-run-card');
  const content = document.getElementById('last-run-content');
  card.style.display = 'block';

  const results = (report.results || []).filter(r => r.status !== 'PASS');
  content.innerHTML = `
    <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
      <span class="badge badge-pass">${report.passed || 0} passed</span>
      ${report.failed > 0 ? `<span class="badge badge-critical">${report.failed} failed</span>` : ''}
      ${report.warned > 0 ? `<span class="badge badge-high">${report.warned} warned</span>` : ''}
      ${report.errored > 0 ? `<span class="badge badge-warn">${report.errored} errored</span>` : ''}
    </div>
    ${results.length ? results.map(r => `
      <div class="violation-card">
        <div class="violation-header">
          <span class="badge badge-${(r.severity||'').toLowerCase()}">${r.severity}</span>
          <span class="badge badge-fail">${r.status}</span>
        </div>
        <div class="violation-check">${r.check_id}</div>
        <div class="violation-vals">actual: ${r.actual_value||'N/A'} → expected: ${r.expected||'N/A'}</div>
      </div>
    `).join('') : '<div class="empty-state">All checks passed</div>'}
  `;
}

// ── Upload & Validate ─────────────────────────────────────────────────

function handleFileSelect(input) {
  if (input.files[0]) setUploadFile(input.files[0]);
}

function handleDrop(event) {
  event.preventDefault();
  document.getElementById('upload-zone').classList.remove('drag-over');
  const file = event.dataTransfer.files[0];
  if (file) setUploadFile(file);
}

function setUploadFile(file) {
  selectedFile = file;
  const info = document.getElementById('upload-file-info');
  info.style.display = 'flex';
  info.innerHTML = `
    <span class="file-info-name">${file.name}</span>
    <span class="file-info-size">${(file.size / 1024).toFixed(1)} KB</span>
  `;
  document.getElementById('upload-btn').disabled = false;
}

async function uploadAndValidate() {
  if (!selectedFile) return;

  const contractId = document.getElementById('upload-contract').value;
  showLoading('Validating your file...');

  try {
    const formData = new FormData();
    formData.append('file', selectedFile);
    formData.append('contract_id', contractId);

    const result = await fetch(`${API}/api/upload`, {
      method: 'POST',
      body: formData
    }).then(r => r.json());

    document.getElementById('upload-results').style.display = 'block';

    if (result.error) {
      document.getElementById('upload-result-title').textContent = 'Error';
      document.getElementById('upload-result-content').innerHTML = `<div class="top-viol-item" style="border-left-color:var(--red)">${result.error}</div>`;
      hideLoading();
      return;
    }

    const report = result.report || {};
    const passed = report.passed || 0;
    const failed = report.failed || 0;
    const total = report.total_checks || 0;
    const pct = total > 0 ? Math.round((passed / total) * 100) : 0;

    document.getElementById('upload-result-title').textContent = `Results for ${result.uploaded_file}`;
    document.getElementById('upload-result-content').innerHTML = `
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
        <span class="badge ${failed > 0 ? 'badge-critical' : 'badge-pass'}">${failed > 0 ? 'VIOLATIONS FOUND' : 'ALL CLEAR'}</span>
        <span class="badge badge-pass">${passed} passed</span>
        ${failed > 0 ? `<span class="badge badge-critical">${failed} failed</span>` : ''}
        <span class="badge badge-info">${pct}% pass rate</span>
      </div>
      <div style="margin-top:12px">
        <div class="progress-bar" style="height:8px">
          <div class="progress-fill" style="width:${pct}%;background:${failed > 0 ? 'var(--red)' : 'var(--green)'}"></div>
        </div>
      </div>
      <div style="margin-top:12px;font-size:12px;color:var(--muted)">
        Validated against: <span style="color:var(--accent);font-family:var(--font)">${result.contract_id}</span>
      </div>
    `;

    const failures = (report.results || []).filter(r => r.status !== 'PASS');
    document.getElementById('upload-checks-list').innerHTML = failures.length ?
      failures.map(r => `
        <div class="violation-card">
          <div class="violation-header">
            <span class="badge badge-${(r.severity||'').toLowerCase()}">${r.severity}</span>
            <span class="badge badge-fail">${r.status}</span>
          </div>
          <div class="violation-check">${r.check_id}</div>
          <div class="violation-vals">actual: ${r.actual_value||'N/A'} → expected: ${r.expected||'N/A'}</div>
          <div class="violation-meta">${r.message || ''}</div>
        </div>
      `).join('') :
      '<div class="empty-state">All checks passed — your data conforms to the contract</div>';

    document.getElementById('upload-results').scrollIntoView({ behavior: 'smooth' });

  } catch (e) {
    document.getElementById('upload-result-content').innerHTML = `<div class="top-viol-item">Error: ${e.message}</div>`;
  }
  hideLoading();
}