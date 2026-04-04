"""
dashboard.py — Data Contract Enforcer · Navy/Gold Theme
Run: streamlit run dashboard.py
"""
import glob, json, os, subprocess, sys
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yaml

st.set_page_config(
    page_title="Data Contract Enforcer",
    page_icon="🔐",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Force sidebar open via JS — works even when CSS fails
st.markdown("""
<script>
(function() {
    function openSidebar() {
        // Try clicking the expand button if sidebar is collapsed
        var btns = window.parent.document.querySelectorAll(
            'button[data-testid="collapsedControl"]');
        btns.forEach(function(btn) {
            if (btn) btn.click();
        });
        // Also force the sidebar element visible
        var sidebar = window.parent.document.querySelector(
            'section[data-testid="stSidebar"]');
        if (sidebar) {
            sidebar.style.setProperty('transform', 'none', 'important');
            sidebar.style.setProperty('min-width', '300px', 'important');
            sidebar.style.setProperty('width', '300px', 'important');
        }
    }
    // Run immediately and after a short delay
    openSidebar();
    setTimeout(openSidebar, 500);
    setTimeout(openSidebar, 1500);
})();
</script>
""", unsafe_allow_html=True)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {
    --navy:  #1a2340;
    --navy2: #232f50;
    --gold:  #c9a84c;
    --gold2: #e8c96a;
    --off:   #f5f7fa;
    --lgray: #dde3ef;
    --dgray: #6b7494;
    --red:   #c0392b;
    --redl:  #fdf0ee;
    --green: #1a7a4a;
    --greenl:#edfaf3;
    --amber: #b7621a;
    --amberl:#fff8ee;
    --text:  #1a2340;
}

#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
.stApp { background: var(--off) !important; font-family:'Space Grotesk', sans-serif; color: var(--text); }

/* ── Remove ALL top whitespace — tabs flush to sidebar top ── */
[data-testid="stHeader"] { display: none !important; height: 0 !important; }
[data-testid="stToolbar"] { display: none !important; height: 0 !important; }
#stDecoration { display: none !important; height: 0 !important; }
[data-testid="stAppViewContainer"] > [data-testid="stHeader"] { display: none !important; }

.main .block-container {
    padding-top: 0 !important;
    padding-left: 1.5rem !important;
    padding-right: 1.5rem !important;
    max-width: 100% !important;
    margin-top: 0 !important;
}

/* Remove any top margin from the very first element in main */
.main .block-container > div:first-child {
    margin-top: 0 !important;
    padding-top: 0 !important;
}

.stTabs {
    margin-top: 0 !important;
    padding-top: 0 !important;
}

.stTabs [data-baseweb="tab-list"] {
    margin-top: 0 !important;
}

/* Make sure app view container starts at 0 */
[data-testid="stAppViewContainer"] {
    padding-top: 0 !important;
    margin-top: 0 !important;
}

/* ── Sidebar always visible ── */
section[data-testid="stSidebar"],
section[data-testid="stSidebar"][aria-expanded="false"],
section[data-testid="stSidebar"][aria-expanded="true"] {
    width: 300px !important;
    min-width: 300px !important;
    transform: none !important;
    background: #1a2340 !important;
    border-right: 4px solid #c9a84c !important;
}
section[data-testid="stSidebar"] > div:first-child {
    width: 300px !important;
    padding: 1rem !important;
    overflow-y: auto !important;
}
/* Hide collapse button */
button[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"],
.st-emotion-cache-1cypcdb,
button[kind="header"] { display: none !important; }
/* Keep main content from overlapping */
.main .block-container {
    padding-left: 1rem !important;
}

section[data-testid="stSidebar"] * { color: #ffffff !important; font-family:'Space Grotesk', sans-serif !important; }
section[data-testid="stSidebar"] .stButton > button {
    background: #c9a84c !important;
    color: #1a2340 !important;
    border: none !important;
    border-radius: 6px !important;
    font-weight: 700 !important;
    font-size: 14px !important;
    padding: 10px 0 !important;
    width: 100% !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: #e8c96a !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: #1a2340 !important;
    border-radius: 8px !important;
    padding: 5px !important;
    gap: 3px !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: #aab4cc !important;
    border-radius: 6px !important;
    font-family:'Space Grotesk', sans-serif !important;
    font-size: 14px !important;
    font-weight: 600 !important;
    padding: 9px 18px !important;
}
.stTabs [aria-selected="true"] {
    background: #c9a84c !important;
    color: #1a2340 !important;
}

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: white !important;
    border: 1.5px solid #dde3ef !important;
    border-top: 4px solid #1a2340 !important;
    border-radius: 8px !important;
    padding: 14px !important;
}
[data-testid="stMetricLabel"] {
    font-size: 12px !important;
    color: #6b7494 !important;
    font-family:'Space Grotesk', sans-serif !important;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
[data-testid="stMetricValue"] {
    font-size: 28px !important;
    font-weight: 700 !important;
    color: #1a2340 !important;
}

/* ── Expander ── */
.streamlit-expanderHeader {
    background: #1a2340 !important;
    color: white !important;
    border-radius: 6px !important;
    font-family:'Space Grotesk', sans-serif !important;
    font-size: 14px !important;
    font-weight: 700 !important;
    padding: 12px 16px !important;
}
.streamlit-expanderContent {
    background: white !important;
    border: 1.5px solid #dde3ef !important;
    border-top: none !important;
    border-radius: 0 0 6px 6px !important;
    padding: 16px !important;
}

/* ── Inputs / selects ── */
.stSelectbox > div > div,
.stTextInput > div > div {
    background: white !important;
    border: 1.5px solid #dde3ef !important;
    border-radius: 6px !important;
    color: #1a2340 !important;
    font-size: 14px !important;
    font-family: 'JetBrains Mono', monospace !important;
}
.stSelectbox label, .stTextInput label, .stCheckbox label {
    font-size: 14px !important;
    font-family:'Space Grotesk', sans-serif !important;
    font-weight: 600 !important;
    color: #1a2340 !important;
}
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stCheckbox label {
    color: white !important;
}

/* ── File uploader ── */
[data-testid="stFileUploader"] {
    background: white !important;
    border: 2px dashed #c9a84c !important;
    border-radius: 8px !important;
    padding: 12px !important;
}
[data-testid="stFileUploader"] label {
    font-size: 14px !important;
    font-weight: 600 !important;
    color: #1a2340 !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border: 1.5px solid #dde3ef !important;
    border-radius: 8px !important;
}

/* ── General text ── */
p, li, div { font-size: 14px !important; line-height: 1.7 !important; }
h1 { font-size: 24px !important; font-weight: 700 !important; }
h2 { font-size: 19px !important; font-weight: 700 !important; }
h3 { font-size: 16px !important; font-weight: 700 !important; }
code { font-size: 13px !important; font-family: 'JetBrains Mono', monospace !important; }

/* ── Spinner ── */
.stSpinner > div { border-top-color: #c9a84c !important; }

/* ── Alert / info boxes ── */
.stAlert { border-radius: 6px !important; font-size: 14px !important; }
</style>
""", unsafe_allow_html=True)

BASE_DIR = Path(__file__).parent

# ── UI helpers ─────────────────────────────────────────────────────────────────
def section_banner(num, title, subtitle=""):
    return f"""
    <div style="background:#1a2340;border-radius:8px;padding:16px 20px;
                margin-bottom:20px;border-left:5px solid #c9a84c;
                display:flex;align-items:center;gap:16px">
        <div style="font-family:'Space Grotesk',sans-serif;font-size:26px;font-weight:800;
                    color:#c9a84c;min-width:38px">{num}</div>
        <div>
            <div style="font-family:'Space Grotesk',sans-serif;font-size:17px;
                        font-weight:700;color:white">{title}</div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:12px;
                        color:#aab4cc;margin-top:3px">{subtitle}</div>
        </div>
    </div>"""

def info_box(text, color="#1a2340", bg="#eef2ff"):
    return f"""
    <div style="background:{bg};border-left:4px solid {color};border-radius:0 6px 6px 0;
                padding:14px 18px;margin:10px 0;font-size:14px;
                line-height:1.7;color:#1a2340;font-family:'Space Grotesk',sans-serif">{text}</div>"""

def stat_mini(val, label, col="#c9a84c"):
    return f"""
    <div style="background:#232f50;border-top:3px solid {col};border-radius:8px;
                padding:10px 6px;text-align:center;margin-bottom:8px">
        <div style="font-family:'Space Grotesk',sans-serif;font-size:26px;font-weight:700;
                    color:{col};line-height:1.1">{val}</div>
        <div style="font-family:'Space Grotesk',sans-serif;font-size:10px;color:#aab4cc;
                    text-transform:uppercase;letter-spacing:0.6px;margin-top:3px;
                    white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{label}</div>
    </div>"""

def chart_style(fig, title=""):
    fig.update_layout(
        title=dict(text=title, font=dict(family="Syne", size=15, color="#1a2340")),
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="Syne", color="#1a2340", size=13),
        legend=dict(font=dict(family="Syne", size=12), bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(gridcolor="#dde3ef", linecolor="#dde3ef", tickfont=dict(size=12)),
        yaxis=dict(gridcolor="#dde3ef", linecolor="#dde3ef", tickfont=dict(size=12)),
        margin=dict(t=50, b=20, l=20, r=20),
    )
    return fig

# ── Data loaders ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=5)
def load_report():
    p = BASE_DIR / "enforcer_report" / "report_data.json"
    return json.loads(p.read_text()) if p.exists() else {}

@st.cache_data(ttl=5)
def load_violations():
    p = BASE_DIR / "violation_log" / "violations.jsonl"
    if not p.exists(): return []
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]

@st.cache_data(ttl=5)
def load_contracts():
    out = []
    for p in sorted(glob.glob(str(BASE_DIR / "generated_contracts" / "*.yaml"))):
        if "_dbt" in p: continue
        try:
            d = yaml.safe_load(Path(p).read_text())
            out.append({"id": d.get("id", Path(p).stem),
                        "clauses": len(d.get("schema", {})), "data": d})
        except: pass
    return out

@st.cache_data(ttl=5)
def load_val_reports():
    out = []
    for p in sorted(glob.glob(str(BASE_DIR / "validation_reports" / "*.json")), reverse=True):
        if any(x in p for x in ["schema_evolution","ai_extensions"]): continue
        try: out.append(json.loads(Path(p).read_text()))
        except: pass
    return out

@st.cache_data(ttl=5)
def load_registry():
    p = BASE_DIR / "contract_registry" / "subscriptions.yaml"
    return yaml.safe_load(p.read_text()).get("subscriptions", []) if p.exists() else []

@st.cache_data(ttl=5)
def load_schema_evo():
    p = BASE_DIR / "validation_reports" / "schema_evolution_all.json"
    return json.loads(p.read_text()) if p.exists() else {}

@st.cache_data(ttl=5)
def load_ai():
    p = BASE_DIR / "validation_reports" / "ai_extensions.json"
    return json.loads(p.read_text()) if p.exists() else {}

def clear_all():
    for fn in [load_report, load_violations, load_contracts,
               load_val_reports, load_registry, load_schema_evo, load_ai]:
        fn.clear()

def run_cmd(cmd):
    try:
        venv = BASE_DIR / "venv" / "bin" / "python"
        py = str(venv) if venv.exists() else sys.executable
        full = [py if c == "python" else c for c in cmd]
        r = subprocess.run(full, capture_output=True, text=True,
                          cwd=str(BASE_DIR), timeout=120)
        return r.returncode == 0, r.stdout + ("\n" + r.stderr if r.stderr else "")
    except Exception as e:
        return False, str(e)

# ── SIDEBAR ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:16px 0 20px">
        <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#aab4cc;
                    letter-spacing:2px;text-transform:uppercase;margin-bottom:6px">TRP1 · Week 7</div>
        <div style="font-family:'Space Grotesk',sans-serif;font-size:22px;font-weight:800;
                    color:white;line-height:1.2">Data Contract<br>Enforcer</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:11px;
                    color:#c9a84c;margin-top:6px">● Live Dashboard</div>
    </div>
    """, unsafe_allow_html=True)

    report = load_report()
    score  = report.get("data_health_score", 0)
    score_col = "#1a7a4a" if score >= 80 else "#b7621a" if score >= 60 else "#c0392b"

    st.markdown(f"""
    <div style="background:#232f50;border:2px solid {score_col};border-radius:10px;
                padding:16px;margin-bottom:16px;text-align:center">
        <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#aab4cc;
                    letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px">Health Score</div>
        <div style="font-family:'Space Grotesk',sans-serif;font-size:52px;font-weight:800;
                    color:{score_col};line-height:1">{score}</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#aab4cc">/ 100</div>
    </div>
    """, unsafe_allow_html=True)

    violations = load_violations()
    real_viols = [v for v in violations if not v.get("injection_note")]
    contracts  = load_contracts()
    registry   = load_registry()

    # 2x2 grid using a single HTML table — avoids Streamlit column width issues
    st.markdown(f"""
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px">
        <div style="background:#232f50;border-top:3px solid #c9a84c;border-radius:8px;padding:10px 8px;text-align:center">
            <div style="font-family:'Space Grotesk',sans-serif;font-size:26px;font-weight:700;color:#c9a84c">{len(contracts)}</div>
            <div style="font-family:'Space Grotesk',sans-serif;font-size:10px;color:#aab4cc;text-transform:uppercase;letter-spacing:0.5px">Contracts</div>
        </div>
        <div style="background:#232f50;border-top:3px solid #c0392b;border-radius:8px;padding:10px 8px;text-align:center">
            <div style="font-family:'Space Grotesk',sans-serif;font-size:26px;font-weight:700;color:#c0392b">{len(real_viols)}</div>
            <div style="font-family:'Space Grotesk',sans-serif;font-size:10px;color:#aab4cc;text-transform:uppercase;letter-spacing:0.5px">Violations</div>
        </div>
        <div style="background:#232f50;border-top:3px solid #c9a84c;border-radius:8px;padding:10px 8px;text-align:center">
            <div style="font-family:'Space Grotesk',sans-serif;font-size:26px;font-weight:700;color:#c9a84c">{len(registry)}</div>
            <div style="font-family:'Space Grotesk',sans-serif;font-size:10px;color:#aab4cc;text-transform:uppercase;letter-spacing:0.5px">Subscriptions</div>
        </div>
        <div style="background:#232f50;border-top:3px solid #1a7a4a;border-radius:8px;padding:10px 8px;text-align:center">
            <div style="font-family:'Space Grotesk',sans-serif;font-size:26px;font-weight:700;color:#1a7a4a">{sum(c['clauses'] for c in contracts)}</div>
            <div style="font-family:'Space Grotesk',sans-serif;font-size:10px;color:#aab4cc;text-transform:uppercase;letter-spacing:0.5px">Clauses</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    if st.button("⟳  Refresh data", use_container_width=True):
        clear_all(); st.rerun()
    st.markdown("""
    <div style="font-family:'JetBrains Mono',monospace;font-size:11px;
                color:#6b7494;text-align:center;margin-top:6px">Auto-refreshes every 5s</div>
    """, unsafe_allow_html=True)

    # ── JSONL Upload Section ──────────────────────────────────────
    st.markdown("""
    <div style="margin-top:24px;padding-top:16px;border-top:1px solid #2d3a60">
        <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#c9a84c;
                    text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px">Upload Data</div>
    </div>
    """, unsafe_allow_html=True)

    uploaded = st.file_uploader(
        "Upload JSONL file",
        type=["jsonl","json"],
        help="Upload a new JSONL data file to validate against a contract"
    )
    if uploaded:
        dest_dir = BASE_DIR / "outputs" / "uploads"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / uploaded.name
        dest_path.write_bytes(uploaded.getvalue())
        st.markdown(f"""
        <div style="background:#0d2e1a;border:1px solid #1a7a4a;border-radius:6px;
                    padding:10px 12px;margin-top:6px;font-family:'JetBrains Mono',monospace;
                    font-size:12px;color:#1a7a4a">
            ✅ Saved to<br>outputs/uploads/{uploaded.name}
        </div>
        """, unsafe_allow_html=True)

        # Quick validate option
        contracts_list = load_contracts()
        if contracts_list:
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            val_contract = st.selectbox(
                "Validate against",
                [c["id"] for c in contracts_list],
                key="sidebar_val_contract"
            )
            val_mode_sb = st.selectbox("Mode", ["AUDIT","WARN","ENFORCE"], key="sidebar_mode")
            if st.button("▶  Validate upload", use_container_width=True):
                st.session_state["run_cmd"] = (
                    f"Validating {uploaded.name}...",
                    ["python","contracts/runner.py",
                     "--contract", f"generated_contracts/{val_contract}.yaml",
                     "--data", str(dest_path),
                     "--output", f"validation_reports/upload_{uploaded.name.replace('.jsonl','')}.json",
                     "--mode", val_mode_sb]
                )
                st.rerun()

# ── MAIN TABS ─────────────────────────────────────────────────────────────────
t_ov, t_co, t_vi, t_re, t_sc, t_ai, t_pi = st.tabs([
    "Overview", "Contracts", "Violations", "Registry",
    "Schema Evolution", "AI Extensions", "Run Pipeline"
])

# ══════════════════════════════════════════════
# OVERVIEW
# ══════════════════════════════════════════════
with t_ov:
    report = load_report()
    st.markdown(section_banner("01", "Enforcer Report Overview",
        "Health score · violations · narrative · recommended actions"),
        unsafe_allow_html=True)

    if not report:
        st.warning("No report found. Run the pipeline first.")
    else:
        narrative = report.get("health_narrative","")
        st.markdown(info_box(f"<b>Health narrative</b><br>{narrative}",
            color="#1a2340", bg="#eef2ff"), unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        sev = report.get("violations_by_severity", {})
        with col1:
            if sev:
                fig = go.Figure(go.Pie(
                    labels=list(sev.keys()), values=list(sev.values()), hole=0.55,
                    marker=dict(
                        colors=["#c0392b","#b7621a","#c9a84c","#1a7a4a"],
                        line=dict(color="white", width=3)),
                    textfont=dict(family="Syne", size=14),
                ))
                chart_style(fig, "Violations by severity")
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            summaries = report.get("contracts_summary", [])
            if summaries:
                df_s = pd.DataFrame(summaries)
                df_s["label"] = df_s["contract_id"].str.replace("week","w").str[:18]
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(
                    x=df_s["label"], y=df_s["passed"], name="Passed",
                    marker_color="#1a7a4a",
                    text=df_s["passed"], textposition="inside",
                    textfont=dict(family="Syne", size=13, color="white")))
                fig2.add_trace(go.Bar(
                    x=df_s["label"], y=df_s["failed"], name="Failed",
                    marker_color="#c0392b",
                    text=df_s["failed"], textposition="inside",
                    textfont=dict(family="Syne", size=13, color="white")))
                fig2.update_layout(barmode="stack")
                chart_style(fig2, "Contract pass / fail")
                fig2.update_layout(height=300)
                st.plotly_chart(fig2, use_container_width=True)

        recs = report.get("recommended_actions", [])
        if recs:
            st.markdown("""
            <div style="font-family:'Space Grotesk',sans-serif;font-size:16px;font-weight:700;
                        color:#1a2340;margin:20px 0 12px">Recommended actions</div>
            """, unsafe_allow_html=True)
            rec_colors = ["#c0392b","#b7621a","#1a7a4a"] + ["#1a2340"]*20
            for i, rec in enumerate(recs):
                c = rec_colors[i]
                st.markdown(f"""
                <div style="background:white;border:1.5px solid #dde3ef;border-left:5px solid {c};
                            border-radius:0 8px 8px 0;padding:14px 18px;margin-bottom:10px;
                            display:flex;gap:14px;align-items:flex-start">
                    <span style="font-family:'JetBrains Mono',monospace;font-size:14px;
                                 font-weight:700;color:{c};flex-shrink:0">{str(i+1).zfill(2)}</span>
                    <span style="font-family:'Space Grotesk',sans-serif;font-size:14px;
                                 color:#1a2340;line-height:1.7">{rec}</span>
                </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# CONTRACTS
# ══════════════════════════════════════════════
with t_co:
    contracts = load_contracts()
    st.markdown(section_banner("02", "Generated Contracts",
        "Bitol YAML · 6 contracts · 96 total clauses"), unsafe_allow_html=True)

    if not contracts:
        st.warning("No contracts found in generated_contracts/")
    else:
        sel = st.selectbox("Select contract", [c["id"] for c in contracts])
        c_obj = next((c for c in contracts if c["id"] == sel), None)
        if c_obj:
            d = c_obj["data"]
            schema = d.get("schema", {})
            col1, col2 = st.columns([1, 2])
            with col1:
                info = d.get("info", {})
                downstream = d.get("lineage", {}).get("downstream", [])
                st.markdown(f"""
                <div style="background:white;border:1.5px solid #dde3ef;border-top:4px solid #c9a84c;
                            border-radius:8px;padding:18px;margin-bottom:12px">
                    <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#c9a84c;
                                text-transform:uppercase;letter-spacing:1px;margin-bottom:12px">Contract info</div>
                    <div style="font-family:'JetBrains Mono',monospace;font-size:13px;
                                color:#1a2340;line-height:2.2">
                        <span style="color:#6b7494">id:</span> {d.get("id","")}<br>
                        <span style="color:#6b7494">version:</span> {info.get("version","1.0.0")}<br>
                        <span style="color:#6b7494">clauses:</span> <b>{len(schema)}</b><br>
                        <span style="color:#6b7494">owner:</span> {info.get("owner","")}<br>
                        <span style="color:#6b7494">api:</span> {d.get("apiVersion","")}
                    </div>
                </div>""", unsafe_allow_html=True)
                if downstream:
                    ds_html = "".join(
                        f'<div style="font-family:JetBrains Mono,monospace;font-size:12px;color:#b7621a;margin-bottom:4px">→ {x.get("id","")}</div>'
                        for x in downstream)
                    st.markdown(f"""
                    <div style="background:white;border:1.5px solid #dde3ef;border-top:4px solid #b7621a;
                                border-radius:8px;padding:16px">
                        <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#b7621a;
                                    text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">
                            Downstream ({len(downstream)})</div>
                        {ds_html}
                    </div>""", unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <div style="font-family:'Space Grotesk',sans-serif;font-size:15px;font-weight:700;
                            color:#1a2340;margin-bottom:10px">Schema clauses ({len(schema)})</div>
                """, unsafe_allow_html=True)
                rows = []
                for field, clause in schema.items():
                    rows.append({
                        "Field": field,
                        "Type": clause.get("type",""),
                        "Required": "✅" if clause.get("required") else "○",
                        "Constraint": (f"[{clause['minimum']}, {clause['maximum']}]"
                                       if clause.get("minimum") is not None
                                       else clause.get("format", str(clause.get("enum",""))[:30])),
                        "LLM annotated": "🤖" if clause.get("llm_business_rule") else "",
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True,
                             hide_index=True, height=420)
            with st.expander("📄  View raw YAML"):
                st.code(yaml.dump(d, default_flow_style=False, sort_keys=False), language="yaml")

# ══════════════════════════════════════════════
# VIOLATIONS
# ══════════════════════════════════════════════
with t_vi:
    violations = load_violations()
    real = [v for v in violations if not v.get("injection_note")]
    inj  = [v for v in violations if v.get("injection_note")]
    st.markdown(section_banner("03", "Violation Deep-Dive",
        "Blast radius · lineage BFS · git blame attribution"), unsafe_allow_html=True)
    if inj:
        x = inj[0]
        st.markdown(info_box(
            f"<b>Injection note</b> — {x.get('injection_type','')} · "
            f"{x.get('injection_description','')}", color="#c9a84c", bg="#fffbee"),
            unsafe_allow_html=True)
    if not real:
        st.info("No violations in log.")
    else:
        sev_filter = st.selectbox("Filter by severity", ["ALL","CRITICAL","HIGH","MEDIUM","LOW"])
        filtered = real if sev_filter == "ALL" else [v for v in real if v.get("severity") == sev_filter]
        for v in filtered:
            sev = v.get("severity","?")
            col = {"CRITICAL":"#c0392b","HIGH":"#b7621a","MEDIUM":"#c9a84c","LOW":"#1a7a4a"}.get(sev,"#6b7494")
            with st.expander(f"[{sev}]  {v.get('check_id','')[:70]}"):
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown(f"""
                    <div style="background:white;border:1.5px solid #dde3ef;border-top:4px solid {col};
                                border-radius:8px;padding:16px">
                        <div style="font-family:'JetBrains Mono',monospace;font-size:13px;
                                    color:#1a2340;line-height:2.3">
                            <span style="color:#6b7494">severity:</span>
                            <b style="color:{col}">{sev}</b><br>
                            <span style="color:#6b7494">actual:</span> {v.get('actual_value','')}<br>
                            <span style="color:#6b7494">expected:</span> {v.get('expected','')}<br>
                            <span style="color:#6b7494">detected:</span> {v.get('detected_at','')[:16]}
                        </div>
                    </div>""", unsafe_allow_html=True)
                with c2:
                    br = v.get("blast_radius", {})
                    subs  = br.get("direct_subscribers", [])
                    trans = br.get("lineage_transitive_nodes", [])
                    depth = br.get("contamination_depth", 0)
                    if subs:
                        subs_html  = "".join(f'<div style="font-family:JetBrains Mono,monospace;font-size:12px;color:#b7621a;margin-bottom:4px">→ {s.get("subscriber_id",s)} [{s.get("validation_mode","")}]</div>' for s in subs)
                        trans_html = "".join(f'<div style="font-family:JetBrains Mono,monospace;font-size:11px;color:#6b7494;margin-bottom:2px">∿ {t}</div>' for t in trans)
                        st.markdown(f"""
                        <div style="background:white;border:1.5px solid #dde3ef;border-top:4px solid #c0392b;
                                    border-radius:8px;padding:16px;margin-bottom:10px">
                            <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#c0392b;
                                        text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">
                                Blast radius · depth {depth}</div>
                            {subs_html}{trans_html}
                        </div>""", unsafe_allow_html=True)
                    chain = v.get("blame_chain",[])
                    if chain:
                        top = chain[0]
                        st.markdown(f"""
                        <div style="background:white;border:1.5px solid #dde3ef;border-top:4px solid #1a2340;
                                    border-radius:8px;padding:16px">
                            <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#1a2340;
                                        text-transform:uppercase;letter-spacing:1px;margin-bottom:10px">
                                Git blame · top candidate</div>
                            <div style="font-family:'JetBrains Mono',monospace;font-size:14px;color:#c9a84c">{top.get("commit_hash","")[:12]}</div>
                            <div style="font-family:'Space Grotesk',sans-serif;font-size:13px;color:#1a2340;margin-top:6px">{top.get("commit_message","")[:65]}</div>
                            <div style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#6b7494;margin-top:4px">{top.get("author","")} · score {top.get("confidence_score",0)}</div>
                        </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# REGISTRY
# ══════════════════════════════════════════════
with t_re:
    registry = load_registry()
    st.markdown(section_banner("04", "Contract Registry",
        "7 subscriptions · tier · failure mode · on_violation_action"), unsafe_allow_html=True)
    if not registry:
        st.warning("Registry not found.")
    else:
        search = st.text_input("Search", placeholder="contract id or subscriber name...")
        filtered_reg = [s for s in registry if
            not search or search.lower() in s.get("contract_id","").lower()
            or search.lower() in s.get("subscriber_id","").lower()]
        for sub in filtered_reg:
            mode    = sub.get("validation_mode","AUDIT")
            mode_col = "#c0392b" if mode=="ENFORCE" else "#b7621a"
            with st.expander(f"{sub['contract_id']}  →  {sub['subscriber_id']}"):
                c1, c2 = st.columns([1,2])
                with c1:
                    st.markdown(f"""
                    <div style="font-family:'JetBrains Mono',monospace;font-size:13px;
                                color:#1a2340;line-height:2.4">
                        <span style="color:#6b7494">mode:</span>
                        <b style="color:{mode_col}">{mode}</b><br>
                        <span style="color:#6b7494">tier:</span> {sub.get("tier","?")}<br>
                        <span style="color:#6b7494">on_violation:</span> {sub.get("on_violation_action","?")}<br>
                        <span style="color:#6b7494">contact:</span> {sub.get("contact","?")}
                    </div>""", unsafe_allow_html=True)
                with c2:
                    for bf in sub.get("breaking_fields",[]):
                        st.markdown(f"""
                        <div style="background:#fdf0ee;border-left:3px solid #c0392b;
                                    border-radius:0 6px 6px 0;padding:8px 12px;margin-bottom:6px;
                                    font-family:'JetBrains Mono',monospace;font-size:12px">
                            <b style="color:#c0392b">{bf.get("field","")}</b>
                            <span style="color:#6b7494;margin-left:8px">{bf.get("reason","")[:70]}</span>
                        </div>""", unsafe_allow_html=True)
                    fm = sub.get("failure_mode_description","")
                    if fm:
                        st.markdown(info_box(f"<b>Failure mode:</b> {fm[:200]}"), unsafe_allow_html=True)

# ══════════════════════════════════════════════
# SCHEMA EVOLUTION
# ══════════════════════════════════════════════
with t_sc:
    evo = load_schema_evo()
    st.markdown(section_banner("05", "Schema Evolution",
        "Snapshot diffing · taxonomy · migration checklist · per-consumer analysis"),
        unsafe_allow_html=True)
    if not evo:
        st.info("No schema evolution report. Run schema_analyzer first.")
    else:
        m1,m2,m3 = st.columns(3)
        m1.metric("Contracts analyzed", evo.get("contracts_analyzed",0))
        m2.metric("Total changes", evo.get("total_changes",0))
        m3.metric("Breaking", evo.get("total_breaking",0))
        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        for rep in evo.get("reports",[]):
            verdict  = rep.get("compatibility_verdict","")
            vcol     = "#c0392b" if verdict=="BREAKING" else "#1a7a4a"
            total_ch = rep.get("total_changes",0)
            with st.expander(f"{rep['contract_id']}  —  {total_ch} changes  [{verdict}]"):
                c1,c2 = st.columns(2)
                with c1:
                    st.markdown(f"<div style='font-family:'JetBrains Mono',monospace;font-size:12px;color:#c9a84c;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px'>Migration checklist</div>", unsafe_allow_html=True)
                    for item in rep.get("migration_checklist",[]):
                        st.markdown(f"<div style='font-family:'JetBrains Mono',monospace;font-size:12px;color:#1a2340;margin-bottom:6px;padding-left:8px'>□ {item}</div>", unsafe_allow_html=True)
                with c2:
                    pca = rep.get("per_consumer_failure_analysis",[])
                    if pca:
                        st.markdown("<div style='font-family:'JetBrains Mono',monospace;font-size:12px;color:#c0392b;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px'>Per-consumer impact</div>", unsafe_allow_html=True)
                        for p in pca:
                            st.markdown(f"""
                            <div style="background:#fdf0ee;border-left:3px solid #c0392b;
                                        border-radius:0 6px 6px 0;padding:8px 12px;margin-bottom:6px">
                                <b style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#b7621a">{p.get("field","")}</b>
                                <span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#c0392b;margin-left:8px">[{p.get("triage_label","")}]</span>
                                <div style="font-family:'Space Grotesk',sans-serif;font-size:13px;color:#6b7494;margin-top:4px">{p.get("narrative","")[:100]}</div>
                            </div>""", unsafe_allow_html=True)
                    else:
                        for ch in rep.get("changes",[])[:6]:
                            icon = "❌" if not ch.get("compatible") else "✅"
                            st.markdown(f"<div style='font-family:'JetBrains Mono',monospace;font-size:12px;color:#1a2340;margin-bottom:4px'>{icon} {ch.get('field','')} — {ch.get('change_type','')}</div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# AI EXTENSIONS
# ══════════════════════════════════════════════
with t_ai:
    ai = load_ai()
    st.markdown(section_banner("06", "AI Contract Extensions",
        "Embedding drift · prompt validation · output violation rate"),
        unsafe_allow_html=True)
    if not ai:
        st.info("No AI extensions report found.")
    else:
        overall = ai.get("overall_status","UNKNOWN")
        ocol = {"PASS":"#1a7a4a","WARN":"#b7621a","FAIL":"#c0392b"}.get(overall,"#6b7494")
        st.markdown(f"""
        <div style="background:white;border:1.5px solid #dde3ef;border-top:4px solid {ocol};
                    border-radius:8px;padding:20px;margin-bottom:20px;
                    display:flex;align-items:center;gap:20px">
            <div style="font-family:'Space Grotesk',sans-serif;font-size:36px;font-weight:800;color:{ocol}">{overall}</div>
            <div>
                <div style="font-family:'Space Grotesk',sans-serif;font-size:16px;font-weight:700;color:#1a2340">Overall AI Contract Status</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#6b7494">Run at {ai.get("run_at","")[:16]}</div>
            </div>
        </div>""", unsafe_allow_html=True)

        exts = ai.get("extensions",{})
        ext_keys = [
            ("embedding_drift","1 — Embedding Drift"),
            ("prompt_input_validation_week3","2a — Prompt Input Validation (Week 3)"),
            ("prompt_input_validation_week2","2b — Prompt Input Validation (Week 2)"),
            ("output_violation_rate_verdicts","3 — Output Violation Rate"),
            ("trace_schema_check","4 — Trace Schema Check"),
        ]
        for key, label in ext_keys:
            ext = exts.get(key,{})
            ext_st = ext.get("status","UNKNOWN")
            ecol = {"PASS":"#1a7a4a","BASELINE_SET":"#c9a84c","WARN":"#b7621a","FAIL":"#c0392b"}.get(ext_st,"#6b7494")
            drift = ext.get("drift_score","")
            valid = ext.get("valid","")
            total = ext.get("total_traces","")
            extra = f"drift score: {drift}" if drift!="" else (f"valid: {valid} · quarantined: {ext.get('quarantined',0)}" if valid!="" else (f"traces: {total} · violations: {ext.get('violations',0)}" if total!="" else ""))
            st.markdown(f"""
            <div style="background:white;border:1.5px solid #dde3ef;border-left:5px solid {ecol};
                        border-radius:0 8px 8px 0;padding:14px 18px;margin-bottom:10px;
                        display:flex;align-items:center;gap:16px">
                <div style="font-family:'Space Grotesk',sans-serif;font-size:18px;font-weight:800;
                            color:{ecol};min-width:70px">{ext_st}</div>
                <div>
                    <div style="font-family:'Space Grotesk',sans-serif;font-size:14px;font-weight:700;color:#1a2340">{label}</div>
                    <div style="font-family:'JetBrains Mono',monospace;font-size:12px;color:#6b7494;margin-top:2px">{extra}</div>
                </div>
            </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# RUN PIPELINE
# ══════════════════════════════════════════════
with t_pi:
    st.markdown(section_banner("07", "Run Pipeline",
        "Execute enforcement commands · live terminal output"), unsafe_allow_html=True)

    contracts = load_contracts()
    cids = [c["id"] for c in contracts]
    col_l, col_r = st.columns([1,1])

    with col_l:
        st.markdown("<div style='font-family:'JetBrains Mono',monospace;font-size:12px;color:#c9a84c;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px'>Generate contract</div>", unsafe_allow_html=True)
        src_map = {
            "outputs/week1/intent_records.jsonl": "week1-intent-records",
            "outputs/week2/verdicts.jsonl": "week2-verdict-records",
            "outputs/week3/extractions.jsonl": "week3-document-refinery-extractions",
            "outputs/week4/lineage_snapshots.jsonl": "week4-lineage-snapshots",
            "outputs/week5/events.jsonl": "week5-event-records",
            "outputs/traces/runs.jsonl": "langsmith-traces",
        }
        gen_src  = st.selectbox("Data source", list(src_map.keys()))
        no_llm   = st.checkbox("Skip LLM annotation (--no-llm)", value=False)
        if st.button("▶  Generate", use_container_width=True, key="btn_gen"):
            cmd = ["python","contracts/generator.py",
                   "--source", gen_src,
                   "--contract-id", src_map[gen_src],
                   "--lineage","outputs/week4/lineage_snapshots.jsonl",
                   "--output","generated_contracts/"]
            if no_llm: cmd.append("--no-llm")
            st.session_state["run_cmd"] = ("Generating...", cmd)

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        st.markdown("<div style='font-family:'JetBrains Mono',monospace;font-size:12px;color:#c9a84c;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px'>Validate</div>", unsafe_allow_html=True)
        val_c    = st.selectbox("Contract", cids, key="pi_contract")
        val_mode = st.selectbox("Enforcement mode", ["AUDIT","WARN","ENFORCE"])
        use_viol = st.checkbox("Use violated data (week3 only)", value=False)
        if st.button("▶  Validate", use_container_width=True, key="btn_val"):
            dm = {"week3-document-refinery-extractions":
                  "outputs/week3/extractions_violated.jsonl" if use_viol
                  else "outputs/week3/extractions.jsonl"}
            data_f = dm.get(val_c, f"outputs/")
            st.session_state["run_cmd"] = (f"Validating [{val_mode}]...", [
                "python","contracts/runner.py",
                "--contract", f"generated_contracts/{val_c}.yaml",
                "--data", data_f,
                "--output", f"validation_reports/{val_c}_run.json",
                "--mode", val_mode])

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        st.markdown("<div style='font-family:'JetBrains Mono',monospace;font-size:12px;color:#c9a84c;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px'>Quick actions</div>", unsafe_allow_html=True)
        qa1, qa2 = st.columns(2)
        with qa1:
            if st.button("▶  Attribute", use_container_width=True, key="btn_attr"):
                st.session_state["run_cmd"] = ("Attributing...", [
                    "python","contracts/attributor.py",
                    "--violation","validation_reports/week3-document-refinery-extractions_run.json",
                    "--lineage","outputs/week4/lineage_snapshots.jsonl",
                    "--registry","contract_registry/subscriptions.yaml",
                    "--output","violation_log/violations.jsonl"])
            if st.button("▶  Schema analyzer", use_container_width=True, key="btn_schema"):
                st.session_state["run_cmd"] = ("Analyzing...", [
                    "python","contracts/schema_analyzer.py","--all",
                    "--registry","contract_registry/subscriptions.yaml",
                    "--output","validation_reports/schema_evolution_all.json"])
        with qa2:
            if st.button("▶  AI extensions", use_container_width=True, key="btn_ai"):
                st.session_state["run_cmd"] = ("Running AI extensions...", [
                    "python","contracts/ai_extensions.py",
                    "--extractions","outputs/week3/extractions.jsonl",
                    "--verdicts","outputs/week2/verdicts.jsonl",
                    "--traces","outputs/traces/runs.jsonl",
                    "--output","validation_reports/ai_extensions.json",
                    "--violation-log","violation_log/violations.jsonl"])
            if st.button("▶  Generate report", use_container_width=True, key="btn_report"):
                st.session_state["run_cmd"] = ("Generating report...", [
                    "python","contracts/report_generator.py",
                    "--output","enforcer_report/report_data.json"])

    with col_r:
        st.markdown("""
        <div style="background:#1a2340;border-radius:8px 8px 0 0;padding:10px 16px;
                    display:flex;align-items:center;gap:8px">
            <div style="width:10px;height:10px;border-radius:50%;background:#c0392b"></div>
            <div style="width:10px;height:10px;border-radius:50%;background:#b7621a"></div>
            <div style="width:10px;height:10px;border-radius:50%;background:#1a7a4a"></div>
            <span style="font-family:'JetBrains Mono',monospace;font-size:12px;
                         color:#aab4cc;margin-left:8px">terminal</span>
        </div>""", unsafe_allow_html=True)

        terminal = st.empty()

        def render_terminal(text):
            lines = text.split("\n")
            out = []
            for line in lines:
                esc = line.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
                if any(x in line for x in ["FAIL","❌","ERROR","BLOCKED"]):
                    out.append(f'<span style="color:#c0392b">{esc}</span>')
                elif any(x in line for x in ["PASS","✅","Done","saved","written"]):
                    out.append(f'<span style="color:#1a7a4a">{esc}</span>')
                elif any(x in line for x in ["WARN","⚠"]):
                    out.append(f'<span style="color:#b7621a">{esc}</span>')
                elif any(x in line for x in ["Step","===","🤖","📊","📐","📸"]):
                    out.append(f'<span style="color:#c9a84c">{esc}</span>')
                else:
                    out.append(f'<span style="color:#aab4cc">{esc}</span>')
            terminal.markdown(f"""
            <div style="background:#1a2340;border-radius:0 0 8px 8px;padding:16px;
                        height:520px;overflow-y:auto;font-family:'JetBrains Mono',monospace;
                        font-size:13px;line-height:1.9">
            {"<br>".join(out)}
            </div>""", unsafe_allow_html=True)

        if "run_cmd" in st.session_state:
            label, cmd = st.session_state.pop("run_cmd")
            render_terminal(f"$ {' '.join(cmd)}\n\nRunning...")
            with st.spinner(label):
                ok, out = run_cmd(cmd)
            render_terminal(f"$ {' '.join(cmd)}\n\n{out}")
            clear_all()
        else:
            render_terminal("Ready. Select a command and click ▶ to run.\n\n$ _")