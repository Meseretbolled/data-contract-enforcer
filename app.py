"""
app.py — Data Contract Enforcer Web Application
================================================
Flask backend that serves the interactive dashboard and exposes
live API endpoints for running the full enforcement pipeline.

Run:
    python app.py
    Open: http://localhost:5000
"""

from flask import Flask, render_template
from flask_cors import CORS
from api.routes import api_bp
import os

app = Flask(__name__)
CORS(app)

app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload
app.config['BASE_DIR'] = os.path.dirname(os.path.abspath(__file__))

os.makedirs('uploads', exist_ok=True)
os.makedirs('enforcer_report', exist_ok=True)
os.makedirs('violation_log', exist_ok=True)
os.makedirs('validation_reports', exist_ok=True)

app.register_blueprint(api_bp, url_prefix='/api')


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/health')
def health():
    return {'status': 'ok', 'version': '1.0.0'}


if __name__ == '__main__':
    print("\n" + "="*55)
    print("  Data Contract Enforcer — Web Dashboard")
    print("="*55)
    print("  URL: http://localhost:5000")
    print("  API: http://localhost:5000/api")
    print("="*55 + "\n")
    app.run(debug=True, host='0.0.0.0', port=5000)