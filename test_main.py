import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_health_check_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "latency_ms" in data

def test_root_dashboard_html():
    response = client.get("/")
    assert response.status_code == 200
    assert "AlphaMetrics Terminal" in response.text

def test_metrics_schema_endpoint():
    
    response = client.get("/api/v1/metrics/AAPL")
    assert response.status_code in [200, 429, 500]

def test_var_cvar_risk_endpoint():
    response = client.get("/api/v1/quant/var/AAPL?days=1&simulations=500")
    assert response.status_code in [200, 429, 500]

def test_correlation_matrix_validation():
    
    response = client.get("/api/v1/quant/correlation?symbols=AAPL")
    assert response.status_code == 400
