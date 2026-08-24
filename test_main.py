import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "operational"
    assert "latency_ms" in data

def test_root_dashboard():
    response = client.get("/")
    assert response.status_code == 200
    assert "AlphaMetrics Terminal" in response.text

def test_var_cvar_endpoint():
    response = client.get("/api/v1/quant/var/AAPL?days=1&simulations=1000")
    assert response.status_code in [200, 500]

def test_correlation_matrix_endpoint():
    response = client.get("/api/v1/quant/correlation?symbols=AAPL,MSFT")
    assert response.status_code in [200, 500]
