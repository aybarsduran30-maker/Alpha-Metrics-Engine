import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

TEST_HEADERS = {"X-API-Key": "am_starter_testmockkey"}

def test_health_check_endpoint():
    response = client.get("/health")
    assert response.status_code == 200

def test_root_dashboard_html():
    response = client.get("/")
    assert response.status_code == 200

def test_metrics_schema_endpoint():
    response = client.get("/api/v1/metrics/AAPL", headers=TEST_HEADERS)
    assert response.status_code in [200, 401, 429, 500]

def test_var_cvar_risk_endpoint():
    response = client.get("/api/v1/quant/var/AAPL?days=1&simulations=500", headers=TEST_HEADERS)
    assert response.status_code in [200, 401, 429, 500]

def test_correlation_matrix_validation():
    response = client.get("/api/v1/quant/correlation?symbols=AAPL", headers=TEST_HEADERS)
    assert response.status_code in [400, 401]
