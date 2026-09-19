import pytest
from fastapi.testclient import TestClient
from main import app
from auth_service import generate_api_key

client = TestClient(app)

def test_health_check_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "operational"

def test_root_dashboard_html():
    response = client.get("/")
    assert response.status_code == 200

def test_api_key_generation_format():
    key = generate_api_key("enterprise")
    assert key.startswith("am_enterprise_")
    assert len(key) > 20

def test_unauthorized_missing_header():
    response = client.get("/api/v1/quant/var/AAPL")
    assert response.status_code == 401

def test_unauthorized_invalid_key():
    response = client.get(
        "/api/v1/quant/var/AAPL",
        headers={"X-API-Key": "invalid_mock_token"}
    )
    assert response.status_code == 401

def test_correlation_matrix_insufficient_symbols():
    response = client.get("/api/v1/quant/correlation?symbols=AAPL")
    assert response.status_code == 401
