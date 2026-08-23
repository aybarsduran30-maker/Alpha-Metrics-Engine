import pytest
import pandas as pd
import numpy as np
from fastapi.testclient import TestClient
from main import app, calculate_rsi

client = TestClient(app)

def test_rsi_calculation_all_gains():
    prices = pd.Series([float(i) for i in range(1, 30)])
    rsi = calculate_rsi(prices, period=14)
    valid_rsi = rsi.dropna()
    assert len(valid_rsi) > 0
    assert float(valid_rsi.iloc[-1]) > 90.0

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "operational"
    assert "redis_connected" in data
    assert "latency_ms" in data
    assert data["version"] == "3.5.0"
