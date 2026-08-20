import pytest
import pandas as pd
import numpy as np
from fastapi.testclient import TestClient
from main import app, calculate_rsi

client = TestClient(app)

def test_rsi_calculation_all_gains():
    prices = pd.Series(np.linspace(10, 100, 30))
    rsi = calculate_rsi(prices, window=14)
    assert rsi == 100.0

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
