import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import main
from auth_service import consume_api_request, generate_api_key, hash_api_key
from database import Base, get_db
from models import ApiClient


def test_crypto_quote_uses_fallback_when_binance_is_unavailable(monkeypatch):
    dates = pd.date_range("2026-10-01", periods=60, freq="D", tz="UTC")
    daily = pd.DataFrame({"Close": range(100, 160)}, index=dates)
    intraday = pd.DataFrame({"Close": [160.0]}, index=dates[-1:])

    def unavailable(_):
        raise ValueError("upstream unavailable")

    monkeypatch.setattr(main, "process_binance_ticker", unavailable)
    monkeypatch.setattr(main, "get_market_history", lambda _: (daily, intraday))
    main.market_metric_cache.clear()

    metric = main.process_single_ticker_sync("BTC-USD")

    assert metric.price == 160.0
    assert metric.quote_source == "Yahoo Finance crypto quote (Binance unavailable)"
    assert metric.cache_hit is False


def test_backtest_enters_after_signal_close(monkeypatch):
    dates = pd.date_range("2026-01-01", periods=60, freq="D")
    prices = [100.0, 110.0, 121.0, 133.1] + [133.1] * 56
    frame = pd.DataFrame({"Close": prices}, index=dates)
    rsi = pd.Series([50.0, 20.0, 50.0, 80.0] + [50.0] * 56, index=dates)

    class FakeTicker:
        def history(self, **_):
            return frame.copy()

    monkeypatch.setattr(main.yf, "Ticker", lambda _: FakeTicker())
    monkeypatch.setattr(main, "calculate_rsi", lambda *_args, **_kwargs: rsi)

    result = main.run_quant_backtest_sync("TEST")

    assert result.strategy_return_pct == pytest.approx(21.0)
    assert result.buy_and_hold_return_pct == pytest.approx(33.1)


def test_gram_gold_historical_analysis_is_rejected():
    with pytest.raises(main.HTTPException) as error:
        main.require_historical_ticker("GRAM-ALTIN-TRY")
    assert error.value.status_code == 422


def test_invalid_request_does_not_consume_quota_and_usage_stays_visible():
    test_engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(bind=test_engine)
    session_factory = sessionmaker(bind=test_engine)
    key = generate_api_key()

    with session_factory() as db:
        db.add(ApiClient(
            company_name="Pilot", email="pilot@example.com", api_key_hash=hash_api_key(key),
            plan_tier="starter", rate_limit_per_min=60, monthly_quota=1,
            used_requests_this_month=0, is_active=True,
        ))
        db.commit()

    def test_db():
        with session_factory() as db:
            yield db

    main.app.dependency_overrides[get_db] = test_db
    try:
        client = TestClient(main.app)
        headers = {"X-API-Key": key}
        invalid = client.get("/api/v1/quant/backtest/GRAM-ALTIN-TRY", headers=headers)
        assert invalid.status_code == 422
        with session_factory() as db:
            record = db.query(ApiClient).one()
            assert record.used_requests_this_month == 0
            consume_api_request(db, record)

        usage = client.get("/api/v1/auth/usage", headers=headers)
        assert usage.status_code == 200
        assert usage.json()["remaining_requests"] == 0
        assert client.get("/api/v1/quant/var/AAPL", headers=headers).status_code == 429
    finally:
        main.app.dependency_overrides.pop(get_db, None)
        test_engine.dispose()
