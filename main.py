import asyncio
from contextlib import asynccontextmanager
import datetime
import json
import os
import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import numpy as np
import pandas as pd
import httpx
from pydantic import BaseModel
from sqlalchemy.orm import Session
import yfinance as yf

from auth_service import generate_api_key, verify_api_key
from database import Base, engine, get_db
from models import ApiClient

Base.metadata.create_all(bind=engine)

class InMemoryEngine:
    def __init__(self):
        self._cache: Dict[str, tuple[Any, float]] = {}
        self._rate_limits: Dict[str, list[float]] = {}
        self._subscribers: Set[asyncio.Queue] = set()
        self._latest_hft_risk: Optional[str] = None
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[str]:
        now = time.time()
        if key in self._cache:
            val, expire_at = self._cache[key]
            if expire_at == 0 or expire_at > now:
                return val
            del self._cache[key]
        return None

    async def setex(self, key: str, ttl: int, value: str):
        expire_at = time.time() + ttl if ttl > 0 else 0
        self._cache[key] = (value, expire_at)

    async def is_rate_limited(self, client_ip: str, limit: int = 60, window: int = 60) -> bool:
        now = time.time()
        async with self._lock:
            timestamps = self._rate_limits.get(client_ip, [])
            timestamps = [t for t in timestamps if now - t < window]
            if len(timestamps) >= limit:
                self._rate_limits[client_ip] = timestamps
                return True
            timestamps.append(now)
            self._rate_limits[client_ip] = timestamps
            return False

    async def broadcast_hft(self, payload: str):
        self._latest_hft_risk = payload
        disconnected = set()
        for q in self._subscribers:
            try:
                if q.qsize() > 50:
                    try:
                        q.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                q.put_nowait(payload)
            except Exception:
                disconnected.add(q)
        self._subscribers.difference_update(disconnected)

    def subscribe_hft(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=100)
        self._subscribers.add(q)
        return q

    def unsubscribe_hft(self, q: asyncio.Queue):
        self._subscribers.discard(q)

    def get_latest_hft(self) -> Optional[str]:
        return self._latest_hft_risk

engine_mem = InMemoryEngine()
market_metric_cache: Dict[str, tuple[float, "MarketRiskMetric"]] = {}
market_metric_cache_lock = threading.Lock()

def calculate_hft_risk_math(prices: list, horizon_ticks: int = 10, simulations: int = 2000):
    if len(prices) < 15:
        return None
    price_series = np.array(prices, dtype=np.float64)
    returns = np.diff(price_series) / price_series[:-1]
    mean_ret = float(np.mean(returns))
    std_ret = float(np.std(returns))
    current_price = float(price_series[-1])
    if std_ret == 0:
        return None
    sim_returns = np.random.normal(
        (mean_ret - 0.5 * std_ret**2) * horizon_ticks,
        std_ret * np.sqrt(horizon_ticks),
        simulations,
    )
    sim_price_changes = current_price * sim_returns
    var_95 = float(np.percentile(sim_price_changes, 5))
    var_99 = float(np.percentile(sim_price_changes, 1))
    cvar_95 = float(sim_price_changes[sim_price_changes <= var_95].mean())
    return {
        "current_price": round(current_price, 2),
        "tick_window": len(prices),
        "tick_volatility": round(std_ret, 6),
        "var_95": round(abs(var_95), 4),
        "var_99": round(abs(var_99), 4),
        "cvar_95": round(abs(cvar_95), 4),
        "updated_at": time.time(),
    }

async def background_lob_engine():
    prices = []
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                response = await client.get(
                    "https://data-api.binance.vision/api/v3/ticker/bookTicker",
                    params={"symbol": "BTCUSDT"},
                )
                response.raise_for_status()
                quote = response.json()
                best_bid = float(quote["bidPrice"])
                best_ask = float(quote["askPrice"])
                bid_size = float(quote["bidQty"])
                ask_size = float(quote["askQty"])
                current_price = (best_bid + best_ask) / 2
                prices.append(current_price)
                if len(prices) > 300:
                    prices = prices[-300:]

                metrics = calculate_hft_risk_math(prices) or {}
                metrics.update({
                    "status": "live",
                    "symbol": "BTC/USDT",
                    "exchange": "Binance Spot",
                    "current_price": current_price,
                    "spread": best_ask - best_bid,
                    "ofi": (bid_size - ask_size) / (bid_size + ask_size)
                    if bid_size + ask_size else 0.0,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "data_as_of": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                })
                await engine_mem.broadcast_hft(json.dumps(metrics))
            except Exception:
                await engine_mem.broadcast_hft(json.dumps({
                    "status": "unavailable",
                    "symbol": "BTC/USDT",
                    "exchange": "Binance Spot",
                    "data_as_of": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }))
            await asyncio.sleep(1.0)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(background_lob_engine())
    yield
    task.cancel()

app = FastAPI(
    title="AlphaMetrics Financial Intelligence API",
    version="4.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

allowed_origins = [
    origin.strip()
    for origin in os.getenv(
        "ALPHAMETRICS_ALLOWED_ORIGINS",
        "https://alpha-metrics-engine.onrender.com,http://localhost:3000,http://127.0.0.1:3000",
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key", "X-Billing-Webhook-Secret"],
)

@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if request.url.scheme == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.url.path in ["/health", "/docs", "/openapi.json", "/", "/api/v1/billing/webhook"]:
        return await call_next(request)

    client_ip = request.client.host if request.client else "unknown"
    if await engine_mem.is_rate_limited(client_ip, limit=60, window=60):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Maximum 60 requests per minute.",
        )

    return await call_next(request)

class ClientRegisterSchema(BaseModel):
    company_name: str
    email: str
    tier: str = "starter"

class MarketRiskMetric(BaseModel):
    ticker: str
    price: float
    change_percent: float
    currency: str
    fifty_day_average: float
    rsi_14: float
    momentum_status: str
    generated_at: str
    quote_source: str = "Yahoo Finance"
    data_as_of: Optional[str] = None
    cache_hit: bool = False

class BacktestResult(BaseModel):
    ticker: str
    period: str
    strategy_return_pct: float
    buy_and_hold_return_pct: float
    sharpe_ratio: float
    max_drawdown_pct: float
    total_trades: int
    win_rate_pct: float
    analysis_date: str

class PortfolioRiskAnalysis(BaseModel):
    assets: List[str]
    correlation_matrix: dict
    average_correlation: float
    diversification_score: str
    generated_at: str

class WebhookPayload(BaseModel):
    event_type: str
    api_key: str
    new_tier: str

def log_metric_to_db(
    ticker: str, price: float, change_24h: float, rsi: float, status_desc: str
):
    try:
        from database import AssetMetricHistory, SessionLocal

        db = SessionLocal()
        record = AssetMetricHistory(
            symbol=ticker,
            price=price,
            change_24h=change_24h,
            fourteen_d_rsi=rsi,
            risk_status=status_desc,
        )
        db.add(record)
        db.commit()
        db.close()
    except Exception as e:
        print(f"DB Ingestion Warning: {e}")

def calculate_rsi(data: pd.Series, period: int = 14) -> pd.Series:
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

BINANCE_SPOT_SYMBOLS = {
    "BTC-USD": "BTCUSDT",
    "ETH-USD": "ETHUSDT",
    "SOL-USD": "SOLUSDT",
}


def fetch_public_json(base_url: str, params: dict) -> Any:
    request = Request(
        f"{base_url}?{urlencode(params)}",
        headers={"User-Agent": "AlphaMetrics/4.1"},
    )
    with urlopen(request, timeout=8) as response:
        return json.loads(response.read().decode("utf-8"))


def get_market_history(ticker: str):
    instrument = yf.Ticker(ticker)
    daily = instrument.history(period="3mo", interval="1d")
    intraday = instrument.history(period="1d", interval="1m")
    if daily.empty:
        raise ValueError(f"No daily market history available for {ticker}")
    if intraday.empty:
        intraday = daily.tail(1)
    return daily, intraday


def build_metric(ticker, price, change_pct, currency, daily_closes, data_as_of, source):
    daily_closes = pd.Series(daily_closes, dtype="float64").dropna()
    rsi_series = calculate_rsi(daily_closes, period=14)
    rsi_value = rsi_series.iloc[-1] if not rsi_series.empty else 50.0
    rsi_value = 50.0 if pd.isna(rsi_value) else float(rsi_value)
    status_desc = "Overbought" if rsi_value >= 70 else "Oversold" if rsi_value <= 30 else "Neutral"

    return MarketRiskMetric(
        ticker=ticker,
        price=round(float(price), 8 if currency == "USDT" else 2),
        change_percent=round(float(change_pct), 2),
        currency=currency,
        fifty_day_average=round(float(daily_closes.tail(50).mean()), 2),
        rsi_14=round(rsi_value, 2),
        momentum_status=status_desc,
        generated_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        quote_source=source,
        data_as_of=pd.Timestamp(data_as_of).isoformat(),
        cache_hit=False,
    )


def process_binance_ticker(ticker_clean: str) -> MarketRiskMetric:
    symbol = BINANCE_SPOT_SYMBOLS[ticker_clean]
    base = "https://data-api.binance.vision/api/v3"
    quote = fetch_public_json(f"{base}/ticker/24hr", {"symbol": symbol})
    klines = fetch_public_json(
        f"{base}/klines", {"symbol": symbol, "interval": "1d", "limit": 90}
    )
    closes = pd.Series([float(candle[4]) for candle in klines], dtype="float64")
    as_of = datetime.datetime.fromtimestamp(
        int(quote["closeTime"]) / 1000, tz=datetime.timezone.utc
    )
    return build_metric(
        ticker=ticker_clean,
        price=float(quote["lastPrice"]),
        change_pct=float(quote["priceChangePercent"]),
        currency="USDT",
        daily_closes=closes,
        data_as_of=as_of,
        source=f"Binance Spot {symbol}",
    )


def process_single_ticker_sync(ticker_clean: str) -> MarketRiskMetric:
    now = time.monotonic()
    with market_metric_cache_lock:
        cached = market_metric_cache.get(ticker_clean)
        if cached and cached[0] > now:
            return cached[1].model_copy(update={"cache_hit": True})

    if ticker_clean in BINANCE_SPOT_SYMBOLS:
        res_obj = process_binance_ticker(ticker_clean)
    elif ticker_clean == "GRAM-ALTIN-TRY":
        gold_daily, gold_intraday = get_market_history("GC=F")
        fx_daily, fx_intraday = get_market_history("USDTRY=X")

        gold_quote = float(gold_intraday["Close"].iloc[-1])
        fx_quote = float(fx_intraday["Close"].iloc[-1])
        current_price = gold_quote * fx_quote / 31.1034768

        gold_closes = gold_daily["Close"].copy()
        fx_closes = fx_daily["Close"].copy()
        gold_closes.index = pd.to_datetime(gold_closes.index.date)
        fx_closes.index = pd.to_datetime(fx_closes.index.date)
        aligned = pd.concat(
            [gold_closes.rename("gold"), fx_closes.rename("fx")], axis=1
        ).dropna()
        gram_history = aligned["gold"] * aligned["fx"] / 31.1034768
        if gram_history.empty:
            raise ValueError("Could not align gold and USD/TRY history")
        previous_price = float(gram_history.iloc[-1])
        if len(gram_history) > 1:
            previous_price = float(gram_history.iloc[-2])
        change_pct = (current_price - previous_price) / previous_price * 100
        data_as_of = min(gold_intraday.index[-1], fx_intraday.index[-1])
        res_obj = build_metric(
            ticker="GRAM-ALTIN-TRY",
            price=current_price,
            change_pct=change_pct,
            currency="TRY",
            daily_closes=gram_history,
            data_as_of=data_as_of,
            source="Yahoo Finance: GC=F futures × USDTRY=X ÷ 31.1035 (indicative gram estimate)",
        )
    else:
        daily, intraday = get_market_history(ticker_clean)
        daily_closes = daily["Close"].dropna()
        if len(daily_closes) < 2:
            raise ValueError(f"Insufficient daily data for {ticker_clean}")

        current_price = float(intraday["Close"].iloc[-1])
        prev_close = float(daily_closes.iloc[-2])
        change_pct = (current_price - prev_close) / prev_close * 100
        try:
            currency = str(yf.Ticker(ticker_clean).fast_info.currency or "USD")
        except Exception:
            currency = "USD"
        res_obj = build_metric(
            ticker=ticker_clean,
            price=current_price,
            change_pct=change_pct,
            currency=currency,
            daily_closes=daily_closes,
            data_as_of=intraday.index[-1],
            source="Yahoo Finance intraday quote",
        )

    with market_metric_cache_lock:
        market_metric_cache[ticker_clean] = (time.monotonic() + 15, res_obj)

    return res_obj

def run_quant_backtest_sync(ticker_clean: str) -> BacktestResult:
    cache_key = f"backtest:{ticker_clean}"
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            cached_data = asyncio.run_coroutine_threadsafe(
                engine_mem.get(cache_key), loop
            ).result(timeout=1.0)
        else:
            cached_data = loop.run_until_complete(engine_mem.get(cache_key))
        if cached_data:
            return BacktestResult(**json.loads(cached_data))
    except Exception:
        pass

    fetch_ticker = "GC=F" if ticker_clean == "GRAM-ALTIN-TRY" else ticker_clean
    stock = yf.Ticker(fetch_ticker)
    df = stock.history(period="1y", interval="1d")

    if df.empty or len(df) < 50:
        raise ValueError(f"Insufficient history data for {ticker_clean}")

    df["RSI"] = calculate_rsi(df["Close"], period=14)
    df["Daily_Return"] = df["Close"].pct_change().fillna(0)

    position = 0
    trades = []
    strategy_returns = []
    entry_price = 0.0

    for i in range(len(df)):
        rsi = df["RSI"].iloc[i]
        price = df["Close"].iloc[i]

        if position == 0 and rsi < 35:
            position = 1
            entry_price = price
        elif position == 1 and rsi > 65:
            position = 0
            ret = (price - entry_price) / entry_price
            trades.append(ret)

        if position == 1:
            strategy_returns.append(df["Daily_Return"].iloc[i])
        else:
            strategy_returns.append(0.0)

    df["Strategy_Return"] = strategy_returns
    df["Cum_Strategy"] = (1 + df["Strategy_Return"]).cumprod()
    df["Cum_BnH"] = (1 + df["Daily_Return"]).cumprod()

    total_strat_return = round(
        (float(df["Cum_Strategy"].iloc[-1]) - 1.0) * 100, 2
    )
    total_bnh_return = round((float(df["Cum_BnH"].iloc[-1]) - 1.0) * 100, 2)

    active_returns = df.loc[df["Strategy_Return"] != 0, "Strategy_Return"]
    if len(active_returns) > 5 and active_returns.std() > 0:
        sharpe = (active_returns.mean() / active_returns.std()) * np.sqrt(252)
        sharpe_val = round(float(sharpe), 2)
    else:
        sharpe_val = 0.0

    rolling_max = df["Cum_Strategy"].cummax()
    drawdown = (df["Cum_Strategy"] - rolling_max) / rolling_max
    max_dd = round(float(drawdown.min()) * 100, 2)

    total_trades = len(trades)
    wins = [t for t in trades if t > 0]
    win_rate = (
        round((len(wins) / total_trades) * 100, 2) if total_trades > 0 else 0.0
    )

    res = BacktestResult(
        ticker=ticker_clean,
        period="1 Year",
        strategy_return_pct=total_strat_return,
        buy_and_hold_return_pct=total_bnh_return,
        sharpe_ratio=sharpe_val,
        max_drawdown_pct=max_dd,
        total_trades=total_trades,
        win_rate_pct=win_rate,
        analysis_date=datetime.datetime.utcnow().isoformat(),
    )

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(
                engine_mem.setex(cache_key, 300, res.model_dump_json()), loop
            ).result(timeout=1.0)
        else:
            loop.run_until_complete(
                engine_mem.setex(cache_key, 300, res.model_dump_json())
            )
    except Exception:
        pass

    return res

def run_monte_carlo_var_sync(
    ticker_clean: str, days: int = 1, simulations: int = 10000
) -> dict:
    cache_key = f"var_cvar:{ticker_clean}:{days}:{simulations}"
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            cached = asyncio.run_coroutine_threadsafe(
                engine_mem.get(cache_key), loop
            ).result(timeout=1.0)
        else:
            cached = loop.run_until_complete(engine_mem.get(cache_key))
        if cached:
            return json.loads(cached)
    except Exception:
        pass

    fetch_ticker = "GC=F" if ticker_clean == "GRAM-ALTIN-TRY" else ticker_clean
    stock = yf.Ticker(fetch_ticker)
    df = stock.history(period="1y", interval="1d")

    if df.empty or len(df) < 50:
        raise ValueError(
            "Insufficient historical data or upstream rate limited for"
            f" {ticker_clean}"
        )

    close_prices = df["Close"].ffill().dropna()
    daily_returns = close_prices.pct_change().dropna().values

    mean_return = float(np.mean(daily_returns))
    std_dev = float(np.std(daily_returns))
    current_price = float(close_prices.iloc[-1])

    simulated_returns = np.random.normal(
        (mean_return - 0.5 * std_dev**2) * days,
        std_dev * np.sqrt(days),
        simulations,
    )
    simulated_price_changes = current_price * simulated_returns

    var_95 = float(np.percentile(simulated_price_changes, 5))
    var_99 = float(np.percentile(simulated_price_changes, 1))

    cvar_95 = float(
        simulated_price_changes[simulated_price_changes <= var_95].mean()
    )
    cvar_99 = float(
        simulated_price_changes[simulated_price_changes <= var_99].mean()
    )

    result = {
        "ticker": ticker_clean,
        "current_price": round(current_price, 2),
        "time_horizon_days": days,
        "simulations_count": simulations,
        "volatility_annualized_pct": round(
            float(std_dev * np.sqrt(252) * 100), 2
        ),
        "var_95_max_loss": round(abs(var_95), 2),
        "var_99_max_loss": round(abs(var_99), 2),
        "cvar_95_expected_shortfall": round(abs(cvar_95), 2),
        "cvar_99_expected_shortfall": round(abs(cvar_99), 2),
        "risk_summary": (
            f"With 95% statistical confidence, expected loss over {days} day(s)"
            f" will not exceed {abs(round(var_95, 2))} units."
        ),
    }

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(
                engine_mem.setex(cache_key, 3600, json.dumps(result)), loop
            ).result(timeout=1.0)
        else:
            loop.run_until_complete(
                engine_mem.setex(cache_key, 3600, json.dumps(result))
            )
    except Exception:
        pass

    return result

def run_correlation_matrix_sync(symbols: List[str]) -> PortfolioRiskAnalysis:
    cache_key = f"corr_matrix:{'_'.join(sorted(symbols))}"
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            cached = asyncio.run_coroutine_threadsafe(
                engine_mem.get(cache_key), loop
            ).result(timeout=1.0)
        else:
            cached = loop.run_until_complete(engine_mem.get(cache_key))
        if cached:
            return PortfolioRiskAnalysis(**json.loads(cached))
    except Exception:
        pass

    price_series = {}
    for s in symbols:
        fetch_sym = "GC=F" if s == "GRAM-ALTIN-TRY" else s
        stock = yf.Ticker(fetch_sym)
        hist = stock.history(period="6mo", interval="1d")
        if not hist.empty and len(hist) > 20:
            price_series[s] = hist["Close"].pct_change().dropna()

    if len(price_series) < 2:
        raise ValueError(
            "At least 2 valid symbols with historical data are required."
        )

    df_returns = pd.DataFrame(price_series).dropna()
    corr_df = df_returns.corr().round(3)

    corr_dict = corr_df.to_dict()
    corr_values = corr_df.values[np.triu_indices_from(corr_df.values, k=1)]
    avg_corr = float(np.mean(corr_values)) if len(corr_values) > 0 else 1.0

    if avg_corr < 0.3:
        div_score = "Excellent Diversification (Low Systemic Risk)"
    elif avg_corr < 0.7:
        div_score = "Moderate Diversification"
    else:
        div_score = "High Correlation Risk (Concentrated Portfolio)"

    res = PortfolioRiskAnalysis(
        assets=symbols,
        correlation_matrix=corr_dict,
        average_correlation=round(avg_corr, 3),
        diversification_score=div_score,
        generated_at=datetime.datetime.utcnow().isoformat(),
    )

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(
                engine_mem.setex(cache_key, 1800, res.model_dump_json()), loop
            ).result(timeout=1.0)
        else:
            loop.run_until_complete(
                engine_mem.setex(cache_key, 1800, res.model_dump_json())
            )
    except Exception:
        pass

    return res

@app.get("/health", status_code=status.HTTP_200_OK, tags=["Monitoring"])
async def health_check():
    start_time = time.perf_counter()
    latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
    return {
        "status": "operational",
        "redis_connected": False,
        "cache_backend": "process-local in-memory",
        "redis_error": "Redis is not configured; process-local memory is used.",
        "latency_ms": latency_ms,
        "version": "4.0.0",
    }

@app.post(
    "/api/v1/auth/register",
    status_code=status.HTTP_201_CREATED,
    tags=["B2B Auth"],
)
def register_client(
    payload: ClientRegisterSchema, db: Session = Depends(get_db)
):
    try:
        tier_quotas = {
            "starter": {"rpm": 60, "quota": 50000},
            "pro": {"rpm": 300, "quota": 500000},
            "enterprise": {"rpm": 1200, "quota": 5000000},
        }
        tier = payload.tier.lower()
        config = tier_quotas.get(tier, tier_quotas["starter"])

        key = generate_api_key(tier)
        client = ApiClient()
        client.company_name = payload.company_name
        client.email = payload.email
        client.api_key = key
        client.plan_tier = tier
        client.rate_limit_per_min = config["rpm"]
        client.monthly_quota = config["quota"]
        client.used_requests_this_month = 0
        client.is_active = True

        db.add(client)
        db.commit()
        db.refresh(client)
        return {
            "status": "success",
            "company": client.company_name,
            "email": client.email,
            "api_key": client.api_key,
            "tier": client.plan_tier,
            "monthly_quota": client.monthly_quota,
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Registration failed: {str(e)}",
        )

@app.get(
    "/api/v1/auth/usage",
    tags=["B2B Auth"],
    status_code=status.HTTP_200_OK,
)
def get_client_usage(
    client: ApiClient = Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    remaining = max(0, client.monthly_quota - (client.used_requests_this_month or 0))
    return {
        "company": client.company_name,
        "plan_tier": client.plan_tier,
        "rate_limit_per_min": client.rate_limit_per_min,
        "monthly_quota": client.monthly_quota,
        "used_requests": client.used_requests_this_month or 0,
        "remaining_requests": remaining,
        "status": "active" if client.is_active else "inactive",
    }

@app.post("/api/v1/billing/webhook", tags=["B2B Billing"])
def stripe_mock_webhook(
    payload: WebhookPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    webhook_secret = os.getenv("BILLING_WEBHOOK_SECRET")
    if not webhook_secret:
        raise HTTPException(status_code=503, detail="Billing webhook is not configured")
    supplied_secret = request.headers.get("X-Billing-Webhook-Secret", "")
    if not secrets.compare_digest(supplied_secret, webhook_secret):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")
    tier_quotas = {
        "starter": {"rpm": 60, "quota": 50000},
        "pro": {"rpm": 300, "quota": 500000},
        "enterprise": {"rpm": 1200, "quota": 5000000},
    }
    target_tier = payload.new_tier.lower()
    if target_tier not in tier_quotas:
        raise HTTPException(status_code=400, detail="Invalid tier name")

    client = db.query(ApiClient).filter(ApiClient.api_key == payload.api_key).first()
    if not client:
        raise HTTPException(status_code=404, detail="API client not found")

    client.plan_tier = target_tier
    client.rate_limit_per_min = tier_quotas[target_tier]["rpm"]
    client.monthly_quota = tier_quotas[target_tier]["quota"]
    db.commit()

    return {
        "status": "success",
        "company": client.company_name,
        "new_tier": client.plan_tier,
        "new_monthly_quota": client.monthly_quota,
    }

@app.get(
    "/api/v1/metrics/{ticker}",
    response_model=MarketRiskMetric,
    tags=["B2B Endpoints"],
)
async def get_metrics(
    ticker: str,
    background_tasks: BackgroundTasks,
    client: ApiClient = Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    client.used_requests_this_month += 1
    db.commit()

    ticker_clean = ticker.upper()
    try:
        loop = asyncio.get_event_loop()
        metric = await loop.run_in_executor(
            None, process_single_ticker_sync, ticker_clean
        )
        background_tasks.add_task(
            log_metric_to_db,
            metric.ticker,
            metric.price,
            metric.change_percent,
            metric.rsi_14,
            metric.momentum_status,
        )
        return metric
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(
    "/api/v1/quant/backtest/{ticker}",
    response_model=BacktestResult,
    tags=["B2B Endpoints"],
)
async def get_backtest(
    ticker: str,
    client: ApiClient = Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    client.used_requests_this_month += 1
    db.commit()

    ticker_clean = ticker.upper()
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, run_quant_backtest_sync, ticker_clean
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/quant/var/{ticker}", tags=["B2B Endpoints"])
async def get_var_cvar(
    ticker: str,
    days: int = 1,
    simulations: int = 10000,
    client: ApiClient = Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    client.used_requests_this_month += 1
    db.commit()

    ticker_clean = ticker.upper()
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, run_monte_carlo_var_sync, ticker_clean, days, simulations
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(
    "/api/v1/quant/correlation",
    response_model=PortfolioRiskAnalysis,
    tags=["B2B Endpoints"],
)
async def get_correlation_matrix(
    symbols: str,
    client: ApiClient = Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    client.used_requests_this_month += 1
    db.commit()

    raw_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if len(raw_list) < 2:
        raise HTTPException(
            status_code=400,
            detail=(
                "Please provide at least 2 comma-separated tickers (e.g."
                " ?symbols=AAPL,TSLA,NVDA)"
            ),
        )
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, run_correlation_matrix_sync, raw_list
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws/stream")
async def websocket_stream_endpoint(websocket: WebSocket):
    await websocket.accept()
    loop = asyncio.get_event_loop()
    try:
        while True:
            raw_msg = await websocket.receive_text()
            payload = json.loads(raw_msg)
            tickers = payload.get("tickers", [])

            for ticker in tickers:
                clean_ticker = ticker.upper()
                try:
                    metric = await loop.run_in_executor(
                        None, process_single_ticker_sync, clean_ticker
                    )
                    await websocket.send_json(
                        {"status": "success", "data": metric.model_dump()}
                    )
                except Exception as ex:
                    await websocket.send_json(
                        {"status": "error", "ticker": clean_ticker, "message": str(ex)}
                    )
                await asyncio.sleep(0.05)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass

@app.websocket("/ws/hft-risk")
async def websocket_hft_risk_endpoint(websocket: WebSocket):
    await websocket.accept()
    queue = engine_mem.subscribe_hft()

    latest = engine_mem.get_latest_hft()
    if latest:
        try:
            await websocket.send_text(latest)
        except Exception:
            pass

    try:
        while True:
            data = await queue.get()
            await websocket.send_text(data)
    except WebSocketDisconnect:
        engine_mem.unsubscribe_hft(queue)
    except Exception:
        engine_mem.unsubscribe_hft(queue)

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AlphaMetrics | Quant Terminal</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
        <style>
            :root {
                color-scheme: dark;
                --canvas: #080b12;
                --panel: rgba(15, 20, 31, .88);
                --line: rgba(148, 163, 184, .13);
                --muted: #8993a5;
                --mint: #67e8bd;
            }
            * { box-sizing: border-box; }
            body {
                font-family: 'DM Sans', sans-serif;
                background: radial-gradient(ellipse at 50% -18%, rgba(16, 185, 129, .13), transparent 40%),
                            radial-gradient(ellipse at 100% 45%, rgba(56, 189, 248, .045), transparent 34%), var(--canvas);
                color: #e8edf5;
                letter-spacing: -.01em;
            }
            body::before {
                content: ''; position: fixed; inset: 0; pointer-events: none; opacity: .14;
                background-image: linear-gradient(rgba(148,163,184,.045) 1px, transparent 1px), linear-gradient(90deg, rgba(148,163,184,.045) 1px, transparent 1px);
                background-size: 48px 48px; mask-image: linear-gradient(to bottom, black, transparent 80%);
            }
            .max-w-5xl { max-width: 1280px; position: relative; }
            h1, h2, h3 { letter-spacing: -.045em; }
            section, .bg-gray-900.border, #quantModal > div {
                backdrop-filter: blur(18px); -webkit-backdrop-filter: blur(18px);
                box-shadow: 0 18px 60px rgba(0,0,0,.24);
            }
            section:first-of-type { background: linear-gradient(120deg, rgba(16, 35, 35, .92), rgba(13, 19, 29, .9) 64%, rgba(11, 23, 29, .92)); }
            section:nth-of-type(2) { background: linear-gradient(135deg, rgba(17, 23, 35, .92), rgba(12, 17, 27, .88)); }
            #watchlistBody tr { transition: background .18s ease, transform .18s ease; }
            #watchlistBody tr:hover { background: rgba(103, 232, 189, .045); }
            thead th { font-size: 10px; letter-spacing: .12em; white-space: nowrap; }
            input { transition: border-color .18s ease, box-shadow .18s ease; }
            input:focus { box-shadow: 0 0 0 3px rgba(103, 232, 189, .1); }
            button { transition: transform .16s ease, background-color .16s ease, border-color .16s ease, box-shadow .16s ease; }
            button:hover { transform: translateY(-1px); }
            #quantModal > div { background: rgba(13, 19, 30, .97); }
            @media (max-width: 640px) {
                body { padding: 18px !important; }
                section { padding: 16px !important; }
                #watchlistBody td, thead th { padding-left: 12px; padding-right: 12px; }
            }
        </style>
    </head>
    <body class="min-h-screen p-4 sm:p-8 flex justify-center">
        <div class="w-full max-w-5xl">
            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4 border-b border-gray-800/80 pb-6 mb-7">
                <div>
                    <div class="flex items-center gap-3">
                        <span class="flex h-10 w-10 items-center justify-center rounded-xl border border-emerald-400/20 bg-emerald-400/10 text-emerald-300 text-lg font-bold">α</span>
                        <div>
                            <h1 class="text-2xl font-bold text-white">Alpha<span class="text-emerald-300">Metrics</span></h1>
                            <p class="text-xs text-gray-500 mt-1">Quant intelligence <span class="mx-1 text-gray-700">/</span> Terminal v4.0</p>
                        </div>
                    </div>
                </div>
                <div class="flex items-center gap-2">
                    <span id="connBadge" class="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-emerald-950 text-emerald-400 border border-emerald-800">
                        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse mr-2"></span> WS Connected
                    </span>
                    <button onclick="requestStreamAll()" class="bg-gray-900 border border-gray-700 hover:border-gray-500 text-xs px-3 py-1.5 rounded-lg text-gray-300 transition">
                        Stream Sync
                    </button>
                </div>
            </div>

            <section class="bg-gray-900/80 border border-emerald-500/30 rounded-xl p-5 mb-6 shadow-2xl relative overflow-hidden">
                <div class="flex items-center justify-between mb-3 border-b border-gray-800/80 pb-2">
                    <div class="flex items-center gap-2">
                        <span class="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping"></span>
                        <span class="text-xs font-bold uppercase tracking-wider text-emerald-300">Binance BTC/USDT <span class="text-gray-500">/</span> Live market feed</span>
                    </div>
                    <span id="hftStatus" class="text-[10px] text-gray-400 font-mono">Connecting to Binance Spot...</span>
                </div>
                <div class="grid grid-cols-2 sm:grid-cols-5 gap-3">
                    <div class="bg-gray-950/70 p-3 rounded-lg border border-gray-800">
                            <span class="text-[10px] text-gray-500 uppercase block">BTC / USDT mid</span>
                        <b id="hftPrice" class="text-sm text-white font-mono">--</b>
                    </div>
                    <div class="bg-gray-950/70 p-3 rounded-lg border border-gray-800">
                            <span class="text-[10px] text-gray-500 uppercase block">10-tick VaR 95%</span>
                        <b id="hftVar95" class="text-sm text-amber-400 font-mono">--</b>
                    </div>
                    <div class="bg-gray-950/70 p-3 rounded-lg border border-gray-800">
                        <span class="text-[10px] text-gray-500 uppercase block">CVaR (Shortfall)</span>
                        <b id="hftCvar" class="text-sm text-red-400 font-mono">--</b>
                    </div>
                    <div class="bg-gray-950/70 p-3 rounded-lg border border-gray-800">
                            <span class="text-[10px] text-gray-500 uppercase block">Best bid/ask spread</span>
                        <b id="hftSpread" class="text-sm text-emerald-400 font-mono">--</b>
                    </div>
                    <div class="bg-gray-950/70 p-3 rounded-lg border border-gray-800">
                            <span class="text-[10px] text-gray-500 uppercase block">Top-book imbalance</span>
                        <b id="hftOfi" class="text-sm text-cyan-400 font-mono">--</b>
                    </div>
                </div>
            </section>

            <section class="bg-gray-900/60 border border-gray-800 rounded-xl p-5 mb-6 shadow-xl">
                <div class="flex items-center justify-between mb-3">
                    <span class="text-xs font-bold uppercase tracking-wider text-emerald-300">API access & usage</span>
                    <span class="text-[11px] text-gray-500">Client workspace</span>
                </div>
                <p class="text-xs text-gray-500 mb-3">Inspect your plan, rate limit and remaining monthly requests.</p>
                <div class="flex flex-col sm:flex-row gap-2">
                    <input id="keyInspectorInput" type="text" placeholder="Enter X-API-Key (e.g. starter_..., enterprise_...)" 
                           class="flex-1 bg-gray-950 border border-gray-800 rounded-lg px-3 py-2 text-xs focus:outline-none focus:border-emerald-500 font-mono text-gray-200">
                        <button onclick="inspectApiKey()" class="bg-emerald-400 hover:bg-emerald-300 text-gray-950 font-bold px-4 py-2 rounded-lg text-xs transition shadow-lg shadow-emerald-950/30">
                        Check access
                    </button>
                </div>
                <div id="inspectorResult" class="hidden mt-3 bg-gray-950/80 border border-gray-800 rounded-lg p-3 text-xs font-mono">
                    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        <div><span class="text-gray-500">Company:</span> <b id="statCompany" class="text-white block">-</b></div>
                        <div><span class="text-gray-500">Tier:</span> <b id="statTier" class="text-emerald-400 block">-</b></div>
                        <div><span class="text-gray-500">RPM Limit:</span> <b id="statRpm" class="text-gray-300 block">-</b></div>
                        <div><span class="text-gray-500">Quota Remaining:</span> <b id="statQuota" class="text-emerald-400 block">-</b></div>
                    </div>
                </div>
            </section>

            <div class="flex flex-col sm:flex-row gap-2 mb-4">
                <input id="newTicker" type="text" placeholder="Add Symbol (e.g. INTC, AMZN, PLTR)" 
                       class="flex-1 bg-gray-900 border border-gray-800 rounded-lg px-4 py-3 text-sm focus:outline-none focus:border-emerald-500 uppercase tracking-wider">
                <button onclick="addCustomTicker()" class="bg-emerald-500 hover:bg-emerald-400 text-black font-bold px-6 py-3 rounded-lg text-sm transition">
                    + Track
                </button>
            </div>

            <div class="flex items-end justify-between gap-3 mb-3 px-1">
                <div>
                    <h2 class="text-lg font-semibold text-gray-100">Market watchlist</h2>
                    <p class="text-xs text-gray-500 mt-1">Live pricing, momentum and risk signals</p>
                </div>
                <span class="hidden sm:inline-flex items-center gap-2 text-[10px] uppercase tracking-widest text-gray-500"><span class="h-1.5 w-1.5 rounded-full bg-emerald-400"></span> Streaming</span>
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden shadow-2xl">
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-sm">
                        <thead class="bg-gray-950 border-b border-gray-800 text-xs text-gray-400 uppercase tracking-wider">
                            <tr>
                                <th class="py-3.5 px-4">Asset</th>
                                <th class="py-3.5 px-4">Price</th>
                                <th class="py-3.5 px-4">24h Change</th>
                                <th class="py-3.5 px-4">50D Avg</th>
                                <th class="py-3.5 px-4">14D RSI</th>
                                <th class="py-3.5 px-4">Status</th>
                                <th class="py-3.5 px-4 text-right">Quant Actions</th>
                            </tr>
                        </thead>
                        <tbody id="watchlistBody" class="divide-y divide-gray-800">
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <div id="quantModal" class="fixed inset-0 bg-black/80 hidden items-center justify-center p-4 z-50">
            <div class="bg-[#0f172a] border border-gray-800 w-full max-w-lg rounded-xl p-6 relative">
                <button onclick="closeModal()" class="absolute top-4 right-4 text-gray-400 hover:text-white text-lg font-bold">&times;</button>
                <div id="modalContent">
                    <p class="text-xs text-gray-500 uppercase tracking-wider">Algorithmic Backtest & Risk Engine</p>
                    <h3 id="modalTicker" class="text-xl font-bold text-emerald-400 mt-1">--</h3>
                    <div id="modalLoading" class="py-8 text-center text-xs text-gray-400 animate-pulse">Running Quantitative Models...</div>
                    <div id="modalResults" class="hidden mt-4 space-y-3">
                        <div class="grid grid-cols-2 gap-3">
                            <div class="bg-gray-900 p-3 rounded border border-gray-800">
                                <span class="text-[11px] text-gray-500">Strategy Return (1Y)</span>
                                <p id="mStratRet" class="text-base font-bold">--</p>
                            </div>
                            <div class="bg-gray-900 p-3 rounded border border-gray-800">
                                <span class="text-[11px] text-gray-500">Buy & Hold Return</span>
                                <p id="mBnhRet" class="text-base font-bold text-gray-400">--</p>
                            </div>
                            <div class="bg-gray-900 p-3 rounded border border-gray-800">
                                <span class="text-[11px] text-gray-500">Sharpe Ratio</span>
                                <p id="mSharpe" class="text-base font-bold text-emerald-400">--</p>
                            </div>
                            <div class="bg-gray-900 p-3 rounded border border-gray-800">
                                <span class="text-[11px] text-gray-500">Max Drawdown</span>
                                <p id="mDrawdown" class="text-base font-bold text-red-400">--</p>
                            </div>
                        </div>
                        <div class="bg-gray-900 p-3 rounded border border-gray-800 flex justify-between text-xs">
                            <span class="text-gray-400">Total Trades: <b id="mTrades" class="text-white">--</b></span>
                            <span class="text-gray-400">Win Rate: <b id="mWinRate" class="text-emerald-400">--</b></span>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <script>
            let defaultTickers = [
                'GRAM-ALTIN-TRY', 'GC=F', 'USDTRY=X',
                'VUAA.L', 'QQQ', '^GSPC',
                'NVDA', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'TSLA', 'AMD',
                'THYAO.IS', 'ASELS.IS', 'EREGL.IS',
                'MBG.DE', 'BMW.DE',
                'BTC-USD', 'ETH-USD', 'SOL-USD'
            ];

            let ws = null;
            let hftWs = null;

            function formatQuoteTime(value) {
                if (!value) return 'time unavailable';
                const date = new Date(value);
                return Number.isNaN(date.getTime()) ? 'time unavailable' : date.toLocaleTimeString();
            }

            function connectHftWebSocket() {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/ws/hft-risk`;
                hftWs = new WebSocket(wsUrl);

                hftWs.onopen = () => {
                    document.getElementById('hftStatus').innerText = 'Waiting for public market quote...';
                    document.getElementById('hftStatus').className = 'text-[10px] text-emerald-400 font-mono';
                };

                hftWs.onmessage = (event) => {
                    const d = JSON.parse(event.data);
                    if (d.status !== 'live') {
                        document.getElementById('hftStatus').innerText = 'Binance quote unavailable';
                        document.getElementById('hftStatus').className = 'text-[10px] text-red-400 font-mono';
                        return;
                    }
                    document.getElementById('hftStatus').innerText = `Binance Spot · Updated ${formatQuoteTime(d.data_as_of)}`;
                    document.getElementById('hftStatus').className = 'text-[10px] text-emerald-400 font-mono';
                    document.getElementById('hftPrice').innerText = Number(d.current_price).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
                    document.getElementById('hftVar95').innerText = d.var_95 ? d.var_95.toFixed(2) : '--';
                    document.getElementById('hftCvar').innerText = d.cvar_95 ? d.cvar_95.toFixed(2) : '--';
                    document.getElementById('hftSpread').innerText = Number(d.spread).toFixed(4);
                    document.getElementById('hftOfi').innerText = Number(d.ofi).toFixed(4);
                };

                hftWs.onclose = () => {
                    document.getElementById('hftStatus').innerText = 'Disconnected. Retrying...';
                    document.getElementById('hftStatus').className = 'text-[10px] text-red-400 font-mono';
                    setTimeout(connectHftWebSocket, 3000);
                };
            }

            async function inspectApiKey() {
                const key = document.getElementById('keyInspectorInput').value.trim();
                const resBox = document.getElementById('inspectorResult');
                if (!key) { alert('Please enter an API Key'); return; }

                try {
                    const res = await fetch('/api/v1/auth/usage', {
                        headers: { 'X-API-Key': key }
                    });
                    if (!res.ok) throw new Error(await res.text());
                    const data = await res.json();
                    resBox.classList.remove('hidden');
                    document.getElementById('statCompany').innerText = data.company;
                    document.getElementById('statTier').innerText = data.plan_tier.toUpperCase();
                    document.getElementById('statRpm').innerText = data.rate_limit_per_min + ' RPM';
                    document.getElementById('statQuota').innerText = data.remaining_requests + ' / ' + data.monthly_quota;
                } catch (err) {
                    alert('Authentication failed: ' + err.message);
                }
            }

            function initLayout() {
                const tbody = document.getElementById('watchlistBody');
                tbody.innerHTML = '';
                defaultTickers.forEach(ticker => {
                    const cleanId = ticker.replace(/[^a-zA-Z0-9]/g, '_');
                    const row = document.createElement('tr');
                    row.className = 'hover:bg-gray-800/40 transition';
                    row.id = `row-${cleanId}`;
                    row.innerHTML = `
                        <td class="py-3.5 px-4 font-bold text-white">${ticker}</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs animate-pulse">Waiting for quote...</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs">--</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs">--</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs">--</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs">--</td>
                        <td class="py-3.5 px-4 text-right">
                            <button onclick="openQuantModal('${ticker}')" class="text-[11px] bg-slate-800 hover:bg-slate-700 border border-slate-700 text-emerald-400 px-2.5 py-1 rounded transition">
                                Quant Risk
                            </button>
                        </td>
                    `;
                    tbody.appendChild(row);
                });
            }

            function updateRowUI(data) {
                const cleanId = data.ticker.replace(/[^a-zA-Z0-9]/g, '_');
                const row = document.getElementById(`row-${cleanId}`);
                if (!row) return;

                const changeColor = data.change_percent >= 0 ? 'text-emerald-400' : 'text-red-400';
                const changeSign = data.change_percent >= 0 ? '+' : '';

                let rsiBadge = 'bg-emerald-950 text-emerald-400 border-emerald-800';
                let statusColor = 'text-emerald-400';

                if (data.rsi_14 >= 70) {
                    rsiBadge = 'bg-red-950 text-red-400 border-red-800';
                    statusColor = 'text-red-400';
                } else if (data.rsi_14 <= 30) {
                    rsiBadge = 'bg-blue-950 text-blue-400 border-blue-800';
                    statusColor = 'text-blue-400';
                }

                row.innerHTML = `
                    <td class="py-3.5 px-4 font-bold text-white tracking-wide">${data.ticker}</td>
                    <td class="py-3.5 px-4 font-semibold text-white">${Number(data.price).toLocaleString(undefined, {maximumFractionDigits: 8})} <span class="text-[10px] text-gray-500">${data.currency}</span><span class="block text-[9px] font-normal text-gray-500" title="${data.quote_source}">${formatQuoteTime(data.data_as_of)} · ${data.quote_source}</span></td>
                    <td class="py-3.5 px-4 font-bold ${changeColor}">${changeSign}${data.change_percent}%</td>
                    <td class="py-3.5 px-4 text-gray-400">${data.fifty_day_average}</td>
                    <td class="py-3.5 px-4">
                        <span class="px-2 py-0.5 rounded text-xs border ${rsiBadge} font-bold">${data.rsi_14}</span>
                    </td>
                    <td class="py-3.5 px-4 font-semibold ${statusColor} text-xs">${data.momentum_status}</td>
                    <td class="py-3.5 px-4 text-right">
                        <button onclick="openQuantModal('${data.ticker}')" class="text-[11px] bg-slate-800 hover:bg-slate-700 border border-slate-700 text-emerald-400 px-2.5 py-1 rounded transition">
                            Quant Risk
                        </button>
                    </td>
                `;
            }

            function connectWebSocket() {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/ws/stream`;
                ws = new WebSocket(wsUrl);

                ws.onopen = () => {
                    const badge = document.getElementById('connBadge');
                    badge.className = 'inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-emerald-950 text-emerald-400 border border-emerald-800';
                    badge.innerHTML = '<span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse mr-2"></span> WS Connected';
                    requestStreamAll();
                };

                ws.onmessage = (event) => {
                    const response = JSON.parse(event.data);
                    if (response.status === 'success') {
                        updateRowUI(response.data);
                    }
                };

                ws.onclose = () => {
                    const badge = document.getElementById('connBadge');
                    badge.className = 'inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-red-950 text-red-400 border border-red-800';
                    badge.innerHTML = '<span class="w-2 h-2 rounded-full bg-red-400 mr-2"></span> Disconnected';
                    setTimeout(connectWebSocket, 3000);
                };
            }

            function requestStreamAll() {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ tickers: defaultTickers }));
                }
            }

            function addCustomTicker() {
                const input = document.getElementById('newTicker');
                const val = input.value.trim().toUpperCase();
                if (val && !defaultTickers.includes(val)) {
                    defaultTickers.unshift(val);
                    input.value = '';
                    initLayout();
                    requestStreamAll();
                }
            }

            async function openQuantModal(ticker) {
                const modal = document.getElementById('quantModal');
                const loading = document.getElementById('modalLoading');
                const results = document.getElementById('modalResults');
                document.getElementById('modalTicker').innerText = ticker;
                
                modal.classList.remove('hidden');
                modal.classList.add('flex');
                loading.classList.remove('hidden');
                results.classList.add('hidden');

                try {
                    const res = await fetch(`/api/v1/quant/backtest/${ticker}`);
                    const data = await res.json();
                    
                    const stratColor = data.strategy_return_pct >= 0 ? 'text-emerald-400' : 'text-red-400';
                    const stratSign = data.strategy_return_pct >= 0 ? '+' : '';
                    const bnhColor = data.buy_and_hold_return_pct >= 0 ? 'text-emerald-400' : 'text-red-400';
                    const bnhSign = data.buy_and_hold_return_pct >= 0 ? '+' : '';

                    document.getElementById('mStratRet').innerHTML = `<span class="${stratColor}">${stratSign}${data.strategy_return_pct}%</span>`;
                    document.getElementById('mBnhRet').innerHTML = `<span class="${bnhColor}">${bnhSign}${data.buy_and_hold_return_pct}%</span>`;
                    document.getElementById('mSharpe').innerText = data.sharpe_ratio;
                    document.getElementById('mDrawdown').innerText = `${data.max_drawdown_pct}%`;
                    document.getElementById('mTrades').innerText = data.total_trades;
                    document.getElementById('mWinRate').innerText = `${data.win_rate_pct}%`;

                    loading.classList.add('hidden');
                    results.classList.remove('hidden');
                } catch {
                    loading.innerText = 'Risk calculation failed for this asset.';
                }
            }

            function closeModal() {
                const modal = document.getElementById('quantModal');
                modal.classList.add('hidden');
                modal.classList.remove('flex');
            }

            window.onload = () => {
                initLayout();
                connectWebSocket();
                connectHftWebSocket();
                setInterval(requestStreamAll, 60000);
            };
        </script>
    </body>
    </html>
    """
