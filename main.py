import asyncio
from contextlib import asynccontextmanager
import datetime
from email.message import EmailMessage
import json
import logging
import math
import os
import re
import secrets
import smtplib
import ssl
import threading
import time
from typing import Any, Dict, List, Literal, Optional, Set
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
import numpy as np
import pandas as pd
import httpx
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session
import yfinance as yf

from auth_service import consume_api_request, find_client_by_api_key, generate_api_key, hash_api_key, verify_api_key
from database import Base, engine, get_db
from models import ApiClient, EmailVerification

logger = logging.getLogger(__name__)

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
        "var_95": round(max(0.0, -var_95), 4),
        "var_99": round(max(0.0, -var_99), 4),
        "cvar_95": round(max(0.0, -cvar_95), 4),
        "updated_at": time.time(),
    }

async def background_lob_engine():
    prices = []
    last_error_log_at: Dict[str, float] = {}
    retry_after: Dict[str, float] = {}
    active_exchange = None
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            published = False
            for exchange in ("Binance Spot", "Coinbase Exchange", "Kraken Spot"):
                if time.monotonic() < retry_after.get(exchange, 0):
                    continue
                try:
                    best_bid, best_ask, bid_size, ask_size, data_as_of = (
                        await fetch_top_of_book(client, exchange)
                    )
                    if exchange != active_exchange:
                        prices = []  # Risk samples must come from the same venue.
                        active_exchange = exchange
                    current_price = (best_bid + best_ask) / 2
                    prices.append(current_price)
                    prices = prices[-300:]
                    risk_metrics = calculate_hft_risk_math(prices)
                    metrics = risk_metrics or {}
                    metrics.update({
                        "status": "live",
                        "risk_status": "ready" if risk_metrics else "warming_up" if len(prices) < 15 else "flat_sample",
                        "sample_count": len(prices),
                        "symbol": "BTC/USDT",
                        "exchange": exchange,
                        "current_price": current_price,
                        "spread": best_ask - best_bid,
                        "ofi": (bid_size - ask_size) / (bid_size + ask_size)
                        if bid_size + ask_size else 0.0,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "data_as_of": data_as_of,
                    })
                    await engine_mem.broadcast_hft(json.dumps(metrics))
                    published = True
                    break
                except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as exc:
                    now = time.monotonic()
                    blocked = isinstance(exc, httpx.HTTPStatusError) and (
                        exc.response.status_code in (403, 418, 429, 451)
                    )
                    retry_after[exchange] = now + (600 if blocked else 30)
                    if now - last_error_log_at.get(exchange, float("-inf")) >= 60:
                        logger.warning("%s order-book request failed: %s", exchange, exc)
                        last_error_log_at[exchange] = now
            if not published:
                await engine_mem.broadcast_hft(json.dumps({
                    "status": "unavailable",
                    "symbol": "BTC/USDT",
                    "reason": "Public BTC/USDT order-book feeds unavailable",
                    "data_as_of": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }))
            await asyncio.sleep(2 if published else 5)


async def fetch_top_of_book(client: httpx.AsyncClient, exchange: str):
    """Return bid, ask, sizes and source timestamp for a real BTC/USDT book."""
    if exchange == "Binance Spot":
        response = await client.get(
            "https://data-api.binance.vision/api/v3/ticker/bookTicker",
            params={"symbol": "BTCUSDT"},
        )
        response.raise_for_status()
        book = response.json()
        bid, ask = book["bidPrice"], book["askPrice"]
        bid_qty, ask_qty = book["bidQty"], book["askQty"]
        as_of = datetime.datetime.now(datetime.timezone.utc).isoformat()
    elif exchange == "Coinbase Exchange":
        response = await client.get(
            "https://api.exchange.coinbase.com/products/BTC-USDT/book",
            params={"level": 1},
        )
        response.raise_for_status()
        book = response.json()
        bid, bid_qty = book["bids"][0][:2]
        ask, ask_qty = book["asks"][0][:2]
        as_of = book["time"]
    elif exchange == "Kraken Spot":
        response = await client.get(
            "https://api.kraken.com/0/public/Depth",
            params={"pair": "XBTUSDT", "count": 1},
        )
        response.raise_for_status()
        payload = response.json()
        if payload["error"]:
            raise ValueError("Kraken returned an order-book error")
        book = payload["result"]["XBTUSDT"]
        bid, bid_qty = book["bids"][0][:2]
        ask, ask_qty = book["asks"][0][:2]
        as_of = datetime.datetime.now(datetime.timezone.utc).isoformat()
    else:
        raise ValueError(f"Unknown exchange: {exchange}")

    values = tuple(map(float, (bid, ask, bid_qty, ask_qty)))
    if not all(map(math.isfinite, values)) or values[0] <= 0 or values[1] < values[0]:
        raise ValueError(f"Invalid {exchange} top-of-book quote")
    if values[2] < 0 or values[3] < 0:
        raise ValueError(f"Invalid {exchange} top-of-book size")
    return (*values, as_of)

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
    request_limits = {
        "/api/v1/auth/register": 5,
        "/api/v1/auth/verify-email": 10,
    }
    request_limit = request_limits.get(request.url.path, 60)
    if request.url.path.startswith("/api/v1/demo/quant/backtest/"):
        request_limit = 5
    bucket = f"http:{request.url.path}:{client_ip}"
    if await engine_mem.is_rate_limited(bucket, limit=request_limit, window=60):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"detail": f"Rate limit exceeded. Maximum {request_limit} requests per minute."},
            headers={"Retry-After": "60"},
        )

    return await call_next(request)

class ClientRegisterSchema(BaseModel):
    company_name: str
    email: EmailStr
    tier: Literal["starter"] = "starter"


class VerifyEmailSchema(BaseModel):
    email: EmailStr
    token: str

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


DEMO_BACKTEST_TICKERS = frozenset({
    "GC=F", "USDTRY=X", "VUAA.L", "QQQ", "^GSPC", "NVDA", "AAPL",
    "MSFT", "AMZN", "GOOGL", "TSLA", "AMD", "THYAO.IS", "ASELS.IS",
    "EREGL.IS", "MBG.DE", "BMW.DE", "BTC-USD", "ETH-USD", "SOL-USD",
})
DEMO_STREAM_TICKERS = DEMO_BACKTEST_TICKERS | {"GRAM-ALTIN-TRY"}
MAX_STREAM_TICKERS = 24
TICKER_PATTERN = re.compile(r"^[A-Z0-9^][A-Z0-9.^=_-]{0,31}$")
demo_backtest_cache: Dict[str, tuple[float, BacktestResult]] = {}
demo_backtest_cache_lock = threading.Lock()


def normalize_ticker(ticker: str) -> str:
    value = ticker.strip().upper()
    if not TICKER_PATTERN.fullmatch(value):
        raise HTTPException(status_code=422, detail="Invalid ticker symbol.")
    return value


def require_historical_ticker(ticker: str) -> str:
    value = normalize_ticker(ticker)
    if value == "GRAM-ALTIN-TRY":
        raise HTTPException(
            status_code=422,
            detail="Historical gram-gold analysis is unavailable; use GC=F for gold futures.",
        )
    return value

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
        try:
            res_obj = process_binance_ticker(ticker_clean)
        except (OSError, ValueError, KeyError, IndexError, TypeError) as exc:
            logger.warning("Binance quote failed for %s: %s", ticker_clean, type(exc).__name__)
            daily, intraday = get_market_history(ticker_clean)
            daily_closes = daily["Close"].dropna()
            if len(daily_closes) < 2:
                raise ValueError(f"Insufficient daily data for {ticker_clean}") from exc
            current_price = float(intraday["Close"].iloc[-1])
            prev_close = float(daily_closes.iloc[-2])
            res_obj = build_metric(
                ticker=ticker_clean,
                price=current_price,
                change_pct=(current_price - prev_close) / prev_close * 100,
                currency="USD",
                daily_closes=daily_closes,
                data_as_of=intraday.index[-1],
                source="Yahoo Finance crypto quote (Binance unavailable)",
            )
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
        market_metric_cache[ticker_clean] = (time.monotonic() + 60, res_obj)

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

    stock = yf.Ticker(ticker_clean)
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

        strategy_returns.append(df["Daily_Return"].iloc[i] if position == 1 else 0.0)

        if position == 0 and rsi < 35:
            position = 1
            entry_price = price
        elif position == 1 and rsi > 65:
            position = 0
            ret = (price - entry_price) / entry_price
            trades.append(ret)

    df["Strategy_Return"] = strategy_returns
    df["Cum_Strategy"] = (1 + df["Strategy_Return"]).cumprod()
    df["Cum_BnH"] = (1 + df["Daily_Return"]).cumprod()

    total_strat_return = round(
        (float(df["Cum_Strategy"].iloc[-1]) - 1.0) * 100, 2
    )
    total_bnh_return = round((float(df["Cum_BnH"].iloc[-1]) - 1.0) * 100, 2)

    strategy_daily_returns = df["Strategy_Return"]
    if len(strategy_daily_returns) > 5 and strategy_daily_returns.std() > 0:
        sharpe = (strategy_daily_returns.mean() / strategy_daily_returns.std()) * np.sqrt(252)
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

    stock = yf.Ticker(ticker_clean)
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
        "var_95_max_loss": round(max(0.0, -var_95), 2),
        "var_99_max_loss": round(max(0.0, -var_99), 2),
        "cvar_95_expected_shortfall": round(max(0.0, -cvar_95), 2),
        "cvar_99_expected_shortfall": round(max(0.0, -cvar_99), 2),
        "risk_summary": (
            f"Model estimate: the 5th percentile price change over {days} day(s)"
            f" implies a loss of {round(max(0.0, -var_95), 2)} price units."
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
        stock = yf.Ticker(s)
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
    market_feed = json.loads(engine_mem._latest_hft_risk) if engine_mem._latest_hft_risk else {}
    return {
        "status": "operational",
        "market_feed": {
            "status": market_feed.get("status", "starting"),
            "exchange": market_feed.get("exchange"),
            "reason": market_feed.get("reason"),
            "data_as_of": market_feed.get("data_as_of"),
        },
        "redis_connected": False,
        "cache_backend": "process-local in-memory",
        "redis_error": "Redis is not configured; process-local memory is used.",
        "latency_ms": latency_ms,
        "version": "4.0.0",
    }

def send_email_verification(email: str, token: str) -> None:
    resend_api_key = os.getenv("RESEND_API_KEY")
    from_email = os.getenv("RESEND_FROM_EMAIL") or os.getenv("SMTP_FROM_EMAIL")
    if not from_email:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Email verification is unavailable because the sender is not configured.",
        )

    body = (
        "Your AlphaMetrics verification code is:\n\n"
        f"{token}\n\n"
        "It expires in 15 minutes. Enter it in POST /api/v1/auth/verify-email "
        "to finish registration. If you did not request this, ignore this email."
    )
    if resend_api_key:
        try:
            response = httpx.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {resend_api_key}"},
                json={
                    "from": from_email,
                    "to": [email],
                    "subject": "Verify your AlphaMetrics email",
                    "text": body,
                },
                timeout=10.0,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            if isinstance(exc, httpx.HTTPStatusError):
                try:
                    error = exc.response.json()
                    provider_error = error.get("message") or error.get("name")
                except (ValueError, AttributeError):
                    provider_error = None
                logger.warning(
                    "Resend rejected verification email: status=%s error=%s",
                    exc.response.status_code,
                    str(provider_error)[:300] if provider_error else "unknown",
                )
            else:
                logger.warning("Resend verification request failed: %s", type(exc).__name__)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Could not deliver the verification email. Please try again later.",
            ) from exc
        return

    smtp_host = os.getenv("SMTP_HOST")
    smtp_username = os.getenv("SMTP_USERNAME")
    smtp_password = os.getenv("SMTP_PASSWORD")
    if not all((smtp_host, smtp_username, smtp_password)):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Email verification is unavailable because no delivery provider is configured.",
        )

    try:
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
    except ValueError as exc:
        raise HTTPException(status_code=503, detail="Email verification is misconfigured.") from exc

    message = EmailMessage()
    message["Subject"] = "Verify your AlphaMetrics email"
    message["From"] = from_email
    message["To"] = email
    message.set_content(body)

    try:
        timeout = 10
        if os.getenv("SMTP_USE_SSL", "false").strip().lower() == "true":
            with smtplib.SMTP_SSL(
                smtp_host, smtp_port, timeout=timeout, context=ssl.create_default_context()
            ) as server:
                server.login(smtp_username, smtp_password)
                server.send_message(message)
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=timeout) as server:
                server.starttls(context=ssl.create_default_context())
                server.login(smtp_username, smtp_password)
                server.send_message(message)
    except (OSError, smtplib.SMTPException) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not deliver the verification email. Please try again later.",
        ) from exc


@app.post(
    "/api/v1/auth/register",
    status_code=status.HTTP_202_ACCEPTED,
    tags=["B2B Auth"],
    description=(
        "Sends a one-time verification code. No client or API key is created "
        "until the email address is verified."
    ),
)
def register_client(
    payload: ClientRegisterSchema, db: Session = Depends(get_db)
):
    company_name = payload.company_name.strip()
    email = str(payload.email).strip().lower()
    if not company_name or len(company_name) > 100:
        raise HTTPException(status_code=422, detail="Company name must be 1–100 characters.")
    if len(email) > 100:
        raise HTTPException(status_code=422, detail="Email address must be 100 characters or fewer.")

    if db.query(ApiClient.id).filter(ApiClient.email == email).first():
        raise HTTPException(status_code=409, detail="This email is already registered.")

    token = secrets.token_urlsafe(32)
    pending = db.query(EmailVerification).filter(EmailVerification.email == email).first()
    if pending is None:
        pending = EmailVerification(email=email, company_name=company_name)
        db.add(pending)
    pending.company_name = company_name
    pending.token_hash = hash_api_key(token)
    pending.expires_at_epoch = time.time() + 15 * 60

    try:
        send_email_verification(email, token)
        db.commit()
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not start email verification. Please try again later.",
        ) from exc

    return {
        "status": "verification_sent",
        "message": "Check your inbox and verify your email within 15 minutes.",
    }


@app.post(
    "/api/v1/auth/verify-email",
    status_code=status.HTTP_201_CREATED,
    tags=["B2B Auth"],
    description=(
        "Verifies the email code, creates a Starter client, and returns its "
        "API key once."
    ),
)
def verify_registration_email(
    payload: VerifyEmailSchema, db: Session = Depends(get_db)
):
    email = str(payload.email).strip().lower()
    pending = db.query(EmailVerification).filter(EmailVerification.email == email).first()
    if (
        pending is None
        or pending.expires_at_epoch <= time.time()
        or not secrets.compare_digest(pending.token_hash, hash_api_key(payload.token))
    ):
        raise HTTPException(status_code=400, detail="Invalid or expired verification code.")

    tier = "starter"
    key = generate_api_key(tier)
    client = ApiClient(
        company_name=pending.company_name,
        email=email,
        api_key_hash=hash_api_key(key),
        plan_tier=tier,
        rate_limit_per_min=60,
        monthly_quota=500,
        used_requests_this_month=0,
        is_active=True,
    )
    try:
        db.add(client)
        db.delete(pending)
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Registration could not be completed. Request a new verification code.",
        ) from exc

    return {
        "status": "success",
        "company": client.company_name,
        "email": client.email,
        "api_key": key,
        "tier": client.plan_tier,
        "monthly_quota": client.monthly_quota,
    }

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
        "starter": {"rpm": 60, "quota": 500},
        "pro": {"rpm": 300, "quota": 500000},
        "enterprise": {"rpm": 1200, "quota": 5000000},
    }
    target_tier = payload.new_tier.lower()
    if target_tier not in tier_quotas:
        raise HTTPException(status_code=400, detail="Invalid tier name")

    client = find_client_by_api_key(db, payload.api_key)
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
    ticker_clean = normalize_ticker(ticker)
    consume_api_request(db, client)
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
    except Exception as exc:
        logger.warning("Metric request failed for %s: %s", ticker_clean, type(exc).__name__)
        raise HTTPException(status_code=502, detail="Market data is temporarily unavailable.") from exc

@app.get(
    "/api/v1/demo/quant/backtest/{ticker}",
    response_model=BacktestResult,
    tags=["Public Demo"],
    description=(
        "Rate-limited public backtest for the dashboard watchlist. "
        "Use the authenticated endpoint for general API access."
    ),
)
async def get_demo_backtest(ticker: str):
    ticker_clean = normalize_ticker(ticker)
    if ticker_clean not in DEMO_BACKTEST_TICKERS:
        raise HTTPException(
            status_code=404,
            detail="This symbol is not available in the public demo. Use an API key for custom symbols.",
        )

    now = time.monotonic()
    with demo_backtest_cache_lock:
        cached = demo_backtest_cache.get(ticker_clean)
        if cached and cached[0] > now:
            return cached[1]

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, run_quant_backtest_sync, ticker_clean
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail="Market data is temporarily unavailable for this symbol. Please try again later.",
        ) from exc

    with demo_backtest_cache_lock:
        demo_backtest_cache[ticker_clean] = (time.monotonic() + 300, result)
    return result


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
    ticker_clean = require_historical_ticker(ticker)
    consume_api_request(db, client)
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, run_quant_backtest_sync, ticker_clean
        )
    except Exception as exc:
        logger.warning("Backtest failed for %s: %s", ticker_clean, type(exc).__name__)
        raise HTTPException(status_code=502, detail="Backtest data is temporarily unavailable.") from exc

@app.get("/api/v1/quant/var/{ticker}", tags=["B2B Endpoints"])
async def get_var_cvar(
    ticker: str,
    days: int = Query(1, ge=1, le=30),
    simulations: int = Query(10000, ge=500, le=20000),
    client: ApiClient = Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    ticker_clean = require_historical_ticker(ticker)
    consume_api_request(db, client)
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, run_monte_carlo_var_sync, ticker_clean, days, simulations
        )
    except Exception as exc:
        logger.warning("VaR calculation failed for %s: %s", ticker_clean, type(exc).__name__)
        raise HTTPException(status_code=502, detail="Risk data is temporarily unavailable.") from exc

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
    raw_list = [require_historical_ticker(s) for s in symbols.split(",") if s.strip()]
    if len(raw_list) < 2:
        raise HTTPException(
            status_code=400,
            detail=(
                "Please provide at least 2 comma-separated tickers (e.g."
                " ?symbols=AAPL,TSLA,NVDA)"
            ),
        )
    if len(raw_list) > 12 or len(set(raw_list)) != len(raw_list):
        raise HTTPException(status_code=422, detail="Provide 2–12 distinct ticker symbols.")
    consume_api_request(db, client)
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, run_correlation_matrix_sync, raw_list
        )
    except Exception as exc:
        logger.warning("Correlation request failed: %s", type(exc).__name__)
        raise HTTPException(status_code=502, detail="Correlation data is temporarily unavailable.") from exc

@app.websocket("/ws/stream")
async def websocket_stream_endpoint(websocket: WebSocket):
    await websocket.accept()
    loop = asyncio.get_event_loop()
    try:
        while True:
            raw_msg = await websocket.receive_text()
            if len(raw_msg) > 2048:
                await websocket.send_json({"status": "error", "message": "Request is too large."})
                continue
            try:
                payload = json.loads(raw_msg)
            except json.JSONDecodeError:
                await websocket.send_json({"status": "error", "message": "Invalid JSON."})
                continue
            tickers = payload.get("tickers") if isinstance(payload, dict) else None
            if not isinstance(tickers, list) or not 1 <= len(tickers) <= MAX_STREAM_TICKERS:
                await websocket.send_json({"status": "error", "message": "Send 1–24 demo symbols."})
                continue

            client_ip = websocket.client.host if websocket.client else "unknown"
            if await engine_mem.is_rate_limited(f"demo-ws:{client_ip}", limit=6, window=60):
                await websocket.close(code=1008, reason="Demo refresh limit reached")
                return

            seen: set[str] = set()
            for ticker in tickers:
                if not isinstance(ticker, str):
                    await websocket.send_json({"status": "error", "message": "Invalid symbol."})
                    continue
                try:
                    clean_ticker = normalize_ticker(ticker)
                except HTTPException:
                    await websocket.send_json({"status": "error", "ticker": ticker[:32], "message": "Invalid symbol."})
                    continue
                if clean_ticker in seen:
                    continue
                seen.add(clean_ticker)
                if clean_ticker not in DEMO_STREAM_TICKERS:
                    await websocket.send_json({"status": "error", "ticker": clean_ticker, "message": "Symbol unavailable in public demo."})
                    continue
                try:
                    metric = await loop.run_in_executor(
                        None, process_single_ticker_sync, clean_ticker
                    )
                    await websocket.send_json(
                        {"status": "success", "data": metric.model_dump()}
                    )
                except Exception as exc:
                    logger.warning("Demo quote failed for %s: %s", clean_ticker, type(exc).__name__)
                    await websocket.send_json(
                        {"status": "error", "ticker": clean_ticker, "message": "Quote temporarily unavailable."}
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
        <title>AlphaMetrics | Market Risk Research API</title>
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
            .hero { background: linear-gradient(120deg, rgba(16, 35, 35, .92), rgba(13, 19, 29, .94) 64%, rgba(11, 23, 29, .92)); }
            #watchlistBody tr { transition: background .18s ease, transform .18s ease; }
            #watchlistBody tr:hover { background: rgba(103, 232, 189, .045); }
            thead th { font-size: 10px; letter-spacing: .12em; white-space: nowrap; }
            input { transition: border-color .18s ease, box-shadow .18s ease; }
            input:focus { box-shadow: 0 0 0 3px rgba(103, 232, 189, .1); }
            button { transition: transform .16s ease, background-color .16s ease, border-color .16s ease, box-shadow .16s ease; }
            button:hover { transform: translateY(-1px); }
            button:focus-visible, a:focus-visible, summary:focus-visible { outline: 2px solid var(--mint); outline-offset: 3px; }
            #quantModal > div { background: rgba(13, 19, 30, .97); }
            @media (prefers-reduced-motion: reduce) {
                *, *::before, *::after { animation-duration: .01ms !important; transition-duration: .01ms !important; scroll-behavior: auto !important; }
            }
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
                            <p class="text-xs text-gray-500 mt-1">Market risk research API <span class="mx-1 text-gray-700">/</span> Tarn Dynamics</p>
                        </div>
                    </div>
                </div>
                <div class="flex items-center gap-2">
                    <span id="connBadge" role="status" aria-live="polite" class="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-gray-900 text-gray-400 border border-gray-700">
                        <span class="w-2 h-2 rounded-full bg-gray-400 mr-2"></span> Connecting
                    </span>
                    <button onclick="requestStreamAll()" class="bg-gray-900 border border-gray-700 hover:border-gray-500 text-xs px-3 py-1.5 rounded-lg text-gray-300 transition">
                        Refresh quotes
                    </button>
                </div>
            </div>

            <nav aria-label="Main navigation" class="flex flex-wrap items-center gap-x-5 gap-y-2 mb-6 text-sm text-gray-400">
                <a class="hover:text-white" href="#market">Live market</a>
                <a class="hover:text-white" href="#api-access">API access</a>
                <a class="hover:text-white" href="/docs">Documentation</a>
            </nav>

            <section class="hero border border-emerald-500/20 rounded-2xl p-6 sm:p-9 mb-6">
                <div class="max-w-3xl">
                    <span class="inline-flex items-center rounded-full border border-emerald-400/20 bg-emerald-400/10 px-3 py-1 text-xs font-semibold text-emerald-300">Market data · risk analytics · developer API</span>
                    <h2 class="mt-4 text-3xl sm:text-4xl font-bold leading-tight text-white">Market data and risk models you can inspect and integrate.</h2>
                    <p class="mt-3 max-w-2xl text-sm sm:text-base leading-7 text-gray-400">Explore public market snapshots, review quantitative outputs, and test the API with a free Starter key.</p>
                    <div class="mt-6 flex flex-wrap gap-3">
                        <a href="#api-access" class="rounded-lg bg-emerald-400 px-4 py-2.5 text-sm font-bold text-gray-950 hover:bg-emerald-300">Get a free API key</a>
                        <a href="/docs" class="rounded-lg border border-gray-700 px-4 py-2.5 text-sm font-semibold text-gray-200 hover:border-gray-500">Explore API docs</a>
                        <a href="mailto:aybarsduran30@gmail.com?subject=AlphaMetrics%20pilot" class="rounded-lg border border-gray-700 px-4 py-2.5 text-sm font-semibold text-gray-200 hover:border-gray-500">Discuss a pilot</a>
                    </div>
                    <div class="mt-6 grid gap-3 sm:grid-cols-3 text-xs text-gray-300">
                        <div class="rounded-lg border border-emerald-500/15 bg-gray-950/40 p-3"><b class="block text-emerald-300">Risk estimates</b><span class="mt-1 block text-gray-400">VaR and expected shortfall from historical data.</span></div>
                        <div class="rounded-lg border border-emerald-500/15 bg-gray-950/40 p-3"><b class="block text-emerald-300">Portfolio context</b><span class="mt-1 block text-gray-400">Cross-asset correlation for a selected basket.</span></div>
                        <div class="rounded-lg border border-emerald-500/15 bg-gray-950/40 p-3"><b class="block text-emerald-300">Strategy checks</b><span class="mt-1 block text-gray-400">A reproducible one-year RSI backtest.</span></div>
                    </div>
                    <p class="mt-4 text-xs leading-5 text-gray-500">Public demo data only. BTC/USDT top-of-book data comes from the exchange named in the live feed; watchlist quotes use public exchange and Yahoo Finance data. This is a research and engineering demo, not investment advice.</p>
                </div>
            </section>

            <section id="market" class="bg-gray-900/80 border border-emerald-500/30 rounded-xl p-5 mb-6 shadow-2xl relative overflow-hidden">
                <div class="flex items-center justify-between mb-3 border-b border-gray-800/80 pb-2">
                    <div class="flex items-center gap-2">
                        <span id="hftDot" class="w-2.5 h-2.5 rounded-full bg-gray-500"></span>
                        <span class="text-xs font-bold uppercase tracking-wider text-emerald-300">BTC/USDT <span class="text-gray-500">/</span> Live market feed</span>
                    </div>
                    <span id="hftStatus" class="text-[10px] text-gray-400 font-mono">Connecting to public order book...</span>
                </div>
                <div class="grid grid-cols-2 sm:grid-cols-5 gap-3">
                    <div class="bg-gray-950/70 p-3 rounded-lg border border-gray-800">
                            <span class="text-[10px] text-gray-500 uppercase block">BTC / USDT mid</span>
                        <b id="hftPrice" class="text-sm text-white font-mono">--</b>
                    </div>
                    <div class="bg-gray-950/70 p-3 rounded-lg border border-gray-800">
                            <span class="text-[10px] text-gray-500 uppercase block">10-snapshot VaR 95%</span>
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

            <section id="api-access" class="bg-gray-900/60 border border-gray-800 rounded-xl p-5 mb-6 shadow-xl">
                <div class="flex items-center justify-between mb-3">
                    <div>
                        <span class="text-xs font-bold uppercase tracking-wider text-emerald-300">Start building</span>
                        <h2 class="mt-1 text-xl font-semibold text-white">Get your Starter API key</h2>
                    </div>
                    <a href="/docs" class="text-xs text-emerald-300 hover:text-emerald-200">Read API docs ↗</a>
                </div>
                <p class="text-sm text-gray-400 mb-5">Verify your email to create a trial client. Your API key is shown once, after verification.</p>
                <form id="signupForm" onsubmit="requestApiKey(event)" class="grid gap-3 sm:grid-cols-2">
                    <label class="text-xs text-gray-400">Company or project name
                        <input id="signupCompany" required maxlength="100" autocomplete="organization" placeholder="e.g. Northstar Labs" class="mt-1 w-full bg-gray-950 border border-gray-800 rounded-lg px-3 py-2.5 text-sm text-gray-100 focus:outline-none focus:border-emerald-500">
                    </label>
                    <label class="text-xs text-gray-400">Email
                        <input id="signupEmail" required type="email" maxlength="100" autocomplete="email" placeholder="you@company.com" class="mt-1 w-full bg-gray-950 border border-gray-800 rounded-lg px-3 py-2.5 text-sm text-gray-100 focus:outline-none focus:border-emerald-500">
                    </label>
                    <div class="sm:col-span-2 flex flex-wrap items-center gap-3">
                        <button id="signupButton" type="submit" class="rounded-lg bg-emerald-400 px-4 py-2.5 text-sm font-bold text-gray-950 hover:bg-emerald-300">Send verification code</button>
                        <p id="signupStatus" role="status" aria-live="polite" class="text-sm text-gray-400"></p>
                    </div>
                </form>
                <form id="verifyForm" onsubmit="verifyApiEmail(event)" class="hidden mt-4 rounded-xl border border-gray-800 bg-gray-950/70 p-4">
                    <p class="text-sm text-gray-300">Enter the code sent to <b id="verificationEmail" class="text-white"></b>.</p>
                    <div class="mt-3 flex flex-col gap-2 sm:flex-row">
                        <input id="verificationCode" required autocomplete="one-time-code" inputmode="text" placeholder="Verification code" class="flex-1 bg-gray-900 border border-gray-800 rounded-lg px-3 py-2.5 text-sm font-mono text-gray-100 focus:outline-none focus:border-emerald-500">
                        <button id="verifyButton" type="submit" class="rounded-lg bg-emerald-400 px-4 py-2.5 text-sm font-bold text-gray-950 hover:bg-emerald-300">Verify email</button>
                    </div>
                    <p id="verifyStatus" role="status" aria-live="polite" class="mt-2 text-sm text-gray-400"></p>
                </form>
                <div id="apiKeyResult" class="hidden mt-4 rounded-xl border border-emerald-500/30 bg-emerald-950/20 p-4">
                    <p class="text-sm font-semibold text-emerald-300">Email verified. Your Starter API key is ready.</p>
                    <p class="mt-1 text-xs text-gray-400">Copy and save it now. For security, it cannot be shown again.</p>
                    <div class="mt-3 flex flex-col gap-2 sm:flex-row">
                        <code id="newApiKey" class="flex-1 overflow-x-auto rounded-lg bg-gray-950 px-3 py-2.5 text-xs text-white"></code>
                        <button type="button" onclick="copyNewApiKey()" class="rounded-lg border border-gray-700 px-4 py-2 text-sm font-semibold text-gray-200 hover:border-gray-500">Copy key</button>
                    </div>
                    <p id="copyStatus" role="status" class="mt-2 text-xs text-gray-400"></p>
                </div>

                <details class="mt-6 border-t border-gray-800 pt-4">
                    <summary class="cursor-pointer text-sm font-semibold text-gray-300 hover:text-white">Already have an API key? Check your usage</summary>
                    <p class="text-xs text-gray-500 mt-3 mb-3">Inspect your plan, rate limit, and remaining trial requests.</p>
                    <div class="flex flex-col sm:flex-row gap-2">
                        <input id="keyInspectorInput" type="password" autocomplete="off" autocapitalize="off" spellcheck="false" placeholder="Paste your API key" class="flex-1 bg-gray-950 border border-gray-800 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-emerald-500 font-mono text-gray-200">
                        <button onclick="inspectApiKey()" class="bg-gray-800 hover:bg-gray-700 border border-gray-700 text-gray-100 font-semibold px-4 py-2 rounded-lg text-sm">Check usage</button>
                    </div>
                </details>
                <div id="inspectorResult" class="hidden mt-3 bg-gray-950/80 border border-gray-800 rounded-lg p-3 text-xs font-mono">
                    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                        <div><span class="text-gray-500">Company:</span> <b id="statCompany" class="text-white block">-</b></div>
                        <div><span class="text-gray-500">Tier:</span> <b id="statTier" class="text-emerald-400 block">-</b></div>
                        <div><span class="text-gray-500">RPM Limit:</span> <b id="statRpm" class="text-gray-300 block">-</b></div>
                        <div><span class="text-gray-500">Trial requests left:</span> <b id="statQuota" class="text-emerald-400 block">-</b></div>
                    </div>
                </div>
            </section>

            <div class="flex items-end justify-between gap-3 mb-3 px-1">
                <div>
                    <h2 class="text-lg font-semibold text-gray-100">Market watchlist</h2>
                    <p class="text-xs text-gray-500 mt-1">Public demo snapshots · change is versus prior close except where the source reports 24-hour change</p>
                </div>
                <span id="watchlistStatus" role="status" aria-live="polite" class="text-[11px] text-gray-400 text-right">Waiting for quotes</span>
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden shadow-2xl">
                <div class="overflow-x-auto">
                    <table class="w-full min-w-[980px] text-left text-sm">
                        <thead class="bg-gray-950 border-b border-gray-800 text-xs text-gray-400 uppercase tracking-wider">
                            <tr>
                                <th class="py-3.5 px-4">Asset</th>
                                <th class="py-3.5 px-4">Price</th>
                                <th class="py-3.5 px-4">Change</th>
                                <th class="py-3.5 px-4">50D Avg</th>
                                <th class="py-3.5 px-4">14D RSI</th>
                                <th class="py-3.5 px-4">Status</th>
                                <th class="py-3.5 px-4 text-right">Backtest</th>
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
                <button onclick="closeModal()" aria-label="Close backtest" class="absolute top-4 right-4 text-gray-400 hover:text-white text-lg font-bold">&times;</button>
                <div id="modalContent">
                    <p class="text-xs text-gray-500 uppercase tracking-wider">1-year RSI strategy backtest</p>
                    <h3 id="modalTicker" class="text-xl font-bold text-emerald-400 mt-1">--</h3>
                    <p class="mt-2 text-xs leading-5 text-gray-500">RSI 14 entry/exit rule on daily history. Excludes fees, slippage, and market impact; historical results do not predict future performance.</p>
                    <div id="modalLoading" class="py-8 text-center text-xs text-gray-400 animate-pulse">Loading one-year RSI backtest…</div>
                    <div id="modalError" role="alert" class="hidden mt-4 rounded-lg border border-amber-500/30 bg-amber-950/20 p-4 text-sm text-amber-200">
                        <p id="modalErrorMessage"></p>
                        <a href="#api-access" onclick="closeModal()" class="mt-3 inline-block text-xs font-semibold text-emerald-300 hover:text-emerald-200">Get an API key for more symbols →</a>
                    </div>
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
                    <p id="modalAnalysisDate" class="mt-3 text-[10px] text-gray-500"></p>
                </div>
            </div>
        </div>

        <script>
            let defaultTickers = [
                'GRAM-ALTIN-TRY', 'USDTRY=X', 'VUAA.L', 'QQQ',
                'NVDA', 'AAPL', 'THYAO.IS', 'ASELS.IS',
                'BTC-USD', 'ETH-USD'
            ];

            let ws = null;
            let hftWs = null;
            let sessionApiKey = null;
            let streamReceived = new Set();
            let streamFailures = 0;

            async function requestApiKey(event) {
                event.preventDefault();
                const button = document.getElementById('signupButton');
                const status = document.getElementById('signupStatus');
                const email = document.getElementById('signupEmail').value.trim().toLowerCase();
                button.disabled = true;
                button.textContent = 'Sending…';
                status.textContent = '';
                try {
                    const response = await fetch('/api/v1/auth/register', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            company_name: document.getElementById('signupCompany').value.trim(),
                            email,
                            tier: 'starter'
                        })
                    });
                    const data = await response.json().catch(() => ({}));
                    if (!response.ok) throw new Error(data.detail || `Request failed (${response.status})`);
                    document.getElementById('verificationEmail').textContent = email;
                    document.getElementById('signupForm').classList.add('hidden');
                    document.getElementById('verifyForm').classList.remove('hidden');
                    document.getElementById('verificationCode').focus();
                } catch (error) {
                    status.textContent = error.message;
                    status.className = 'text-sm text-red-300';
                } finally {
                    button.disabled = false;
                    button.textContent = 'Send verification code';
                }
            }

            async function verifyApiEmail(event) {
                event.preventDefault();
                const button = document.getElementById('verifyButton');
                const status = document.getElementById('verifyStatus');
                button.disabled = true;
                button.textContent = 'Verifying…';
                status.textContent = '';
                try {
                    const response = await fetch('/api/v1/auth/verify-email', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            email: document.getElementById('signupEmail').value.trim().toLowerCase(),
                            token: document.getElementById('verificationCode').value.trim()
                        })
                    });
                    const data = await response.json().catch(() => ({}));
                    if (!response.ok) throw new Error(data.detail || `Verification failed (${response.status})`);
                    sessionApiKey = data.api_key;
                    document.getElementById('newApiKey').textContent = data.api_key;
                    document.getElementById('verifyForm').classList.add('hidden');
                    document.getElementById('apiKeyResult').classList.remove('hidden');
                    document.getElementById('apiKeyResult').scrollIntoView({ behavior: 'smooth', block: 'center' });
                } catch (error) {
                    status.textContent = error.message;
                    status.className = 'mt-2 text-sm text-red-300';
                } finally {
                    button.disabled = false;
                    button.textContent = 'Verify email';
                }
            }

            async function copyNewApiKey() {
                const key = document.getElementById('newApiKey').textContent;
                const status = document.getElementById('copyStatus');
                try {
                    await navigator.clipboard.writeText(key);
                    status.textContent = 'Copied. Store your key somewhere safe.';
                } catch {
                    status.textContent = 'Copy was blocked by your browser. Select the key and copy it manually.';
                }
            }

            function formatQuoteTime(value) {
                if (!value) return 'time unavailable';
                const date = new Date(value);
                return Number.isNaN(date.getTime()) ? 'time unavailable' : date.toLocaleTimeString();
            }

            function escapeHtml(value) {
                return String(value).replace(/[&<>"']/g, char => ({
                    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
                })[char]);
            }

            function backtestAction(ticker) {
                if (ticker === 'GRAM-ALTIN-TRY') {
                    return '<span class="text-[11px] text-gray-500" title="The available backtest uses gold futures, not a historical gram-gold series">Indicative quote</span>';
                }
                return `<button onclick="openQuantModal('${ticker}')" class="text-[11px] bg-slate-800 hover:bg-slate-700 border border-slate-700 text-emerald-400 px-2.5 py-1 rounded transition">1Y Backtest</button>`;
            }

            function connectHftWebSocket() {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/ws/hft-risk`;
                hftWs = new WebSocket(wsUrl);

                hftWs.onopen = () => {
                    document.getElementById('hftStatus').innerText = 'Waiting for public market quote...';
                    document.getElementById('hftStatus').className = 'text-[10px] text-gray-400 font-mono';
                };

                hftWs.onmessage = (event) => {
                    const d = JSON.parse(event.data);
                    if (d.status !== 'live') {
                        document.getElementById('hftStatus').innerText = d.reason || 'Market quote unavailable';
                        document.getElementById('hftStatus').className = 'text-[10px] text-red-400 font-mono';
                        document.getElementById('hftDot').className = 'w-2.5 h-2.5 rounded-full bg-red-400';
                        for (const id of ['hftPrice', 'hftVar95', 'hftCvar', 'hftSpread', 'hftOfi']) {
                            document.getElementById(id).innerText = '--';
                        }
                        return;
                    }
                    document.getElementById('hftStatus').innerText = `${d.exchange} · Updated ${formatQuoteTime(d.data_as_of)}`;
                    document.getElementById('hftStatus').className = 'text-[10px] text-emerald-400 font-mono';
                    document.getElementById('hftDot').className = 'w-2.5 h-2.5 rounded-full bg-emerald-400';
                    document.getElementById('hftPrice').innerText = Number(d.current_price).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
                    const riskPlaceholder = d.risk_status === 'flat_sample' ? 'Flat sample' : `Collecting ${d.sample_count || 0}/15`;
                    document.getElementById('hftVar95').innerText = Number.isFinite(d.var_95) ? d.var_95.toFixed(2) : riskPlaceholder;
                    document.getElementById('hftCvar').innerText = Number.isFinite(d.cvar_95) ? d.cvar_95.toFixed(2) : riskPlaceholder;
                    document.getElementById('hftSpread').innerText = Number(d.spread).toFixed(4);
                    document.getElementById('hftOfi').innerText = Number(d.ofi).toFixed(4);
                };

                hftWs.onclose = () => {
                    document.getElementById('hftStatus').innerText = 'Disconnected. Retrying...';
                    document.getElementById('hftStatus').className = 'text-[10px] text-red-400 font-mono';
                    document.getElementById('hftDot').className = 'w-2.5 h-2.5 rounded-full bg-red-400';
                    for (const id of ['hftPrice', 'hftVar95', 'hftCvar', 'hftSpread', 'hftOfi']) {
                        document.getElementById(id).innerText = '--';
                    }
                    setTimeout(connectHftWebSocket, 3000);
                };
            }

            async function inspectApiKey() {
                const keyInput = document.getElementById('keyInspectorInput');
                const key = keyInput.value.trim();
                const resBox = document.getElementById('inspectorResult');
                if (!key) { alert('Please enter an API Key'); return; }

                try {
                    const res = await fetch('/api/v1/auth/usage', {
                        headers: { 'X-API-Key': key }
                    });
                    if (!res.ok) throw new Error(await res.text());
                    const data = await res.json();
                    sessionApiKey = key;
                    resBox.classList.remove('hidden');
                    document.getElementById('statCompany').innerText = data.company;
                    document.getElementById('statTier').innerText = data.plan_tier.toUpperCase();
                    document.getElementById('statRpm').innerText = data.rate_limit_per_min + ' RPM';
                    document.getElementById('statQuota').innerText = data.remaining_requests + ' / ' + data.monthly_quota;
                } catch (err) {
                    alert('Authentication failed: ' + err.message);
                } finally {
                    keyInput.value = '';
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
                            ${backtestAction(ticker)}
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
                    <td class="py-3.5 px-4 font-semibold text-white">${Number(data.price).toLocaleString(undefined, {maximumFractionDigits: 8})} <span class="text-[10px] text-gray-500">${escapeHtml(data.currency)}</span><span class="block text-[9px] font-normal text-gray-500" title="${escapeHtml(data.quote_source)}">${formatQuoteTime(data.data_as_of)} · ${escapeHtml(data.quote_source)}</span></td>
                    <td class="py-3.5 px-4 font-bold ${changeColor}">${changeSign}${data.change_percent}%</td>
                    <td class="py-3.5 px-4 text-gray-400">${data.fifty_day_average}</td>
                    <td class="py-3.5 px-4">
                        <span class="px-2 py-0.5 rounded text-xs border ${rsiBadge} font-bold">${data.rsi_14}</span>
                    </td>
                    <td class="py-3.5 px-4 font-semibold ${statusColor} text-xs">${data.momentum_status}</td>
                    <td class="py-3.5 px-4 text-right">
                        ${backtestAction(data.ticker)}
                    </td>
                `;
            }

            function showRowError(ticker) {
                const row = document.getElementById(`row-${ticker.replace(/[^a-zA-Z0-9]/g, '_')}`);
                if (!row) return;
                row.children[1].textContent = 'Quote unavailable · retry refresh';
                row.children[1].className = 'py-3.5 px-4 text-amber-300 text-xs';
                for (let index = 2; index <= 5; index++) row.children[index].textContent = '--';
            }

            function updateWatchlistStatus() {
                const status = document.getElementById('watchlistStatus');
                const completed = streamReceived.size;
                status.textContent = `${completed}/${defaultTickers.length} updated${streamFailures ? ` · ${streamFailures} unavailable` : ''}`;
                status.className = `text-[11px] text-right ${streamFailures ? 'text-amber-300' : 'text-gray-400'}`;
            }

            function connectWebSocket() {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/ws/stream`;
                ws = new WebSocket(wsUrl);

                ws.onopen = () => {
                    const badge = document.getElementById('connBadge');
                    badge.className = 'inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-emerald-950 text-emerald-400 border border-emerald-800';
                    badge.innerHTML = '<span class="w-2 h-2 rounded-full bg-emerald-400 mr-2"></span> Stream connected';
                    requestStreamAll();
                };

                ws.onmessage = (event) => {
                    const response = JSON.parse(event.data);
                    if (response.status === 'success') {
                        updateRowUI(response.data);
                        streamReceived.add(response.data.ticker);
                        updateWatchlistStatus();
                    } else if (response.status === 'error' && response.ticker && defaultTickers.includes(response.ticker)) {
                        showRowError(response.ticker);
                        streamReceived.add(response.ticker);
                        streamFailures += 1;
                        updateWatchlistStatus();
                    } else if (response.status === 'error') {
                        document.getElementById('watchlistStatus').textContent = response.message || 'Stream request failed';
                    }
                };

                ws.onclose = () => {
                    const badge = document.getElementById('connBadge');
                    badge.className = 'inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-red-950 text-red-400 border border-red-800';
                    badge.innerHTML = '<span class="w-2 h-2 rounded-full bg-red-400 mr-2"></span> Reconnecting';
                    document.getElementById('watchlistStatus').textContent = 'Stream disconnected';
                    setTimeout(connectWebSocket, 3000);
                };
            }

            function requestStreamAll() {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    streamReceived = new Set();
                    streamFailures = 0;
                    document.getElementById('watchlistStatus').textContent = 'Refreshing quotes...';
                    ws.send(JSON.stringify({ tickers: defaultTickers }));
                }
            }

            async function openQuantModal(ticker) {
                const modal = document.getElementById('quantModal');
                const loading = document.getElementById('modalLoading');
                const results = document.getElementById('modalResults');
                const errorBox = document.getElementById('modalError');
                document.getElementById('modalTicker').innerText = ticker;
                
                modal.classList.remove('hidden');
                modal.classList.add('flex');
                loading.classList.remove('hidden');
                loading.innerText = 'Loading one-year RSI backtest…';
                errorBox.classList.add('hidden');
                results.classList.add('hidden');

                try {
                    const endpoint = sessionApiKey
                        ? `/api/v1/quant/backtest/${encodeURIComponent(ticker)}`
                        : `/api/v1/demo/quant/backtest/${encodeURIComponent(ticker)}`;
                    const headers = sessionApiKey ? { 'X-API-Key': sessionApiKey } : {};
                    const res = await fetch(endpoint, { headers });
                    const data = await res.json().catch(() => ({}));
                    if (!res.ok) throw new Error(data.detail || `Analysis failed (${res.status})`);

                    const formatMetric = (value) => {
                        const number = Number(value);
                        return Number.isFinite(number) ? number.toFixed(2) : '—';
                    };
                    const strategyReturn = Number(data.strategy_return_pct);
                    const buyAndHoldReturn = Number(data.buy_and_hold_return_pct);
                    const stratColor = Number.isFinite(strategyReturn) && strategyReturn >= 0 ? 'text-emerald-400' : 'text-red-400';
                    const stratSign = Number.isFinite(strategyReturn) && strategyReturn > 0 ? '+' : '';
                    const bnhColor = Number.isFinite(buyAndHoldReturn) && buyAndHoldReturn >= 0 ? 'text-emerald-400' : 'text-red-400';
                    const bnhSign = Number.isFinite(buyAndHoldReturn) && buyAndHoldReturn > 0 ? '+' : '';

                    document.getElementById('mStratRet').innerHTML = `<span class="${stratColor}">${stratSign}${formatMetric(data.strategy_return_pct)}%</span>`;
                    document.getElementById('mBnhRet').innerHTML = `<span class="${bnhColor}">${bnhSign}${formatMetric(data.buy_and_hold_return_pct)}%</span>`;
                    document.getElementById('mSharpe').innerText = formatMetric(data.sharpe_ratio);
                    document.getElementById('mDrawdown').innerText = `${formatMetric(data.max_drawdown_pct)}%`;
                    document.getElementById('mTrades').innerText = Number.isFinite(Number(data.total_trades)) ? data.total_trades : '—';
                    document.getElementById('mWinRate').innerText = `${formatMetric(data.win_rate_pct)}%`;
                    const dateLabel = data.analysis_date ? new Date(data.analysis_date).toLocaleString() : 'time unavailable';
                    document.getElementById('modalAnalysisDate').innerText = `Calculated ${dateLabel} · ${data.period || '1 Year'}`;

                    loading.classList.add('hidden');
                    results.classList.remove('hidden');
                } catch (error) {
                    loading.classList.add('hidden');
                    document.getElementById('modalErrorMessage').textContent = error.message || 'Analysis is temporarily unavailable.';
                    errorBox.classList.remove('hidden');
                }
            }

            function closeModal() {
                const modal = document.getElementById('quantModal');
                modal.classList.add('hidden');
                modal.classList.remove('flex');
            }

            document.addEventListener('keydown', event => {
                if (event.key === 'Escape') closeModal();
            });

            window.onload = () => {
                initLayout();
                connectWebSocket();
                connectHftWebSocket();
                setInterval(() => {
                    if (document.visibilityState === 'visible') requestStreamAll();
                }, 60000);
            };
        </script>
    </body>
    </html>
    """
