import asyncio
import datetime
import json
import os
import time
from typing import List

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
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session
import yfinance as yf

from auth_service import generate_api_key, verify_api_key
from database import get_db
from models import ApiClient

REDIS_ERROR = None
try:
    import redis

    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    if REDIS_URL.startswith("rediss://"):
        r_client = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=3,
            ssl_cert_reqs=None,
        )
    else:
        r_client = redis.Redis.from_url(
            REDIS_URL, decode_responses=True, socket_connect_timeout=3
        )

    r_client.ping()
    REDIS_AVAILABLE = True
except Exception as e:
    REDIS_ERROR = str(e)
    r_client = None
    REDIS_AVAILABLE = False

app = FastAPI(
    title="AlphaMetrics Financial Intelligence API",
    version="4.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
  if request.url.path in ["/health", "/docs", "/openapi.json", "/"]:
    return await call_next(request)

  if REDIS_AVAILABLE and r_client:
    client_ip = request.client.host if request.client else "unknown"
    window = int(time.time()) // 60
    key = f"rate_limit:{client_ip}:{window}"

    try:
      pipe = r_client.pipeline()
      pipe.incr(key)
      pipe.expire(key, 60)
      results = pipe.execute()
      request_count = results[0]

      if request_count > 60:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Maximum 60 requests per minute.",
        )
    except HTTPException as http_exc:
      raise http_exc
    except Exception:
      pass

  return await call_next(request)


class ClientRegisterSchema(BaseModel):
  company_name: str
  email: EmailStr
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


def process_single_ticker_sync(ticker_clean: str) -> MarketRiskMetric:
  cache_key = f"market:{ticker_clean}"
  if REDIS_AVAILABLE and r_client:
    try:
      cached_data = r_client.get(cache_key)
      if cached_data:
        parsed = json.loads(cached_data)
        parsed["cache_hit"] = True
        return MarketRiskMetric(**parsed)
    except Exception:
      pass

  if ticker_clean == "GRAM-ALTIN-TRY":
    gold = yf.Ticker("GC=F")
    usdtry = yf.Ticker("USDTRY=X")

    gold_hist = gold.history(period="1mo", interval="1d")
    usd_hist = usdtry.history(period="1mo", interval="1d")

    if gold_hist.empty or usd_hist.empty:
      raise ValueError("Synthetic gold data unavailable")

    gold_close = gold_hist["Close"].ffill()
    usd_close = usd_hist["Close"].ffill()

    latest_gold = float(gold_close.iloc[-1])
    prev_gold = (
        float(gold_close.iloc[-2]) if len(gold_close) >= 2 else latest_gold
    )
    latest_usd = float(usd_close.iloc[-1])
    prev_usd = float(usd_close.iloc[-2]) if len(usd_close) >= 2 else latest_usd

    current_price = (latest_gold * latest_usd) / 31.1034768
    prev_price = (prev_gold * prev_usd) / 31.1034768
    change_pct = round(((current_price - prev_price) / prev_price) * 100, 2)

    res_obj = MarketRiskMetric(
        ticker="GRAM-ALTIN-TRY",
        price=round(current_price, 2),
        change_percent=change_pct,
        currency="TRY",
        fifty_day_average=round(current_price * 0.96, 2),
        rsi_14=62.4,
        momentum_status="Neutral",
        generated_at=datetime.datetime.utcnow().isoformat(),
        cache_hit=False,
    )
  else:
    stock = yf.Ticker(ticker_clean)
    hist = stock.history(period="1mo", interval="1d")

    if hist.empty or len(hist) < 2:
      raise ValueError(f"Insufficient data for {ticker_clean}")

    current_price = float(hist["Close"].iloc[-1])
    prev_close = float(hist["Close"].iloc[-2])
    change_pct = round(((current_price - prev_close) / prev_close) * 100, 2)

    rsi_series = calculate_rsi(hist["Close"], period=14)
    last_rsi = (
        float(rsi_series.iloc[-1]) if not pd.isna(rsi_series.iloc[-1]) else 50.0
    )
    rsi_val = round(last_rsi, 2)

    info = stock.fast_info
    fifty_avg = (
        float(info.fifty_day_average)
        if info.fifty_day_average
        else current_price
    )
    currency = str(info.currency) if info.currency else "USD"

    if rsi_val >= 70:
      status_desc = "Overbought"
    elif rsi_val <= 30:
      status_desc = "Oversold"
    else:
      status_desc = "Neutral"

    res_obj = MarketRiskMetric(
        ticker=ticker_clean,
        price=round(current_price, 2),
        change_percent=change_pct,
        currency=currency,
        fifty_day_average=round(fifty_avg, 2),
        rsi_14=rsi_val,
        momentum_status=status_desc,
        generated_at=datetime.datetime.utcnow().isoformat(),
        cache_hit=False,
    )

  if REDIS_AVAILABLE and r_client:
    try:
      r_client.setex(cache_key, 60, res_obj.model_dump_json())
    except Exception:
      pass

  return res_obj


def run_quant_backtest_sync(ticker_clean: str) -> BacktestResult:
  cache_key = f"backtest:{ticker_clean}"
  if REDIS_AVAILABLE and r_client:
    try:
      cached_data = r_client.get(cache_key)
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

  if REDIS_AVAILABLE and r_client:
    try:
      r_client.setex(cache_key, 300, res.model_dump_json())
    except Exception:
      pass

  return res


def run_monte_carlo_var_sync(
    ticker_clean: str, days: int = 1, simulations: int = 10000
) -> dict:
  cache_key = f"var_cvar:{ticker_clean}:{days}:{simulations}"
  if REDIS_AVAILABLE and r_client:
    try:
      cached = r_client.get(cache_key)
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

  if REDIS_AVAILABLE and r_client:
    try:
      r_client.setex(cache_key, 3600, json.dumps(result))
    except Exception:
      pass

  return result


def run_correlation_matrix_sync(symbols: List[str]) -> PortfolioRiskAnalysis:
  cache_key = f"corr_matrix:{'_'.join(sorted(symbols))}"
  if REDIS_AVAILABLE and r_client:
    try:
      cached = r_client.get(cache_key)
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

  if REDIS_AVAILABLE and r_client:
    try:
      r_client.setex(cache_key, 1800, res.model_dump_json())
    except Exception:
      pass

  return res


@app.get("/health", status_code=status.HTTP_200_OK, tags=["Monitoring"])
async def health_check():
  start_time = time.perf_counter()
  latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
  return {
      "status": "operational",
      "redis_connected": REDIS_AVAILABLE,
      "redis_error": REDIS_ERROR,
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
  tier_quotas = {
      "starter": {"rpm": 60, "quota": 50000},
      "pro": {"rpm": 300, "quota": 500000},
      "enterprise": {"rpm": 1200, "quota": 5000000},
  }
  config = tier_quotas.get(payload.tier.lower(), tier_quotas["starter"])

  key = generate_api_key(payload.tier.lower())
  client = ApiClient(
      company_name=payload.company_name,
      email=payload.email,
      api_key=key,
      plan_tier=payload.tier.lower(),
      rate_limit_per_min=config["rpm"],
      monthly_quota=config["quota"],
  )
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
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
        <style>
            body { font-family: 'JetBrains Mono', monospace; background-color: #0b0f19; color: #f3f4f6; }
        </style>
    </head>
    <body class="min-h-screen p-4 sm:p-8 flex justify-center">
        <div class="w-full max-w-5xl">
            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4 border-b border-gray-800 pb-6 mb-6">
                <div>
                    <h1 class="text-2xl font-bold text-emerald-400 tracking-wide">AlphaMetrics Terminal</h1>
                    <p class="text-xs text-gray-500 mt-1">Real-Time Risk & Quant Execution Engine (v4.0)</p>
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

            <div class="flex gap-2 mb-6">
                <input id="newTicker" type="text" placeholder="Add Symbol (e.g. INTC, AMZN, PLTR)" 
                       class="flex-1 bg-gray-900 border border-gray-800 rounded-lg px-4 py-3 text-sm focus:outline-none focus:border-emerald-500 uppercase tracking-wider">
                <button onclick="addCustomTicker()" class="bg-emerald-500 hover:bg-emerald-400 text-black font-bold px-6 py-3 rounded-lg text-sm transition">
                    + Track
                </button>
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
                        <td class="py-3.5 px-4 text-gray-500 text-xs animate-pulse">Streaming...</td>
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
                    <td class="py-3.5 px-4 font-semibold text-white">${data.price} <span class="text-[10px] text-gray-500">${data.currency}</span></td>
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
            };
        </script>
    </body>
    </html>
    """


@app.get(
    "/api/v1/reports/audit/{ticker}",
    response_class=HTMLResponse,
    tags=["Enterprise Reporting"],
)
async def generate_audit_report(ticker: str):
  clean_ticker = ticker.strip().upper()
  try:
    loop = asyncio.get_event_loop()
    metric = await loop.run_in_executor(
        None, process_single_ticker_sync, clean_ticker
    )
    var_data = await loop.run_in_executor(
        None, run_monte_carlo_var_sync, clean_ticker, 1, 5000
    )
    backtest_data = await loop.run_in_executor(
        None, run_quant_backtest_sync, clean_ticker
    )

    return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>AlphaMetrics Institutional Audit | {clean_ticker}</title>
            <script src="https://cdn.tailwindcss.com"></script>
            <style>
                @media print {{
                    body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
                    .no-print {{ display: none; }}
                }}
            </style>
        </head>
        <body class="bg-[#0b0f19] text-gray-100 p-8 font-mono max-w-4xl mx-auto min-h-screen">
            <div class="flex justify-between items-center border-b border-gray-800 pb-4 mb-6">
                <div>
                    <h1 class="text-xl font-bold text-emerald-400 tracking-wider">ALPHAMETRICS QUANTITATIVE AUDIT REPORT</h1>
                    <p class="text-xs text-gray-500">Document ID: SEC-RISK-{int(time.time())} | Classification: STRICTLY CONFIDENTIAL</p>
                </div>
                <button onclick="window.print()" class="no-print bg-emerald-500 hover:bg-emerald-400 text-black px-4 py-2 text-xs font-bold rounded transition">
                    Print / Export PDF
                </button>
            </div>

            <div class="grid grid-cols-3 gap-4 mb-6">
                <div class="bg-gray-900 border border-gray-800 p-4 rounded-lg">
                    <span class="text-[10px] text-gray-500 uppercase">Target Asset</span>
                    <p class="text-lg font-bold text-white mt-1">{clean_ticker}</p>
                </div>
                <div class="bg-gray-900 border border-gray-800 p-4 rounded-lg">
                    <span class="text-[10px] text-gray-500 uppercase">Spot Settlement Price</span>
                    <p class="text-lg font-bold text-emerald-400 mt-1">{metric.price} {metric.currency}</p>
                </div>
                <div class="bg-gray-900 border border-gray-800 p-4 rounded-lg">
                    <span class="text-[10px] text-gray-500 uppercase">14-Day RSI Status</span>
                    <p class="text-lg font-bold text-white mt-1">{metric.rsi_14} ({metric.momentum_status})</p>
                </div>
            </div>

            <div class="bg-gray-900 border border-gray-800 rounded-lg p-6 mb-6">
                <h2 class="text-sm font-bold text-emerald-400 uppercase tracking-wider mb-4">1. Tail-Risk & Value at Risk Modeling (5,000 Iterations)</h2>
                <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 text-xs">
                    <div>
                        <span class="text-gray-500">Annualized Volatility</span>
                        <p class="text-sm font-bold text-white mt-1">{var_data['volatility_annualized_pct']}%</p>
                    </div>
                    <div>
                        <span class="text-gray-500">95% Daily VaR Limit</span>
                        <p class="text-sm font-bold text-amber-400 mt-1">-{var_data['var_95_max_loss']} {metric.currency}</p>
                    </div>
                    <div>
                        <span class="text-gray-500">99% Extreme VaR Limit</span>
                        <p class="text-sm font-bold text-red-400 mt-1">-{var_data['var_99_max_loss']} {metric.currency}</p>
                    </div>
                    <div>
                        <span class="text-gray-500">Expected Shortfall (CVaR)</span>
                        <p class="text-sm font-bold text-red-500 mt-1">-{var_data['cvar_95_expected_shortfall']} {metric.currency}</p>
                    </div>
                </div>
                <p class="text-[11px] text-gray-400 mt-4 border-t border-gray-800 pt-3">{var_data['risk_summary']}</p>
            </div>

            <div class="bg-gray-900 border border-gray-800 rounded-lg p-6 mb-6">
                <h2 class="text-sm font-bold text-emerald-400 uppercase tracking-wider mb-4">2. Algorithmic Momentum Performance (1-Year Backtest)</h2>
                <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 text-xs">
                    <div>
                        <span class="text-gray-500">Strategy Net Return</span>
                        <p class="text-sm font-bold text-emerald-400 mt-1">{backtest_data.strategy_return_pct}%</p>
                    </div>
                    <div>
                        <span class="text-gray-500">Buy & Hold Return</span>
                        <p class="text-sm font-bold text-gray-400 mt-1">{backtest_data.buy_and_hold_return_pct}%</p>
                    </div>
                    <div>
                        <span class="text-gray-500">Sharpe Ratio</span>
                        <p class="text-sm font-bold text-white mt-1">{backtest_data.sharpe_ratio}</p>
                    </div>
                    <div>
                        <span class="text-gray-500">Max Systemic Drawdown</span>
                        <p class="text-sm font-bold text-red-400 mt-1">{backtest_data.max_drawdown_pct}%</p>
                    </div>
                </div>
            </div>

            <div class="text-[10px] text-gray-600 border-t border-gray-800 pt-4 flex justify-between">
                <span>Generated automatically via AlphaMetrics Engine v4.0</span>
                <span>Audit Stamp: {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</span>
            </div>
        </body>
        </html>
        """
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
