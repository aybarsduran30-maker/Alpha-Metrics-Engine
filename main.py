import os
import asyncio
import redis.asyncio as redis
import pandas as pd
import numpy as np
import yfinance as yf
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import init_db, get_db, AssetMetricHistory

app = FastAPI(
    title="AlphaMetrics Engine",
    version="2.0.0",
    description="Real-Time Financial Analytics, Vectorized Quantitative Risk Engine & Historical Data Store"
)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = None


@app.on_event("startup")
async def startup_event():
    global redis_client
    init_db()
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        redis_client = None

class MetricResponse(BaseModel):
    symbol: str
    price: float
    currency: str
    change_24h: float
    fifty_d_avg: float
    fourteen_d_rsi: float
    risk_status: str

def calculate_rsi(prices: pd.Series, window: int = 14) -> float:
    if len(prices) < window + 1:
        return 50.0
    deltas = prices.diff()
    gains = deltas.where(deltas > 0, 0.0)
    losses = -deltas.where(deltas < 0, 0.0)
    avg_gain = gains.rolling(window=window, min_periods=window).mean().iloc[-1]
    avg_loss = losses.rolling(window=window, min_periods=window).mean().iloc[-1]
    
    if avg_loss == 0 or np.isnan(avg_loss):
        return 100.0 if avg_gain > 0 else 50.0
    if avg_gain == 0 or np.isnan(avg_gain):
        return 0.0
        
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(float(rsi), 2)

async def fetch_synthetic_gold() -> dict:
    loop = asyncio.get_running_loop()
    def get_data():
        gold = yf.Ticker("GC=F").history(period="3mo")
        usdtry = yf.Ticker("USDTRY=X").history(period="3mo")
        return gold, usdtry
    
    gold_df, usd_df = await loop.run_in_executor(None, get_data)
    if gold_df.empty or usd_df.empty:
        raise ValueError("Gold or USD data unavailable")
    
    common_idx = gold_df.index.intersection(usd_df.index)
    if len(common_idx) < 15:
        combined_close = (gold_df['Close'].iloc[-15:] * usd_df['Close'].iloc[-15:]) / 31.1035
    else:
        combined_close = (gold_df.loc[common_idx, 'Close'] * usd_df.loc[common_idx, 'Close']) / 31.1035

    current_price = round(float(combined_close.iloc[-1]), 2)
    prev_price = float(combined_close.iloc[-2]) if len(combined_close) > 1 else current_price
    change_24h = round(((current_price - prev_price) / prev_price) * 100, 2)
    fifty_d = round(float(combined_close.tail(50).mean()), 2)
    rsi = calculate_rsi(combined_close, window=14)

    status = "Overbought" if rsi >= 70 else "Oversold" if rsi <= 30 else "Neutral"
    return {
        "symbol": "GRAM-ALTIN-TRY",
        "price": current_price,
        "currency": "TRY",
        "change_24h": change_24h,
        "fifty_d_avg": fifty_d,
        "fourteen_d_rsi": rsi,
        "risk_status": status
    }

async def fetch_asset_metrics(symbol: str) -> dict:
    if symbol.upper() in ["GRAM-ALTIN-TRY", "GRAM_ALTIN", "ALTIN"]:
        return await fetch_synthetic_gold()

    loop = asyncio.get_running_loop()
    def get_ticker_data():
        t = yf.Ticker(symbol)
        df = t.history(period="3mo")
        cur = t.fast_info.currency or "USD"
        return df, cur

    df, cur = await loop.run_in_executor(None, get_ticker_data)
    if df.empty:
        raise ValueError(f"Symbol '{symbol}' not found")

    closes = df['Close']
    current_price = round(float(closes.iloc[-1]), 2)
    prev_price = float(closes.iloc[-2]) if len(closes) > 1 else current_price
    change_24h = round(((current_price - prev_price) / prev_price) * 100, 2)
    fifty_d = round(float(closes.tail(50).mean()), 2)
    rsi = calculate_rsi(closes, window=14)

    status = "Overbought" if rsi >= 70 else "Oversold" if rsi <= 30 else "Neutral"
    return {
        "symbol": symbol.upper(),
        "price": current_price,
        "currency": cur,
        "change_24h": change_24h,
        "fifty_d_avg": fifty_d,
        "fourteen_d_rsi": rsi,
        "risk_status": status
    }

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/api/v1/metrics/{symbol}", response_model=MetricResponse)
async def get_metrics(symbol: str, db: Session = Depends(get_db)):
    sym = symbol.upper()
    
    
    if redis_client:
        try:
            cached = await redis_client.get(f"metric:{sym}")
            if cached:
                import json
                return MetricResponse(**json.loads(cached))
        except Exception:
            pass

    
    try:
        data = await fetch_asset_metrics(sym)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    
    try:
        record = AssetMetricHistory(
            symbol=data["symbol"],
            price=data["price"],
            change_24h=data["change_24h"],
            fifty_d_avg=data["fifty_d_avg"],
            fourteen_d_rsi=data["fourteen_d_rsi"],
            risk_status=data["risk_status"]
        )
        db.add(record)
        db.commit()
    except Exception:
        db.rollback()

    
    if redis_client:
        try:
            import json
            await redis_client.setex(f"metric:{sym}", 60, json.dumps(data))
        except Exception:
            pass

    return MetricResponse(**data)

@app.get("/api/v1/history/{symbol}")
def get_history(symbol: str, limit: int = 10, db: Session = Depends(get_db)):
    records = db.query(AssetMetricHistory).filter(
        AssetMetricHistory.symbol == symbol.upper()
    ).order_by(AssetMetricHistory.timestamp.desc()).limit(limit).all()
    
    return [
        {
            "price": r.price,
            "change_24h": r.change_24h,
            "rsi": r.fourteen_d_rsi,
            "status": r.risk_status,
            "timestamp": r.timestamp.isoformat()
        } for r in records
    ]

HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AlphaMetrics Engine</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen p-8">
    <div class="max-w-4xl mx-auto space-y-6">
        <header class="border-b border-slate-800 pb-4">
            <h1 class="text-3xl font-bold tracking-tight text-emerald-400">AlphaMetrics Engine</h1>
            <p class="text-slate-400 text-sm">Real-Time Risk, Momentum & Quantitative Analytics</p>
        </header>

        <div class="flex gap-4">
            <input type="text" id="symbolInput" placeholder="Enter Symbol (e.g. NVDA, AAPL, GRAM-ALTIN-TRY)" 
                   class="flex-1 bg-slate-900 border border-slate-800 rounded-lg px-4 py-2 text-slate-100 focus:outline-none focus:border-emerald-500">
            <button onclick="fetchMetrics()" class="bg-emerald-600 hover:bg-emerald-500 font-medium px-6 py-2 rounded-lg transition-colors">Analyze</button>
        </div>

        <div id="resultCard" class="hidden bg-slate-900 border border-slate-800 rounded-xl p-6 space-y-4">
            <div class="flex justify-between items-center border-b border-slate-800 pb-4">
                <div>
                    <h2 id="resSymbol" class="text-2xl font-bold"></h2>
                    <span id="resStatus" class="px-2.5 py-0.5 rounded-full text-xs font-semibold"></span>
                </div>
                <div class="text-right">
                    <div id="resPrice" class="text-3xl font-extrabold"></div>
                    <div id="resChange" class="text-sm font-medium"></div>
                </div>
            </div>
            <div class="grid grid-cols-2 gap-4 pt-2">
                <div class="bg-slate-950 p-4 rounded-lg border border-slate-800/60">
                    <div class="text-slate-400 text-xs uppercase font-semibold">14-Day RSI</div>
                    <div id="resRsi" class="text-xl font-bold mt-1"></div>
                </div>
                <div class="bg-slate-950 p-4 rounded-lg border border-slate-800/60">
                    <div class="text-slate-400 text-xs uppercase font-semibold">50-Day Moving Avg</div>
                    <div id="resAvg" class="text-xl font-bold mt-1"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        async function fetchMetrics() {
            const sym = document.getElementById('symbolInput').value.trim();
            if (!sym) return;
            const resCard = document.getElementById('resultCard');
            try {
                const res = await fetch(`/api/v1/metrics/${sym}`);
                if (!res.ok) throw new Error('Symbol not found');
                const data = await res.json();
                
                document.getElementById('resSymbol').innerText = data.symbol;
                document.getElementById('resPrice').innerText = `${data.price} ${data.currency}`;
                
                const changeEl = document.getElementById('resChange');
                changeEl.innerText = `${data.change_24h > 0 ? '+' : ''}${data.change_24h}% (24h)`;
                changeEl.className = `text-sm font-medium ${data.change_24h >= 0 ? 'text-emerald-400' : 'text-rose-400'}`;

                document.getElementById('resRsi').innerText = data.fourteen_d_rsi;
                document.getElementById('resAvg').innerText = `${data.fifty_d_avg} ${data.currency}`;
                
                const statusEl = document.getElementById('resStatus');
                statusEl.innerText = data.risk_status;
                if (data.risk_status === 'Overbought') {
                    statusEl.className = 'bg-rose-500/10 text-rose-400 border border-rose-500/20 px-2.5 py-0.5 rounded-full text-xs font-semibold';
                } else if (data.risk_status === 'Oversold') {
                    statusEl.className = 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 px-2.5 py-0.5 rounded-full text-xs font-semibold';
                } else {
                    statusEl.className = 'bg-slate-500/10 text-slate-400 border border-slate-500/20 px-2.5 py-0.5 rounded-full text-xs font-semibold';
                }
                
                resCard.classList.remove('hidden');
            } catch (err) {
                alert(err.message);
            }
        }
    </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    return HTML_CONTENT
