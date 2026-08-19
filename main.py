from fastapi import FastAPI, HTTPException, Security, status, Depends
from fastapi.responses import HTMLResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import yfinance as yf
import pandas as pd
import datetime

app = FastAPI(
    title="AlphaMetrics Financial Intelligence API",
    version="1.2.0"
)

API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

VALID_API_KEYS = {
    "tier1_secret_prime_token_99": "Enterprise Client Tier 1",
    "tier2_analytics_beta_token_44": "Enterprise Client Tier 2"
}

async def authenticate_client(api_key: str = Security(api_key_header)):
    if not api_key or api_key not in VALID_API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access Denied: Invalid Security Token."
        )
    return api_key



def calculate_rsi(data: pd.Series, period: int = 14) -> float:
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    last_rsi = rsi.iloc[-1]
    return round(float(last_rsi), 2) if not pd.isna(last_rsi) else 50.0

@app.get("/api/v1/metrics/{ticker}", response_model=MarketRiskMetric)
async def get_metrics(ticker: str):
    ticker_clean = ticker.upper()
    try:
        stock = yf.Ticker(ticker_clean)
        hist = stock.history(period="1mo", interval="1d")
        
        if hist.empty or len(hist) < 14:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Insufficient historical market data for ticker '{ticker_clean}'."
            )
            
        current_price = float(hist['Close'].iloc[-1])
        rsi_val = calculate_rsi(hist['Close'], period=14)
        
        info = stock.fast_info
        fifty_avg = float(info.fifty_day_average) if info.fifty_day_average else current_price
        currency = str(info.currency) if info.currency else "USD"

        if rsi_val >= 70:
            status_desc = "Overbought (High Correction Risk)"
        elif rsi_val <= 30:
            status_desc = "Oversold (Technical Rebound Potential)"
        else:
            status_desc = "Neutral / Stable Momentum"

        return MarketRiskMetric(
            ticker=ticker_clean,
            price=round(current_price, 2),
            currency=currency,
            fifty_day_average=round(fifty_avg, 2),
            rsi_14=rsi_val,
            momentum_status=status_desc,
            generated_at=datetime.datetime.utcnow()
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Market data ingestion failure: {str(e)}"
        )

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AlphaMetrics | Market Intelligence</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
        <style>
            body { font-family: 'JetBrains Mono', monospace; background-color: #0b0f19; color: #f3f4f6; }
        </style>
    </head>
    <body class="min-h-screen flex flex-col items-center justify-center p-4">
        <div class="w-full max-w-xl bg-gray-900 border border-gray-800 rounded-xl p-6 shadow-2xl">
            <div class="flex items-center justify-between border-b border-gray-800 pb-4 mb-6">
                <div>
                    <h1 class="text-xl font-bold text-emerald-400">AlphaMetrics Intelligence</h1>
                    <p class="text-xs text-gray-500">Live Institutional Risk Terminal</p>
                </div>
                <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-emerald-950 text-emerald-400 border border-emerald-800">
                    Production Live
                </span>
            </div>

            <div class="flex gap-2 mb-6">
                <input id="tickerInput" type="text" placeholder="TICKER (e.g. NVDA, AAPL, TSLA)" value="NVDA" 
                       class="flex-1 bg-gray-950 border border-gray-800 rounded-lg px-4 py-3 text-sm focus:outline-none focus:border-emerald-500 uppercase tracking-wider">
                <button onclick="fetchData()" class="bg-emerald-500 hover:bg-emerald-400 text-black font-bold px-6 py-3 rounded-lg text-sm transition">
                    Analyze
                </button>
            </div>

            <div id="loading" class="hidden text-center py-8 text-gray-500 text-sm">Processing live market data...</div>

            <div id="resultCard" class="hidden space-y-4">
                <div class="grid grid-cols-2 gap-4">
                    <div class="bg-gray-950 border border-gray-800 p-4 rounded-lg">
                        <span class="text-xs text-gray-500">Current Price</span>
                        <div id="price" class="text-2xl font-bold text-white mt-1">--</div>
                    </div>
                    <div class="bg-gray-950 border border-gray-800 p-4 rounded-lg">
                        <span class="text-xs text-gray-500">50-Day Moving Avg</span>
                        <div id="avg50" class="text-2xl font-bold text-gray-300 mt-1">--</div>
                    </div>
                </div>

                <div class="bg-gray-950 border border-gray-800 p-4 rounded-lg">
                    <div class="flex justify-between items-center mb-2">
                        <span class="text-xs text-gray-500">14-Day RSI Indicator</span>
                        <span id="rsiValue" class="text-sm font-bold text-emerald-400">--</span>
                    </div>
                    <div class="w-full bg-gray-800 rounded-full h-2 overflow-hidden">
                        <div id="rsiBar" class="bg-emerald-500 h-2 rounded-full transition-all duration-500" style="width: 50%"></div>
                    </div>
                    <div class="flex justify-between text-[10px] text-gray-600 mt-1">
                        <span>Oversold (30)</span>
                        <span>Neutral (50)</span>
                        <span>Overbought (70)</span>
                    </div>
                </div>

                <div class="bg-gray-950 border border-gray-800 p-4 rounded-lg">
                    <span class="text-xs text-gray-500">Momentum Evaluation</span>
                    <div id="status" class="text-sm font-bold text-amber-400 mt-1">--</div>
                </div>
            </div>
        </div>

        <script>
            async function fetchData() {
                const ticker = document.getElementById('tickerInput').value.trim();
                if(!ticker) return;
                
                document.getElementById('loading').classList.remove('hidden');
                document.getElementById('resultCard').classList.add('hidden');

                try {
                    const res = await fetch(`/api/v1/metrics/${ticker}`);
                    if(!res.ok) throw new Error('Data fetch failed');
                    const data = await res.json();

                    document.getElementById('price').innerText = `${data.price} ${data.currency}`;
                    document.getElementById('avg50').innerText = `${data.fifty_day_average} ${data.currency}`;
                    document.getElementById('rsiValue').innerText = data.rsi_14;
                    document.getElementById('status').innerText = data.momentum_status;
                    
                    const rsiBar = document.getElementById('rsiBar');
                    rsiBar.style.width = `${Math.min(Math.max(data.rsi_14, 0), 100)}%`;

                    if(data.rsi_14 >= 70) {
                        rsiBar.className = 'bg-red-500 h-2 rounded-full transition-all duration-500';
                        document.getElementById('status').className = 'text-sm font-bold text-red-400 mt-1';
                    } else if(data.rsi_14 <= 30) {
                        rsiBar.className = 'bg-blue-500 h-2 rounded-full transition-all duration-500';
                        document.getElementById('status').className = 'text-sm font-bold text-blue-400 mt-1';
                    } else {
                        rsiBar.className = 'bg-emerald-500 h-2 rounded-full transition-all duration-500';
                        document.getElementById('status').className = 'text-sm font-bold text-emerald-400 mt-1';
                    }

                    document.getElementById('resultCard').classList.remove('hidden');
                } catch(err) {
                    alert('Market data could not be fetched. Please enter a valid ticker.');
                } finally {
                    document.getElementById('loading').classList.add('hidden');
                }
            }
            window.onload = fetchData;
        </script>
    </body>
    </html>
    """

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "operational", "latency_ms": 0.6}
