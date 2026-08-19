from fastapi import FastAPI, HTTPException, Security, status, Depends
from fastapi.responses import HTMLResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import yfinance as yf
import pandas as pd
import datetime

app = FastAPI(
    title="AlphaMetrics Financial Intelligence API",
    version="2.2.0"
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

class MarketRiskMetric(BaseModel):
    ticker: str
    price: float
    change_percent: float
    currency: str
    fifty_day_average: float
    rsi_14: float
    momentum_status: str
    generated_at: datetime.datetime

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
        if ticker_clean == "GRAM-ALTIN-TRY":
            gold = yf.Ticker("GC=F")
            usdtry = yf.Ticker("USDTRY=X")
            
            gold_hist = gold.history(period="1mo", interval="1d")
            usd_hist = usdtry.history(period="1mo", interval="1d")
            
            if gold_hist.empty or usd_hist.empty:
                raise HTTPException(status_code=404, detail="Synthetic gold data unavailable")
                
            merged_close = (gold_hist['Close'] * usd_hist['Close']) / 31.1034768
            current_price = float(merged_close.iloc[-1])
            prev_close = float(merged_close.iloc[-2]) if len(merged_close) >= 2 else current_price
            change_pct = round(((current_price - prev_close) / prev_close) * 100, 2)
            rsi_val = calculate_rsi(merged_close, period=14) if len(merged_close) >= 14 else 50.0
            
            return MarketRiskMetric(
                ticker="GRAM-ALTIN-TRY",
                price=round(current_price, 2),
                change_percent=change_pct,
                currency="TRY",
                fifty_day_average=round(float(merged_close.mean()), 2),
                rsi_14=rsi_val,
                momentum_status="Overbought" if rsi_val >= 70 else ("Oversold" if rsi_val <= 30 else "Neutral"),
                generated_at=datetime.datetime.utcnow()
            )

        stock = yf.Ticker(ticker_clean)
        hist = stock.history(period="1mo", interval="1d")
        
        if hist.empty or len(hist) < 2:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Insufficient market data for '{ticker_clean}'."
            )
            
        current_price = float(hist['Close'].iloc[-1])
        prev_close = float(hist['Close'].iloc[-2])
        change_pct = round(((current_price - prev_close) / prev_close) * 100, 2)
        
        rsi_val = calculate_rsi(hist['Close'], period=14) if len(hist) >= 14 else 50.0
        
        info = stock.fast_info
        fifty_avg = float(info.fifty_day_average) if info.fifty_day_average else current_price
        currency = str(info.currency) if info.currency else "USD"

        if rsi_val >= 70:
            status_desc = "Overbought"
        elif rsi_val <= 30:
            status_desc = "Oversold"
        else:
            status_desc = "Neutral"

        return MarketRiskMetric(
            ticker=ticker_clean,
            price=round(current_price, 2),
            change_percent=change_pct,
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
        <title>AlphaMetrics | Global Market Intelligence</title>
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
                    <p class="text-xs text-gray-500 mt-1">Institutional Multi-Asset Real-Time Watchlist</p>
                </div>
                <div class="flex items-center gap-2">
                    <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-emerald-950 text-emerald-400 border border-emerald-800">
                        Live Feed
                    </span>
                    <button onclick="renderWatchlist()" class="bg-gray-900 border border-gray-700 hover:border-gray-500 text-xs px-3 py-1.5 rounded-lg text-gray-300 transition">
                        Refresh
                    </button>
                </div>
            </div>

            <div class="flex gap-2 mb-6">
                <input id="newTicker" type="text" placeholder="Add Asset Symbol (e.g. INTC, AMZN, NFLX)" 
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
                            </tr>
                        </thead>
                        <tbody id="watchlistBody" class="divide-y divide-gray-800">
                        </tbody>
                    </table>
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

            async function fetchRowData(ticker) {
                try {
                    const res = await fetch(`/api/v1/metrics/${ticker}`);
                    if (!res.ok) return null;
                    return await res.json();
                } catch {
                    return null;
                }
            }

            async function renderWatchlist() {
                const tbody = document.getElementById('watchlistBody');
                tbody.innerHTML = '';

                defaultTickers.forEach(ticker => {
                    const cleanId = ticker.replace(/[^a-zA-Z0-9]/g, '_');
                    const row = document.createElement('tr');
                    row.className = 'hover:bg-gray-800/40 transition';
                    row.id = `row-${cleanId}`;
                    row.innerHTML = `
                        <td class="py-3.5 px-4 font-bold text-white">${ticker}</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs animate-pulse">Loading...</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs">--</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs">--</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs">--</td>
                        <td class="py-3.5 px-4 text-gray-500 text-xs">--</td>
                    `;
                    tbody.appendChild(row);

                    fetchRowData(ticker).then(data => {
                        const targetRow = document.getElementById(`row-${cleanId}`);
                        if (!targetRow) return;

                        if (!data) {
                            targetRow.innerHTML = `
                                <td class="py-3.5 px-4 font-bold text-white">${ticker}</td>
                                <td colspan="5" class="py-3.5 px-4 text-red-500 text-xs">Offline / Market Closed</td>
                            `;
                            return;
                        }

                        const changeColor = data.change_percent >= 0 ? 'text-emerald-400' : 'text-red-400';
                        const changeSign = data.change_percent >= 0 ? '+' : '';

                        let rsiBadge = 'bg-gray-800 text-gray-300 border-gray-700';
                        let statusColor = 'text-gray-400';

                        if (data.rsi_14 >= 70) {
                            rsiBadge = 'bg-red-950 text-red-400 border-red-800';
                            statusColor = 'text-red-400';
                        } else if (data.rsi_14 <= 30) {
                            rsiBadge = 'bg-blue-950 text-blue-400 border-blue-800';
                            statusColor = 'text-blue-400';
                        } else {
                            rsiBadge = 'bg-emerald-950 text-emerald-400 border-emerald-800';
                            statusColor = 'text-emerald-400';
                        }

                        targetRow.innerHTML = `
                            <td class="py-3.5 px-4 font-bold text-white tracking-wide">${data.ticker}</td>
                            <td class="py-3.5 px-4 font-semibold text-white">${data.price} <span class="text-[10px] text-gray-500">${data.currency}</span></td>
                            <td class="py-3.5 px-4 font-bold ${changeColor}">${changeSign}${data.change_percent}%</td>
                            <td class="py-3.5 px-4 text-gray-400">${data.fifty_day_average}</td>
                            <td class="py-3.5 px-4">
                                <span class="px-2 py-0.5 rounded text-xs border ${rsiBadge} font-bold">${data.rsi_14}</span>
                            </td>
                            <td class="py-3.5 px-4 font-semibold ${statusColor} text-xs">${data.momentum_status}</td>
                        `;
                    });
                });
            }

            function addCustomTicker() {
                const input = document.getElementById('newTicker');
                const val = input.value.trim().toUpperCase();
                if (val && !defaultTickers.includes(val)) {
                    defaultTickers.unshift(val);
                    input.value = '';
                    renderWatchlist();
                }
            }

            window.onload = renderWatchlist;
        </script>
    </body>
    </html>
    """

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "operational", "latency_ms": 0.6}
