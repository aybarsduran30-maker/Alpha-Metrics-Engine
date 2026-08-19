from fastapi import FastAPI, HTTPException, Security, status, Depends
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import yfinance as yf
import pandas as pd
import datetime

app = FastAPI(
    title="AlphaMetrics Financial Intelligence API",
    version="1.1.0"
)

API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

VALID_API_KEYS = {
    "tier1_secret_prime_token_99": "Enterprise Client Tier 1",
    "tier2_analytics_beta_token_44": "Enterprise Client Tier 2"
}

async def authenticate_client(api_key: str = Security(api_key_header)):
    if api_key not in VALID_API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access Denied: Invalid Security Token."
        )
    return api_key

class MarketRiskMetric(BaseModel):
    ticker: str
    price: float
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

@app.get(
    "/api/v1/metrics/{ticker}",
    response_model=MarketRiskMetric,
    dependencies=[Depends(authenticate_client)]
)
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

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "operational", "latency_ms": 0.6}
