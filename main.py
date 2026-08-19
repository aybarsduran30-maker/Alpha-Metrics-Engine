from fastapi import FastAPI, HTTPException, Security, status, Depends
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import yfinance as yf
import datetime

app = FastAPI(
    title="AlphaMetrics Financial Intelligence API",
    version="1.0.0"
)


API_KEY_NAME = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

VALID_API_KEYS = {
    "tier1_secret_prime_token_99": "Enterprise Fund Access",
    "jpmorgan_demo_key_2026": "Tier-1 Partner Access"
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
    status: str
    generated_at: datetime.datetime


@app.get(
    "/api/v1/metrics/{ticker}",
    response_model=MarketRiskMetric,
    dependencies=[Depends(authenticate_client)]
)
async def get_metrics(ticker: str):
    ticker_clean = ticker.upper()
    try:
        stock = yf.Ticker(ticker_clean)
        info = stock.fast_info
        
        current_price = float(info.last_price) if info.last_price else 0.0
        fifty_avg = float(info.fifty_day_average) if info.fifty_day_average else current_price
        currency = str(info.currency) if info.currency else "USD"
        
        if current_price == 0.0:
            raise HTTPException(status_code=404, detail=f"Ticker '{ticker_clean}' verisi bulunamadi.")

        status_text = "Bullish Momentum" if current_price >= fifty_avg else "Bearish / Pullback"

        return MarketRiskMetric(
            ticker=ticker_clean,
            price=round(current_price, 2),
            currency=currency,
            fifty_day_average=round(fifty_avg, 2),
            status=status_text,
            generated_at=datetime.datetime.utcnow()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Veri cekme hatasi: {str(e)}")

@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "operational", "latency_ms": 0.8}