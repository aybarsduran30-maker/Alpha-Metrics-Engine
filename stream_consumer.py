import os
import json
import time
import redis
import numpy as np

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_KEY = "market_trades"
GROUP_NAME = "alphametrics_risk_group"
CONSUMER_NAME = "risk_engine_worker_1"

def calculate_hft_risk(prices: list, horizon_ticks: int = 10, simulations: int = 5000):
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
        simulations
    )
    sim_price_changes = current_price * sim_returns
    
    var_95 = float(np.percentile(sim_price_changes, 5))
    var_99 = float(np.percentile(sim_price_changes, 1))
    cvar_95 = float(sim_price_changes[sim_price_changes <= var_95].mean())
    
    return {
        "current_price": current_price,
        "tick_window": len(prices),
        "tick_volatility": round(std_ret, 6),
        "var_95": round(abs(var_95), 4),
        "var_99": round(abs(var_99), 4),
        "cvar_95": round(abs(cvar_95), 4),
        "updated_at": time.time()
    }

def run_consumer():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    
    try:
        r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
    except redis.exceptions.ResponseError:
        pass

    recent_prices = []
    latest_microstructure = {"spread": 0, "ofi": 0.0, "best_bid": 0, "best_ask": 0}

    while True:
        try:
            entries = r.xreadgroup(
                GROUP_NAME, 
                CONSUMER_NAME, 
                {STREAM_KEY: ">"}, 
                count=50, 
                block=500
            )

            if not entries:
                continue

            ack_ids = []
            for stream, messages in entries:
                for msg_id, payload in messages:
                    trade_data = json.loads(payload.get("data", "{}"))
                    price = trade_data.get("price")
                    if price:
                        recent_prices.append(float(price))
                    
                    if "spread" in trade_data:
                        latest_microstructure["spread"] = trade_data.get("spread", 0)
                        latest_microstructure["ofi"] = trade_data.get("ofi", 0.0)
                        latest_microstructure["best_bid"] = trade_data.get("best_bid", 0)
                        latest_microstructure["best_ask"] = trade_data.get("best_ask", 0)

                    ack_ids.append(msg_id)

            if ack_ids:
                r.xack(STREAM_KEY, GROUP_NAME, *ack_ids)

            if len(recent_prices) > 500:
                recent_prices = recent_prices[-500:]

            risk_metrics = calculate_hft_risk(recent_prices)
            if risk_metrics:
                risk_metrics.update(latest_microstructure)
                r.set("live_hft_risk", json.dumps(risk_metrics))
                r.publish("lob_risk_feed", json.dumps(risk_metrics))
                print(f"[VaR 95%: {risk_metrics['var_95']} | OFI: {risk_metrics['ofi']} | Spread: {risk_metrics['spread']}]")

        except Exception:
            time.sleep(1)

if __name__ == "__main__":
    run_consumer()