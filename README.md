 AlphaMetrics Financial Intelligence & Execution Engine (v4.0)

[![AlphaMetrics CI Engine](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine/actions/workflows/ci.yml)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110.0-009688.svg?style=flat&logo=FastAPI&logoColor=white)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Redis](https://img.shields.io/badge/Redis-Upstash%20RAM%20Cache-DC382D.svg?style=flat&logo=redis&logoColor=white)](https://redis.io/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-D71F00.svg?style=flat&logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)

A high-throughput, asynchronous quantitative risk engine and enterprise API infrastructure built with FastAPI, Redis, SQLAlchemy, and WebSockets. Engineered to ingest streaming market feeds, execute heavy statistical risk modeling (Monte Carlo VaR/CVaR, Sharpe, Cross-Asset Correlation), and enforce multi-tenant API token provisioning with granular rate limits and monthly quota governance.



 Live Production Links
* **Interactive Quant Dashboard:** https://alpha-metrics-engine.onrender.com
* **Interactive Swagger API Docs:** https://alpha-metrics-engine.onrender.com/docs
* **System Health & Observability:** https://alpha-metrics-engine.onrender.com/health




 Key System Architecture
* **Multi-Tenant Access Control:** Cryptographically secure API token generation (`am_<tier>_<token>`) supporting Starter, Pro, and Enterprise tiers with live consumption metering.
* **Tail-Risk Modeling Layer:** 10,000-iteration Monte Carlo engine generating Parametric Value at Risk (VaR 95/99) and Conditional Value at Risk (CVaR Expected Shortfall).
* **Cross-Asset Correlation Analysis:** High-dimensional return matrices analyzing portfolio diversification and systemic market exposure.
* **Distributed Caching & Ingestion:** Upstash Redis cache delivering sub-10ms response times and sliding-window rate limiting (60 req/min).
* **Streaming Engine:** Asynchronous bi-directional WebSocket interface (`/ws/stream`) broadcasting real-time technical indicators.


 API Endpoints Architecture

Authentication & Tier Management
* `POST /api/v1/auth/register` - Registers corporate clients and issues cryptographic API tokens with tier-based quotas.
* `GET /api/v1/auth/usage` - Retrieves real-time token status, rate limits, consumed requests, and remaining monthly quota.

Quantitative & Market Risk Endpoints (Header: `X-API-Key`)
* `GET /api/v1/metrics/{ticker}` - Ingests spot feeds and computes 14-Day RSI, 50D MA, and momentum states.
* `GET /api/v1/quant/var/{ticker}` - Executes 10k Monte Carlo simulations for 1-day/multi-day VaR 95%, VaR 99%, and CVaR.
* `GET /api/v1/quant/correlation` - Computes covariance and correlation matrix across multi-asset baskets (e.g. `?symbols=AAPL,TSLA,NVDA`).
* `GET /api/v1/quant/backtest/{ticker}` - Backtests 1-year algorithmic strategies against Buy & Hold benchmarks (Sharpe, Win Rate, Max Drawdown).
* `GET /api/v1/reports/audit/{ticker}` - Generates an executive, printable quantitative risk audit report.
* `WS /ws/stream` - Low-latency real-time market data WebSocket pipeline.

---

## Local Development & Testing
Running Locally
```bash
git clone [https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine.git](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine.git)
cd Alpha-Metrics-Engine

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

uvicorn main:app --reload --port 8000
