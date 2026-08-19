# AlphaMetrics Financial Intelligence Engine

A real-time financial analytics backend, quantitative risk terminal, and containerized API built with Python, FastAPI, and Pandas. The system ingests live market data to compute technical indicators, including 14-day Relative Strength Index (RSI), moving averages, and momentum risk evaluations.


# Live Production Demo
- **Live Terminal:** [https://alpha-metrics-engine.onrender.com](https://alpha-metrics-engine.onrender.com)
- **Interactive Swagger Docs:** [https://alpha-metrics-engine.onrender.com/docs](https://alpha-metrics-engine.onrender.com/docs)

---

#Features
- **Real-Time Multi-Asset Feed:** Ingests live data across Equities, Precious Metals, FX, ETFs, and Crypto via `yfinance`.
- **Synthetic Asset Calculation:** Dynamic pricing model computing Gram Gold (TRY) from Troy Ounce and USD/TRY time-series data.
- **Quantitative RSI Engine:** Vectorized RSI computation and risk status detection using `pandas`.
- **High-Performance Caching:** Cache-Aside architecture with Redis integration for sub-5ms cached latency.
- **Enterprise Security:** Header-based API Key validation (`X-API-KEY`) with strict Pydantic v2 schemas.
- **Production Architecture:** Containerized with multi-stage Docker builds and automated 24/7 uptime monitoring.

---

# Stack
- **Backend:** Python 3.12, FastAPI, Uvicorn
- **Data & Computation:** Pandas, NumPy, yfinance
- **Caching & Infrastructure:** Redis 7, Docker, Docker Compose, Render
- **Validation & Security:** Pydantic v2, API Key Auth

---

Local Deployment (Docker Compose)
To run the full stack (FastAPI + Redis) locally:

```bash
git clone [https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine.git](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine.git)
cd Alpha-Metrics-Engine
docker compose up --build
```bash

git clone [https://github.com/aybarsduran30-maker/AlphaMetrics-Engine.git](https://github.com/aybarsduran30-maker/AlphaMetrics-Engine.git)


pip install -r requirements.txt


uvicorn main:app --reload

Built and deployed AlphaMetrics Engine, a real-time financial analytics terminal and API.

The stack and architecture:
- FastAPI backend integrated with Pandas for time-series calculations
- Vectorized 14-day RSI and moving average risk metrics
- Header-based API key authentication and strict Pydantic schemas
- Embedded dark-mode web terminal
- Hosted on Render with automated CI/CD from GitHub

Live Dashboard: https://alpha-metrics-engine.onrender.com
API Docs: https://alpha-metrics-engine.onrender.com/docs
GitHub Repo: https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine

#python #fastapi #datascience #backend #fintech #softwareengineering
