# AlphaMetrics Financial Intelligence Engine

[![AlphaMetrics CI Engine](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine/actions/workflows/ci.yml)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110.0-009688.svg?style=flat&logo=FastAPI&logoColor=white)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Redis](https://img.shields.io/badge/Redis-Upstash%20RAM%20Cache-DC382D.svg?style=flat&logo=redis&logoColor=white)](https://redis.io/)

A high-performance, asynchronous quantitative risk engine and real-time market data analytics platform built with FastAPI, Redis, and WebSockets. Designed to ingest market feeds, compute mathematical risk metrics (14-Day RSI, Sharpe Ratio, Max Drawdown), evaluate synthetic assets, and serve high-throughput financial endpoints under strict rate-limiting policies.

---

## Live Production Links

* **Live Interactive Terminal:** https://alpha-metrics-engine.onrender.com
* **Interactive Swagger API Docs:** https://alpha-metrics-engine.onrender.com/docs
* **System Observability & Health:** https://alpha-metrics-engine.onrender.com/health

---

## System Architecture Overview

* **Client Layer:** Interactive web dashboard and REST clients consuming real-time market feeds.
* **Security & Ingestion:** Redis-backed sliding-window rate limiting (60 requests/min) protecting ingestion routes[cite: 1].
* **Caching Infrastructure:** Upstash Redis in-memory TTL caching delivering sub-10ms response times for repeat calculations[cite: 1].
* **Quantitative Analysis Engine:** Algorithmic calculation layer computing vectorized 14-day RSI, Buy & Hold comparative benchmarks, annual return, and Maximum Drawdown[cite: 1].
* **Persistence & Stream:** PostgreSQL historical storage alongside bi-directional WebSocket streaming (/ws/stream)[cite: 1, 3].

---

## Core Engineering Features

* **Sub-10ms In-Memory Caching:** Redis TTL layer caching market snapshots and quantitative backtests to eliminate redundant upstream API overhead[cite: 1].
* **Sliding-Window Rate Limiting:** Redis-backed middleware enforcing a 60 req/min threshold per IP, mitigating DDoS and abusive scraping[cite: 1].
* **Synthetic Gold Pricing Engine:** Real-time formula mapping `(GC=F * USDTRY=X) / 31.1034768` to produce live Gram Gold (TRY) asset metrics[cite: 1].
* **Algorithmic Backtesting:** 1-Year historical backtesting engine calculating annual return, Buy & Hold benchmark comparison, Sharpe Ratio, and Maximum Drawdown[cite: 1].
* **Full CI/CD Pipeline:** Automated Pytest execution across pull requests and pushes via GitHub Actions.

---

## API Endpoints Overview

* **GET /** - Web-based Real-Time Quant Dashboard[cite: 1]
* **GET /health** - Observability endpoint returning Redis health, latency, and API version[cite: 1]
* **GET /api/v1/metrics/{ticker}** - Returns price, 24h change, 50D avg, RSI, and risk status (Rate Limit: 60 req/min)[cite: 1]
* **GET /api/v1/quant/backtest/{ticker}** - Returns 1-year backtest performance metrics (Rate Limit: 60 req/min)[cite: 1]
* **WS /ws/stream** - Low-latency bi-directional WebSocket price streamer[cite: 1]

---

## Local Development & Containerization

### Running with Docker Compose
```bash
git clone [https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine.git](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine.git)
cd Alpha-Metrics-Engine
docker compose up --build


python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000

pytest -v test_main.py
