# AlphaMetrics Financial Intelligence Engine

[![AlphaMetrics CI Engine](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine/actions/workflows/ci.yml)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110.0-009688.svg?style=flat&logo=FastAPI&logoColor=white)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Redis](https://img.shields.io/badge/Redis-Upstash%20RAM%20Cache-DC382D.svg?style=flat&logo=redis&logoColor=white)](https://redis.io/)

A high-performance, asynchronous quantitative risk engine and real-time market data analytics platform built with **FastAPI**, **Redis**, and **WebSockets**. Designed to ingest market feeds, compute mathematical risk metrics (14-Day RSI, Sharpe Ratio, Max Drawdown), evaluate synthetic assets, and serve high-throughput financial endpoints under strict rate-limiting policies.

---

## Live Production Links

* **Live Interactive Terminal:** [https://alpha-metrics-engine.onrender.com](https://alpha-metrics-engine.onrender.com)
* **Interactive Swagger API Docs:** [https://alpha-metrics-engine.onrender.com/docs](https://alpha-metrics-engine.onrender.com/docs)
* **System Observability & Health:** [https://alpha-metrics-engine.onrender.com/health](https://alpha-metrics-engine.onrender.com/health)

---

## System Architecture

```mermaid
flowchart TD
    Client[Browser / Quant Bot / REST Client] -->|HTTP / REST API| RL[Redis Sliding-Window Rate Limiter]
    Client <-->|Bi-directional Stream| WS[FastAPI WebSocket Endpoint /ws/stream]
    
    RL --> Router[FastAPI Router & Security Layer]
    Router --> Ingest[Async Ingestion & Synthetic Pricing Engine]
    
    Ingest <-->|Sub-10ms In-Memory Cache| Redis[(Upstash Redis RAM Layer)]
    Ingest -->|Vectorized Analytics| Quant[Quant Risk Engine: RSI / Sharpe / Drawdown]
    
    Quant --> DB[(PostgreSQL / AssetMetricHistory)]
    Quant --> LiveUI[Tailwind & Vanilla JS Reactive Terminal]
