 AlphaMetrics Financial Intelligence & Execution Engine (v4.0)

 [![Docker Image](https://img.shields.io/badge/Docker-Hub%20Image-blue.svg?style=flat&logo=docker&logoColor=white)](https://hub.docker.com/r/aybarsdrn/alphametrics)

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
* **Multi-Tenant Access Control:** Cryptographically secure API tokens for Starter, Pro, and Enterprise clients, with live consumption metering. Public registration issues Starter trial keys; paid tiers are provisioned through the billing flow.
* **Tail-Risk Modeling Layer:** 10,000-iteration Monte Carlo engine generating Parametric Value at Risk (VaR 95/99) and Conditional Value at Risk (CVaR Expected Shortfall).
* **Cross-Asset Correlation Analysis:** High-dimensional return matrices analyzing portfolio diversification and systemic market exposure.
* **Distributed Caching & Ingestion:** Upstash Redis cache delivering sub-10ms response times and sliding-window rate limiting (60 req/min).
* **Streaming Engine:** Asynchronous bi-directional WebSocket interface (`/ws/stream`) broadcasting real-time technical indicators.


 API Endpoints Architecture

Authentication & Tier Management
* `POST /api/v1/auth/register` - Sends an email verification code; it does not create a client or key.
* `POST /api/v1/auth/verify-email` - Verifies the code, creates the Starter client, and returns its API key once.
* `GET /api/v1/auth/usage` - Retrieves real-time token status, rate limits, consumed requests, and remaining monthly quota.

Quantitative & Market Risk Endpoints (Header: `X-API-Key`)
* `GET /api/v1/metrics/{ticker}` - Ingests spot feeds and computes 14-Day RSI, 50D MA, and momentum states.
* `GET /api/v1/quant/var/{ticker}` - Executes 10k Monte Carlo simulations for 1-day/multi-day VaR 95%, VaR 99%, and CVaR.
* `GET /api/v1/quant/correlation` - Computes covariance and correlation matrix across multi-asset baskets (e.g. `?symbols=AAPL,TSLA,NVDA`).
* `GET /api/v1/quant/backtest/{ticker}` - Backtests 1-year algorithmic strategies against Buy & Hold benchmarks (Sharpe, Win Rate, Max Drawdown).
* `GET /api/v1/demo/quant/backtest/{ticker}` - Public, IP-limited backtest for the dashboard watchlist only; cached for five minutes.
* `GET /api/v1/reports/audit/{ticker}` - Generates an executive, printable quantitative risk audit report.
* `WS /ws/stream` - Low-latency real-time market data WebSocket pipeline.

---

## Local Development & Testing
Running Locally


```bash
docker pull aybarsdrn/alphametrics:latest
docker run -p 8000:8000 aybarsdrn/alphametrics:latest

git clone [https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine.git](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine.git)
cd Alpha-Metrics-Engine

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

uvicorn main:app --reload --port 8000



## Real-Time Risk Subsystem
* **Worker:** Asynchronous tick-by-tick risk processor via Redis consumer groups.
* **Metrics:** Live Value at Risk (VaR 95%), Expected Shortfall (CVaR), and Level-2 LOB Spread.
* **Telemetry:** Broadcasts via Redis Pub/Sub to FastAPI WebSockets (`/ws/hft-risk`).

## Security Configuration

The API sends baseline browser security headers and limits browser CORS access
to the Render app and local development origins by default. Set
`ALPHAMETRICS_ALLOWED_ORIGINS` to a comma-separated allowlist when using a
separate frontend. The mock billing tier-upgrade endpoint is disabled unless
`BILLING_WEBHOOK_SECRET` is configured and the matching
`X-Billing-Webhook-Secret` header is sent. This mock endpoint is not a verified
Stripe webhook integration.

### API key handling

Public registration only creates a 15-minute pending verification record.
The client and API key are created only after the one-time code is verified.
On Render Free, configure `RESEND_API_KEY` and `RESEND_FROM_EMAIL` to send
verification mail through Resend's HTTPS API. Render Free blocks outbound SMTP
ports 25, 465, and 587. For local development or hosting that permits SMTP,
`SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`, and
`SMTP_FROM_EMAIL` remain available as a fallback. Set `SMTP_USE_SSL=true` for
implicit TLS (commonly port 465); otherwise STARTTLS is used (commonly port
587). If delivery is not configured or fails, registration is rejected and no
pending record is saved. New API keys contain 256 bits of random entropy, are returned
once, and only SHA-256 hashes are stored. Existing plaintext keys continue to
work and are converted to hashes on first successful use. Registration is
limited to five requests per IP per minute and code verification to ten; these
limits are currently process-local.

For local setup, copy `.env.example` to `.env` and enter your Resend API key.
The sender address must use a domain verified in Resend. Never commit `.env`;
it is excluded by `.gitignore`. On Render, add these values as private
environment variables. Do not use the example key for sending mail.

### Neon Postgres

`neon.ts` declares the Neon service configuration. The local `.neon` directory
holds the project link and is not committed. Run `neon deploy` in the
project root to pull `DATABASE_URL`, `DATABASE_URL_UNPOOLED`, and `NEON_BRANCH`
into `.env.local`. The app reads `.env.local` and then `.env`; process environment
variables take precedence. Install Python dependencies from `requirements.txt`
and run `npm run build` for the Python syntax and dependency check.

The running app uses `DATABASE_URL` for its pooled PostgreSQL connection. On
Render, set `DATABASE_URL` as a private environment variable using the Neon
pooled URL; the local `.env.local` file is ignored by Git and is not sent to
Render. Keep `DATABASE_URL_UNPOOLED` for future schema migrations. Existing
SQLite data is not copied automatically. Neon stores the API client, email
verification, and metric history tables; the in-memory cache is separate.
