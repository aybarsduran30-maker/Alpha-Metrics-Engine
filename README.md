# AlphaMetrics — market data and risk research API

[![Docker Image](https://img.shields.io/badge/Docker-Hub%20Image-blue.svg?style=flat&logo=docker&logoColor=white)](https://hub.docker.com/r/aybarsdrn/alphametrics)

[![AlphaMetrics CI Engine](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/aybarsduran30-maker/Alpha-Metrics-Engine/actions/workflows/ci.yml)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110.0-009688.svg?style=flat&logo=FastAPI&logoColor=white)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-D71F00.svg?style=flat&logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)

AlphaMetrics is a FastAPI research prototype with a public market dashboard and an authenticated API. It calculates market indicators, Monte Carlo risk estimates, cross-asset correlations, and a simple RSI strategy backtest. API keys are issued after email verification. This codebase supports demos and limited pilots; see the limitations below before relying on it commercially.



## Hosted links

* [Dashboard](https://alpha-metrics-engine.onrender.com)
* [API docs](https://alpha-metrics-engine.onrender.com/docs)
* [Health check](https://alpha-metrics-engine.onrender.com/health)




## Architecture
* **API access:** Hashed API keys, email verification, and Starter usage tracking in PostgreSQL.
* **Risk analytics:** Monte Carlo VaR/CVaR estimates, correlation matrices, and a one-year RSI backtest.
* **Public demo:** A limited watchlist and backtest endpoint. The watchlist WebSocket sends a batch of quote snapshots.
* **BTC/USDT top of book:** Separate WebSocket with Binance, Coinbase, and Kraken fallbacks. The selected venue and quote time appear in the dashboard.
* **Cache and rate limits:** Process-local memory. State resets on restart and is not shared between multiple instances.


## API endpoints

### Authentication and tiers
* `POST /api/v1/auth/register` - Sends an email verification code; it does not create a client or key.
* `POST /api/v1/auth/verify-email` - Verifies the code, creates the Starter client, and returns its API key once.
* `GET /api/v1/auth/usage` - Retrieves token status, rate limits, consumed requests, and remaining trial quota.

### Quantitative and market risk endpoints

These endpoints require an `X-API-Key` header except for the public demo backtest and WebSocket.
* `GET /api/v1/metrics/{ticker}` - Fetches a market snapshot and calculates RSI, 50-day average, and momentum state.
* `GET /api/v1/quant/var/{ticker}` - Monte Carlo VaR/CVaR estimate. `days` is 1–30 and `simulations` is 500–20,000.
* `GET /api/v1/quant/correlation` - Correlation matrix for 2–12 distinct symbols (e.g. `?symbols=AAPL,TSLA,NVDA`).
* `GET /api/v1/quant/backtest/{ticker}` - One-year RSI strategy comparison with buy and hold. Results exclude transaction costs and market impact.
* `GET /api/v1/demo/quant/backtest/{ticker}` - Public, IP-limited backtest for the dashboard watchlist only; cached for five minutes.
* `GET /api/v1/reports/audit/{ticker}` - Generates an executive, printable quantitative risk audit report.
* `WS /ws/stream` - Rate-limited, allowlisted public quote snapshots.

---

## Local Development & Testing
Create a virtual environment and install `requirements.txt`. Set `DATABASE_URL` and email settings as described below; SQLite is only a local default. `npm run build` checks Python syntax and installed Python dependencies, not a frontend bundle.

On macOS or Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

On Windows, activate with `.venv\Scripts\Activate.ps1` and use `python` instead of `python3`.

## BTC/USDT risk display
The background task polls the best bid and ask from public spot feeds and publishes snapshots on `/ws/hft-risk`. VaR and CVaR are model estimates from recent mid-price samples. They do not describe an executable trading strategy or a guaranteed loss bound. This is top-of-book data, not a Level-2 order book.

## Commercial pilot limits
* No payment checkout or verified Stripe webhook. The billing endpoint is a shared-secret mock and must not be used as proof of payment.
* New Starter keys have a 500-request trial quota. The existing `monthly_quota` API field is a legacy name; counters do not reset automatically. Do not advertise the current quota as a renewable subscription.
* In-memory cache and rate limits need a shared store before multiple app instances or contractual uptime targets.
* Public data sources can throttle or change terms. Confirm commercial redistribution rights and obtain a licensed feed before selling market data.
* Backtests and risk estimates need independent model validation, operational monitoring, and customer-specific acceptance criteria before use in financial decisions.

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
