# Trading Platform Project

## Architecture (high-level)
- Core strategies: Funding rate arbitrage (spot + perps) and Smart Grid.
- Data layer: WebSocket managers, order book aggregation, normalization.
- Storage: TimescaleDB (historical), Redis (cache), SQLite (local state).
- Execution: Smart order router, order manager with retries, fee/slippage tracking.
- Risk: position limits, drawdown controller, circuit breaker.

## Services
- Docker: Kafka, RabbitMQ, TimescaleDB (Postgres), Redis.
- Rust core: `market-data`, `strategy` (lib), `bot` (binary).

## Getting started
1. Copy `.env.example` to `.env` and fill values.
2. `docker compose up -d` to start infra.
3. Build Rust workspace in `rust-core/`: `cargo build --workspace`.

## Roadmap
- MVP: Binance WS, simple grid on one pair, basic risk, logging.
- Multi-exchange: add Bybit, funding arb, paper trading.
- Advanced: dynamic grid, cross-exchange arb, monitoring and alerts.
