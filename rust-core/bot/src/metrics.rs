use axum::{routing::get, Router, response::IntoResponse};
use once_cell::sync::Lazy;
use prometheus::{Encoder, TextEncoder, Registry, IntCounter, Gauge};
use std::net::SocketAddr;

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);
pub static TICKS_TOTAL: Lazy<IntCounter> = Lazy::new(|| IntCounter::new("ticks_total", "Total received bookTicker messages").unwrap());
pub static WS_DISCONNECTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| IntCounter::new("ws_disconnects_total", "Total websocket disconnects").unwrap());
pub static TRADES_TOTAL: Lazy<IntCounter> = Lazy::new(|| IntCounter::new("trades_total", "Total simulated trades").unwrap());
pub static CURRENT_PNL_QUOTE: Lazy<Gauge> = Lazy::new(|| Gauge::new("current_pnl_quote", "Current unrealized PnL in quote asset").unwrap());

pub fn init() {
    let _ = REGISTRY.register(Box::new(TICKS_TOTAL.clone()));
    let _ = REGISTRY.register(Box::new(WS_DISCONNECTS_TOTAL.clone()));
    let _ = REGISTRY.register(Box::new(TRADES_TOTAL.clone()));
    let _ = REGISTRY.register(Box::new(CURRENT_PNL_QUOTE.clone()));
}

async fn healthz() -> impl IntoResponse { "ok" }

async fn metrics() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    let _ = encoder.encode(&metric_families, &mut buffer);
    buffer
}

pub async fn serve(addr: SocketAddr) {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics));

    if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
        let _ = axum::serve(listener, app).await;
    }
}
