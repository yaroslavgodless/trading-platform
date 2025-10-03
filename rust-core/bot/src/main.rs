use env_logger;
use log::{info, error};

mod binance_ws;
mod config;
mod metrics;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use strategy::{PaperEngine, StrategyConfig, GridEngine, GridConfig};
use tokio::sync::mpsc;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    info!("Trading bot starting...");

    metrics::init();
    let metrics_port: u16 = std::env::var("METRICS_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(9000);
    let addr: SocketAddr = ([0,0,0,0], metrics_port).into();
    tokio::spawn(async move { metrics::serve(addr).await });

    let cfg = config::BotConfig::from_env();

    let (tx, mut rx) = mpsc::channel(1024);
    let symbol = cfg.symbol.clone();
    let ws_task = tokio::spawn(async move { binance_ws::run_book_ticker(&symbol, tx).await });

    let mut engine = PaperEngine::new(StrategyConfig { order_size: Decimal::from(0_001u64) });

    let mut grid = match (cfg.grid_lower, cfg.grid_upper, cfg.grid_levels, cfg.grid_order_size, cfg.start_balance_quote) {
        (Some(lower), Some(upper), Some(levels), Some(order_sz), Some(start_q)) => {
            Some(GridEngine::new(GridConfig { lower_price: lower, upper_price: upper, levels, order_size: order_sz, start_balance_quote: start_q }))
        }
        _ => None
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        res = async {
            while let Some(t) = rx.recv().await {
                let bid = t.bid_price.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                let ask = t.ask_price.parse::<Decimal>().unwrap_or(Decimal::ZERO);
                engine.process_ticker(bid, ask);

                if let Some(g) = grid.as_mut() {
                    // use mid price for grid logic
                    let mid = if bid.is_zero() { ask } else { (bid + ask) / Decimal::from(2) };
                    if let Some(_trade) = g.process_mid(mid) {
                        metrics::TRADES_TOTAL.inc();
                    }
                    let pnl = g.unrealized_pnl_quote().to_f64().unwrap_or(0.0);
                    metrics::CURRENT_PNL_QUOTE.set(pnl);
                }
            }
            Ok::<(), anyhow::Error>(())
        } => { let _ = res; }
        ws = ws_task => {
            if let Err(e) = ws { error!("WS task join error: {}", e); }
        }
    }

    info!("Shutting down bot");
    Ok(())
}
