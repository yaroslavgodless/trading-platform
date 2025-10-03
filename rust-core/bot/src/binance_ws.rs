use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn, debug};
use serde::Deserialize;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::metrics::{TICKS_TOTAL, WS_DISCONNECTS_TOTAL};

#[derive(Debug, Deserialize, Clone)]
pub struct BookTicker {
    #[serde(rename = "s")] pub symbol: String,
    #[serde(rename = "b")] pub bid_price: String,
    #[serde(rename = "B")] pub bid_qty: String,
    #[serde(rename = "a")] pub ask_price: String,
    #[serde(rename = "A")] pub ask_qty: String,
}

pub async fn run_book_ticker(symbol: &str, tx: Sender<BookTicker>) -> anyhow::Result<()> {
    let url = format!("wss://stream.binance.com:9443/ws/{}@bookTicker", symbol.to_lowercase());
    let mut attempt: u32 = 0;
    let print_all = std::env::var("PRINT_TICKS").ok().map(|v| v == "1" || v.eq_ignore_ascii_case("true")).unwrap_or(false);

    loop {
        info!("Connecting WS (attempt {}): {}", attempt + 1, url);
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!("Connected WS: {}", url);
                attempt = 0;
                let (mut write, mut read) = ws_stream.split();
                let mut local_count: u64 = 0;

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(txt)) => {
                            if let Ok(t) = serde_json::from_str::<BookTicker>(&txt) {
                                TICKS_TOTAL.inc();
                                local_count += 1;
                                if print_all || local_count % 100 == 0 {
                                    debug!("tick {} {} b={} a={}", local_count, t.symbol, t.bid_price, t.ask_price);
                                }
                                let _ = tx.send(t).await;
                            }
                        }
                        Ok(Message::Ping(p)) => { let _ = write.send(Message::Pong(p)).await; }
                        Err(e) => { error!("WS error: {}", e); break; }
                        _ => {}
                    }
                }
                WS_DISCONNECTS_TOTAL.inc();
                warn!("WS disconnected, will reconnect...");
            }
            Err(e) => {
                error!("Failed to connect WS: {}", e);
            }
        }
        attempt += 1;
        let backoff = Duration::from_secs((attempt.min(10)) as u64);
        sleep(backoff).await;
    }
}
