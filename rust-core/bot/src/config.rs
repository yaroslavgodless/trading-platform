use std::env;
use rust_decimal::Decimal;

#[derive(Clone, Debug)]
pub struct BotConfig {
    pub symbol: String,
    pub binance_api_key: Option<String>,
    pub binance_api_secret: Option<String>,
    pub paper_trading: bool,
    pub grid_lower: Option<Decimal>,
    pub grid_upper: Option<Decimal>,
    pub grid_levels: Option<u32>,
    pub grid_order_size: Option<Decimal>,
    pub start_balance_quote: Option<Decimal>,
}

impl BotConfig {
    pub fn from_env() -> Self {
        let symbol = env::var("SYMBOL").unwrap_or_else(|_| "ETHUSDT".to_string());
        let binance_api_key = env::var("BINANCE_API_KEY").ok();
        let binance_api_secret = env::var("BINANCE_API_SECRET").ok();
        let paper_trading = env::var("PAPER_TRADING")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        let grid_lower = env::var("GRID_LOWER").ok().and_then(|v| v.parse::<Decimal>().ok());
        let grid_upper = env::var("GRID_UPPER").ok().and_then(|v| v.parse::<Decimal>().ok());
        let grid_levels = env::var("GRID_LEVELS").ok().and_then(|v| v.parse::<u32>().ok());
        let grid_order_size = env::var("GRID_ORDER_SIZE").ok().and_then(|v| v.parse::<Decimal>().ok());
        let start_balance_quote = env::var("START_BALANCE_QUOTE").ok().and_then(|v| v.parse::<Decimal>().ok());

        Self {
            symbol,
            binance_api_key,
            binance_api_secret,
            paper_trading,
            grid_lower,
            grid_upper,
            grid_levels,
            grid_order_size,
            start_balance_quote,
        }
    }
}
