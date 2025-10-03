//! Strategy engine placeholder for Funding-Rate arbitrage and Smart Grid.

use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;

#[derive(Clone, Debug, Default)]
pub struct StrategyConfig {
    pub order_size: Decimal,
}

#[derive(Clone, Debug, Default)]
pub struct PaperEngine {
    pub config: StrategyConfig,
    pub last_bid: Option<Decimal>,
    pub last_ask: Option<Decimal>,
}

impl PaperEngine {
    pub fn new(config: StrategyConfig) -> Self { Self { config, ..Default::default() } }

    pub fn process_ticker(&mut self, bid: Decimal, ask: Decimal) {
        self.last_bid = Some(bid);
        self.last_ask = Some(ask);
    }
}

// --- Minimal Grid Engine ---

#[derive(Clone, Debug)]
pub struct GridConfig {
    pub lower_price: Decimal,
    pub upper_price: Decimal,
    pub levels: u32,
    pub order_size: Decimal,        // in base asset (e.g., ETH)
    pub start_balance_quote: Decimal, // e.g., USDT
}

#[derive(Clone, Debug)]
pub struct Trade {
    pub side: TradeSide,
    pub price: Decimal,
    pub size: Decimal, // base asset amount
    pub pnl_quote: Decimal, // realized PnL in quote asset
}

#[derive(Clone, Debug)]
pub enum TradeSide { Buy, Sell }

#[derive(Clone, Debug)]
pub struct GridEngine {
    cfg: GridConfig,
    grid_prices: Vec<Decimal>,
    pub inventory_base: Decimal,
    pub balance_quote: Decimal,
    last_price: Option<Decimal>,
}

impl GridEngine {
    pub fn new(cfg: GridConfig) -> Self {
        let mut grid_prices = Vec::new();
        if cfg.levels > 1 {
            let step = (cfg.upper_price - cfg.lower_price) / Decimal::from(cfg.levels as i64 - 1);
            for i in 0..cfg.levels {
                grid_prices.push(cfg.lower_price + step * Decimal::from(i as i64));
            }
        } else {
            grid_prices.push(cfg.lower_price);
        }
        Self {
            balance_quote: cfg.start_balance_quote,
            cfg,
            grid_prices,
            inventory_base: Decimal::ZERO,
            last_price: None,
        }
    }

    pub fn process_mid(&mut self, mid: Decimal) -> Option<Trade> {
        if self.grid_prices.is_empty() || mid.is_zero() { return None; }
        let prev = self.last_price;
        self.last_price = Some(mid);
        if let Some(prev_price) = prev {
            // If crossed up through a grid level -> sell; crossed down -> buy
            for level in &self.grid_prices {
                if prev_price < *level && mid >= *level {
                    // Crossed upward: place sell if inventory available
                    if self.inventory_base >= self.cfg.order_size {
                        let proceeds = *level * self.cfg.order_size;
                        self.inventory_base -= self.cfg.order_size;
                        self.balance_quote += proceeds;
                        return Some(Trade { side: TradeSide::Sell, price: *level, size: self.cfg.order_size, pnl_quote: Decimal::ZERO });
                    }
                } else if prev_price > *level && mid <= *level {
                    // Crossed downward: place buy if quote balance sufficient
                    let cost = *level * self.cfg.order_size;
                    if self.balance_quote >= cost {
                        self.inventory_base += self.cfg.order_size;
                        self.balance_quote -= cost;
                        return Some(Trade { side: TradeSide::Buy, price: *level, size: self.cfg.order_size, pnl_quote: Decimal::ZERO });
                    }
                }
            }
        }
        None
    }

    pub fn unrealized_pnl_quote(&self) -> Decimal {
        match (self.last_price, self.inventory_base.is_zero()) {
            (Some(p), false) => self.inventory_base * p,
            _ => Decimal::ZERO,
        }
    }

    pub fn balances(&self) -> (Decimal, Decimal) { (self.inventory_base, self.balance_quote) }
}
