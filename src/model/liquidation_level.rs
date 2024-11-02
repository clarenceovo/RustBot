use serde::Serialize;
use log::{info, LevelFilter};
use std::collections::HashMap;

#[derive(Serialize)]
pub enum Side {
    BUY,
    SELL,
}

#[derive(Serialize)]
pub struct Level {
    pub price: f64,
    pub quantity: f64,
    pub side: Side,
    pub timestamp: i64,
}

#[derive(Serialize)]
pub struct LiquidationLevel {
    pub symbol: String,
    pub levels: HashMap<u32, Level>,
}

impl Level {
    pub fn new(price: f64, quantity: f64, side: Side, timestamp: i64) -> Self {
        Level {
            price,
            quantity,
            side,
            timestamp,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

impl LiquidationLevel {
    pub fn new(symbol: &str) -> Self {
        LiquidationLevel {
            symbol: symbol.to_string(),
            levels: HashMap::new(),
        }
    }

    pub fn add_level(&mut self, level: Level) {
        self.levels.insert(level.price as u32, level);
    }

    pub fn remove_level(&mut self, timestamp: i64) {
        self.levels.remove(&(timestamp as u32));
    }

    pub fn get_level(&self, timestamp: i64) -> Option<&Level> {
        self.levels.get(&(timestamp as u32))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}