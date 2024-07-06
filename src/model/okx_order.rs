use std::fmt;
use uuid::Uuid;
use chrono::Utc;
use std::collections::HashMap;
use log::{debug, warn, error};

#[derive(PartialEq, Eq, Debug, Clone)]
enum OrderSide {
    Buy,
    Sell,
}

#[derive(PartialEq, Eq, Debug, Clone)]
enum OrderType {
    Market,
    Limit,
    StopMarket,
    StopLimit,
}

#[derive(PartialEq, Eq, Debug, Clone)]
enum OrderState {
    Live,
    Filled,
}

#[derive(PartialEq, Eq, Debug, Clone)]
enum PosSide {
    Net,
}

struct ExchangeOrder {
    ref_id: Uuid,
    exchange_name: String,
    order_id: String,
    order_state: OrderState,
    price: f64,
    size: f64,
    side: OrderSide,
    manager_id: String,
    strategy: String,
    trading_mode: String,
    instrument: String,
    is_live: bool,
    created_ts: chrono::DateTime<Utc>,
    pos_side: PosSide,
    fill_size: f64,
    fill_price: f64,
    is_filled: bool,
    updated_ts: Option<chrono::DateTime<Utc>>,
    reduce_only: bool,
    order_type: OrderType,
}

impl ExchangeOrder {
    fn new(
        exchange_name: &str,
        strategy: &str,
        manager_id: &str,
        instrument: &str,
        price: f64,
        size: f64,
        side: OrderSide,
        order_type: OrderType,
        reduce_only: bool,
    ) -> Self {
        let ref_id = Uuid::new_v4();
        let created_ts = Utc::now();
        let pos_side = match side {
            OrderSide::Buy => PosSide::Net,
            OrderSide::Sell => PosSide::Net,
        };
        Self {
            ref_id,
            exchange_name: exchange_name.to_string(),
            order_id: "".to_string(),
            order_state: OrderState::Live,
            price,
            size,
            side,
            manager_id: manager_id.to_string(),
            strategy: strategy.to_string(),
            trading_mode: "cross".to_string(),
            instrument: instrument.to_string(),
            is_live: false,
            created_ts,
            pos_side,
            fill_size: 0.0,
            fill_price: 0.0,
            is_filled: false,
            updated_ts: None,
            reduce_only,
            order_type,
        }
    }

    fn is_fully_filled(&mut self) -> bool {
        if self.fill_size == self.size {
            self.is_filled = true;
            self.order_state = OrderState::Filled;
            self.print(format!("Order FILLED! ID:{} @ {}", self.ref_id, Utc::now()));
            true
        } else {
            false
        }
    }

    fn set_state(&mut self, state: OrderState) {
        self.order_state = state;
    }

    fn update_order_price(&mut self, price: f64) {
        self.price = price;
        self.print(format!(
            "Order Updated, ID:{} | PRICE :{} | SIZE :{} | SIDE:{} |@ {}",
            self.ref_id,
            self.price,
            self.size,
            self.side,
            Utc::now()
        ));
    }

    fn update_order_price_size(&mut self, price: f64, size: f64) {
        self.price = price;
        self.size = size;
        self.print(format!(
            "Order Updated, ID:{} | PRICE :{} | SIZE :{} | SIDE:{} |@ {}",
            self.ref_id,
            self.price,
            self.size,
            self.side,
            Utc::now()
        ));
    }

    fn update_order_size(&mut self, size: f64) {
        self.size = size;
        self.is_fully_filled();
    }

    fn update_fill_size(&mut self, fill_size: f64, fill_price: f64) {
        self.updated_ts = Some(Utc::now());
        self.fill_size = fill_size;
        self.fill_price = fill_price;
        self.print(format!(
            "Order Updated, ID:{} | PRICE :{} | SIZE :{} | FILLED:{} |@ {}",
            self.ref_id,
            self.price,
            self.size,
            self.fill_size)
        );
    }
}