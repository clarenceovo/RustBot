use std::cmp::Ordering;
use crc32fast::Hasher;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: f64,
    pub price_string: String,
    pub quantity_string: String,
}
impl OrderBookLevel {
    pub fn new(price: f64, quantity: f64) -> Self {
        OrderBookLevel {
            price,
            quantity,
            price_string: format!("{:.8}", price),
            quantity_string: format!("{:.8}", quantity),

        }
    }
    
}
impl PartialEq for OrderBookLevel {
    fn eq(&self, other: &Self) -> bool {
        self.price == other.price
    }
}

impl PartialOrd for OrderBookLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.price.partial_cmp(&other.price)
    }
}

impl Eq for OrderBookLevel {}

impl Ord for OrderBookLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.price.partial_cmp(&other.price).unwrap_or(Ordering::Equal)
    }


}

#[derive(Debug)]
pub struct OrderBook {
    pub inst_id: String,
    _bids: Vec<OrderBookLevel>,
    _asks: Vec<OrderBookLevel>,
    pub timestamp: i64,
    pub exch_check_sum: i32,
}

impl OrderBook {
    pub fn new(inst_id: String) -> Self {
        OrderBook {
            inst_id,
            _bids: Vec::new(),
            _asks: Vec::new(),
            timestamp: 0,
            exch_check_sum: 0,
        }
    }

    pub fn set_bids_on_snapshot(&mut self, order_book_level_list: Vec<OrderBookLevel>) {
        self._bids = order_book_level_list;
        self._bids.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));
    }

    pub fn set_asks_on_snapshot(&mut self, order_book_level_list: Vec<OrderBookLevel>) {
        self._asks = order_book_level_list;
        self._asks.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    }

    pub fn set_bids_on_update(&mut self, order_book_level: OrderBookLevel) {
        if self._bids.is_empty() || &order_book_level < self._bids.last().unwrap() {
            self._bids.push(order_book_level);
        } else {
            for i in 0..self._bids.len() {
                if &order_book_level > &self._bids[i] {
                    self._bids.insert(i, order_book_level);
                    break;
                } else if order_book_level == self._bids[i] {
                    if order_book_level.quantity == 0.0 {
                        self._bids.remove(i);
                    } else {
                        self._bids[i] = order_book_level;
                    }
                    break;
                }
            }
        }
    }

    pub fn set_asks_on_update(&mut self, order_book_level: OrderBookLevel) {
        if self._asks.is_empty() || &order_book_level > self._asks.last().unwrap() {
            self._asks.push(order_book_level);
        } else {
            for i in 0..self._asks.len() {
                if &order_book_level < &self._asks[i] {
                    self._asks.insert(i, order_book_level);
                    break;
                } else if order_book_level == self._asks[i] {
                    if order_book_level.quantity == 0.0 {
                        self._asks.remove(i);
                    } else {
                        self._asks[i] = order_book_level;
                    }
                    break;
                }
            }
        }
    }

    pub fn set_timestamp(&mut self, timestamp: i64) {
        self.timestamp = timestamp;
    }

    pub fn set_exch_check_sum(&mut self, checksum: i32) {
        self.exch_check_sum = checksum;
    }

    fn _current_check_sum(&self) -> i32 {
        let mut bid_ask_string = String::new();
        for i in 0..std::cmp::max(self._bids.len(), self._asks.len()) {
            if i < self._bids.len() {
                bid_ask_string.push_str(&format!("{}:{}:", self._bids[i].price_string, self._bids[i].quantity_string));
            }
            if i < self._asks.len() {
                bid_ask_string.push_str(&format!("{}:{}:", self._asks[i].price_string, self._asks[i].quantity_string));
            }
            if i + 1 >= 25 {
                break;
            }
        }
        if !bid_ask_string.is_empty() {
            bid_ask_string.pop();
        }
        let mut hasher = Hasher::new();
        hasher.update(bid_ask_string.as_bytes());
        hasher.finalize() as i32
    }

    pub fn do_check_sum(&self) -> bool {
        if self.exch_check_sum == 0 {
            return true; // ignore check sum
        }
        let current_crc = self._current_check_sum();
        current_crc == self.exch_check_sum
    }

    fn _check_empty_array(&self, order_book_array: &[OrderBookLevel]) -> Result<(), String> {
        if order_book_array.is_empty() {
            Err(format!("Orderbook for {}: either bids or asks array not initiated.", self.inst_id))
        } else {
            Ok(())
        }
    }

    pub fn best_bid(&self) -> Result<&OrderBookLevel, String> {
        self._check_empty_array(&self._bids)?;
        Ok(&self._bids[0])
    }

    pub fn best_ask(&self) -> Result<&OrderBookLevel, String> {
        self._check_empty_array(&self._asks)?;
        Ok(&self._asks[0])
    }

    pub fn best_bid_price(&self) -> Result<f64, String> {
        Ok(self.best_bid()?.price)
    }

    pub fn best_ask_price(&self) -> Result<f64, String> {
        Ok(self.best_ask()?.price)
    }

    pub fn bid_by_level(&self, level: usize) -> Result<&OrderBookLevel, String> {
        self._check_empty_array(&self._bids)?;
        let level = level.max(1).min(self._bids.len());
        Ok(&self._bids[level - 1])
    }

    pub fn ask_by_level(&self, level: usize) -> Result<&OrderBookLevel, String> {
        self._check_empty_array(&self._asks)?;
        let level = level.max(1).min(self._asks.len());
        Ok(&self._asks[level - 1])
    }

    pub fn middle_price(&self) -> Result<f64, String> {
        Ok((self.best_bid()?.price + self.best_ask()?.price) / 2.0)
    }

    pub fn get_mid(&self) -> Result<f64, String> {
        Ok(((self.best_bid()?.price + self.best_ask()?.price) / 2.0 * 1000.0).round() / 1000.0)
    }
    pub fn spread_bp(&self) -> Result<f64, String> {
        let spread = (self.best_ask()?.price - self.best_bid()?.price * 10000.0).round();
        Ok(spread)
    }
}