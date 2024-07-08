use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use crossbeam::queue::SegQueue;
use log::{info, LevelFilter};
use chrono::Utc;
use serde::Serialize;
use simplelog::{Config, SimpleLogger};

#[derive(Debug, Clone, Serialize)]
struct ExchangeOrder {
    ref_id: String,
    // other fields and methods
}

impl ExchangeOrder {
    pub fn set_state(&mut self, _state: &str) {
        // implement setting state
    }

    pub fn update_fill_size(&mut self, _fill_size: f64, _fill_price: f64) {
        // implement updating fill size
    }

    pub fn to_json(&self) -> String {
        // convert to JSON
        serde_json::to_string(self).unwrap()
    }
}

#[derive(Debug)]
struct CircularOrderBook {
    instrument_name: String,
    bid_queue: Arc<Mutex<VecDeque<ExchangeOrder>>>,
    ask_queue: Arc<Mutex<VecDeque<ExchangeOrder>>>,
    bid_dict: Arc<Mutex<HashMap<String, ExchangeOrder>>>,
    ask_dict: Arc<Mutex<HashMap<String, ExchangeOrder>>>,
    fill_list: Arc<Mutex<Vec<String>>>,
    bid_order_queue: Arc<SegQueue<ExchangeOrder>>,
    ask_order_queue: Arc<SegQueue<ExchangeOrder>>,
}

impl CircularOrderBook {
    pub fn new(instrument_name: &str, max_order_per_side: usize) -> Self {
        SimpleLogger::init(LevelFilter::Info, Config::default()).unwrap();

        CircularOrderBook {
            instrument_name: instrument_name.to_string(),
            bid_queue: Arc::new(Mutex::new(VecDeque::with_capacity(max_order_per_side))),
            ask_queue: Arc::new(Mutex::new(VecDeque::with_capacity(max_order_per_side))),
            bid_dict: Arc::new(Mutex::new(HashMap::new())),
            ask_dict: Arc::new(Mutex::new(HashMap::new())),
            fill_list: Arc::new(Mutex::new(Vec::new())),
            bid_order_queue: Arc::new(SegQueue::new()),
            ask_order_queue: Arc::new(SegQueue::new()),
        }
    }

    fn print_log(&self, log: &str) {
        info!("{}", log);
    }

    pub fn add_bid_order(&self, bid_order: ExchangeOrder) {
        self.bid_order_queue.push(bid_order.clone());

        let bid_queue = Arc::clone(&self.bid_queue);
        let bid_dict = Arc::clone(&self.bid_dict);

        while !self.bid_order_queue.is_empty() {
            let order = self.bid_order_queue.pop().unwrap();
            let mut bid_queue = bid_queue.lock().unwrap();
            let mut bid_dict = bid_dict.lock().unwrap();

            if bid_queue.len() == bid_queue.capacity() {
                let removed_order = bid_queue.pop_front().unwrap();
                self.print_log(&format!("Removing Order {} from {} Bid Queue", removed_order.ref_id, self.instrument_name));
                bid_dict.remove(&removed_order.ref_id);
            }

            bid_queue.push_back(order.clone());
            bid_dict.insert(order.ref_id.clone(), order);
        }
    }

    pub fn add_ask_order(&self, ask_order: ExchangeOrder) {
        self.ask_order_queue.push(ask_order.clone());

        let ask_queue = Arc::clone(&self.ask_queue);
        let ask_dict = Arc::clone(&self.ask_dict);

        while !self.ask_order_queue.is_empty() {
            let order = self.ask_order_queue.pop().unwrap();
            let mut ask_queue = ask_queue.lock().unwrap();
            let mut ask_dict = ask_dict.lock().unwrap();

            if ask_queue.len() == ask_queue.capacity() {
                let removed_order = ask_queue.pop_front().unwrap();
                self.print_log(&format!("Removing Order {} from {} Ask Queue", removed_order.ref_id, self.instrument_name));
                ask_dict.remove(&removed_order.ref_id);
            }

            ask_queue.push_back(order.clone());
            ask_dict.insert(order.ref_id.clone(), order);
        }
    }

    pub fn get_bid_queue(&self) -> Arc<Mutex<VecDeque<ExchangeOrder>>> {
        Arc::clone(&self.bid_queue)
    }

    pub fn get_ask_queue(&self) -> Arc<Mutex<VecDeque<ExchangeOrder>>> {
        Arc::clone(&self.ask_queue)
    }

    pub fn set_order_state(&self, ref_id: &str, state: &str) -> bool {
        if let Some(order) = self.bid_dict.lock().unwrap().get_mut(ref_id) {
            order.set_state(state);
            true
        } else if let Some(order) = self.ask_dict.lock().unwrap().get_mut(ref_id) {
            order.set_state(state);
            true
        } else {
            false
        }
    }

    pub fn get_order_list(&self) -> Vec<String> {
        let bid_keys: Vec<String> = self.bid_dict.lock().unwrap().keys().cloned().collect();
        let ask_keys: Vec<String> = self.ask_dict.lock().unwrap().keys().cloned().collect();
        let fill_list = self.fill_list.lock().unwrap().clone();
        [bid_keys, ask_keys, fill_list].concat()
    }

    pub fn get_order_index(&self, ref_id: &str) -> Option<usize> {
        if self.bid_dict.lock().unwrap().contains_key(ref_id) {
            let bid_queue = self.bid_queue.lock().unwrap();
            for (idx, order) in bid_queue.iter().enumerate() {
                if order.ref_id == ref_id {
                    return Some(idx);
                }
            }
        }
        if self.ask_dict.lock().unwrap().contains_key(ref_id) {
            let ask_queue = self.ask_queue.lock().unwrap();
            for (idx, order) in ask_queue.iter().enumerate() {
                if order.ref_id == ref_id {
                    return Some(idx);
                }
            }
        }
        None
    }

    pub fn delete_fill_list(&self, ref_id: &str) {
        self.fill_list.lock().unwrap().retain(|x| x != ref_id);
    }

    pub fn add_fill_list(&self, ref_id: &str) {
        self.fill_list.lock().unwrap().push(ref_id.to_string());
    }

    pub fn get_order(&self, ref_id: &str) -> Option<ExchangeOrder> {
        if let Some(order) = self.bid_dict.lock().unwrap().get(ref_id) {
            Some(order.clone())
        } else if let Some(order) = self.ask_dict.lock().unwrap().get(ref_id) {
            Some(order.clone())
        } else {
            None
        }
    }

    pub fn set_fill_size(&self, ref_id: &str, fill_size: f64, fill_price: f64) {
        if let Some(order) = self.bid_dict.lock().unwrap().get_mut(ref_id) {
            order.update_fill_size(fill_size, fill_price);
        } else if let Some(order) = self.ask_dict.lock().unwrap().get_mut(ref_id) {
            order.update_fill_size(fill_size, fill_price);
        }
    }

    pub fn is_ref_id_in_list(&self, ref_id: &str) -> bool {
        self.fill_list.lock().unwrap().contains(&ref_id.to_string())
    }

    pub fn get_orderbook_dump(&self) -> serde_json::Value {
        let bid_queue = self.bid_queue.lock().unwrap();
        let ask_queue = self.ask_queue.lock().unwrap();
        let bid_json: Vec<String> = bid_queue.iter().map(|item| item.to_json()).collect();
        let ask_json: Vec<String> = ask_queue.iter().map(|item| item.to_json()).collect();
        let update_ts = Utc::now().to_rfc3339();

        serde_json::json!({
            "bid": bid_json,
            "ask": ask_json,
            "update_ts": update_ts,
        })
    }
}
