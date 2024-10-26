use crate::{connector::{BinanceFuturesWebSocketConnector, OkxMarketDataWebSocketConnector}, model::orderbook};
use log::{debug, error, info, warn};
use crate::model::orderbook::{OrderBooks};
use crate::model::{okx_order::OkxOrder, okx_order_message::OrderFillData};
use serde_json::json;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use std::thread;
use tokio::time::{sleep, Duration};
use std::error::Error;

pub struct BoltHedger {
    okx_orderbook: Receiver<OrderBooks>,
    trade: Sender<OkxOrder>,
    okx_fill: Receiver<OrderFillData>,
}

impl BoltHedger {
    pub fn new(okx_connector: Receiver<OrderBooks>, trade: Sender<OkxOrder>, fill_channel: Receiver<OrderFillData>) -> Self {
        BoltHedger {
            okx_orderbook: okx_connector,
            trade: trade,
            okx_fill: fill_channel,
        }
    }

    async fn run_strategy(&mut self) {
        //println!("Start bolt hedger channel");
        tokio::select! {
            Some(orderbooks) = self.okx_orderbook.recv() => {
                //println!("Received orderbooks");
                self.process_orderbooks(orderbooks);
            }
            Some(fill_data) = self.okx_fill.recv() => {
                //println!("Received fill data");
                self.process_fill_data(fill_data);
            }
            else => {
                warn!("One of the channels closed");
                // Handle channel closure, maybe reconnect or exit
            }
        }
    }

    fn process_orderbooks(&self, orderbooks: OrderBooks) {
        for (inst_id, orderbook_result) in orderbooks.order_books.iter() {
            match orderbook_result.get_mid() {
                Ok(mid) => {
                    // Process the mid price, e.g., log or use in strategy
                    debug!("Mid price for {} is {}", inst_id, mid);
                }
                Err(e) => {
                    error!("Error calculating mid price for {}: {}", inst_id, e);
                }
            }
        }
    }

    fn process_fill_data(&self, fill_data: OrderFillData) {
        // Process the fill data, e.g., log or use in strategy
        println!("Received fill data in Hedger: {:?}", fill_data);
        // Example: Send an order based on fill data
        /* 
        let order = OkxOrder {
            // Populate with necessary fields
            ..Default::default()
        };

        */

        /* 
        if let Err(e) = self.trade.try_send(order) {
            error!("Failed to send order: {}", e);
        }

        */
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Starting Bolt Hedger");
        
        loop {
            self.run_strategy().await;
            // Optional: Add a small delay to prevent tight looping
            sleep(Duration::from_millis(1)).await;
        }
    }

    pub fn stop(&self) {
        info!("Stopping Bolt Hedger");
        let message = json!({
            "message": "Bolt Hedger stopped"
        });
        info!("{}", message);
    }
}