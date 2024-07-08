use crate::{connector::{BinanceFuturesWebSocketConnector, OkxMarketDataWebSocketConnector}, model::orderbook};
use log::{debug, error, info, warn};
use crate::model::orderbook::{OrderBooks};
use serde_json::json;
use tokio::sync::mpsc::Receiver;
use std::thread;
use tokio::time::{sleep, Duration};
use std::error::Error;


pub struct BoltHedger {
    okx_orderbook: Receiver<OrderBooks>,
}

impl BoltHedger {
    pub fn new(okx_connector: Receiver<OrderBooks>) -> Self {
        BoltHedger {
            okx_orderbook: okx_connector,
        }
    }

    async fn run_strategy(&mut self) {
        match self.okx_orderbook.recv().await {
            Some(orderbooks) => {
                self.process_orderbooks(orderbooks);
            }
            None => {
                warn!("OKX orderbook channel closed");
                // Handle channel closure, maybe reconnect or exit
            }
        }
        
    }

    fn process_orderbooks(&self, orderbooks: OrderBooks) {
        for (inst_id, orderbook_result) in orderbooks.order_books.iter() {
            match orderbook_result.get_mid(){
                Ok(mid) => {
                    //println!("Mid price for {} is {}", inst_id, mid);
                    
                }
                Err(e) => {
                    println!("Error calculating mid price for {}: {}", inst_id, e);
                }

            }
        }
    }

    pub async fn start(&mut self) ->Result<(), Box<dyn Error + Send + Sync>> {
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