use crate::connector::{BinanceFuturesWebSocketConnector, OkxMarketDataWebSocketConnector};
use log::{debug, error, info, warn};
use serde_json::json;

pub struct BoltHedger {
    okx_connector: OkxMarketDataWebSocketConnector,
    binance_connector: BinanceFuturesWebSocketConnector,
}

impl BoltHedger {
    pub fn new(
        okx_connector: OkxMarketDataWebSocketConnector,
        binance_connector: BinanceFuturesWebSocketConnector,
    ) -> Self {
        BoltHedger {
            okx_connector,
            binance_connector,
        }
    }
    fn run_strategy(&self) {
        print!("Hello from run_strategy");
    }
    pub fn start(&self) {
        info!("Starting Bolt Hedger");
        loop {
            self.run_strategy();
        
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
