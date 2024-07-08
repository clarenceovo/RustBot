mod connector;
pub mod util;
use connector::OkxMarketDataWebSocketConnector;
use connector::BinanceFuturesWebSocketConnector;
pub mod transport;
use std::error::Error;
pub mod model;
pub mod Bolt_Hedger;
use Bolt_Hedger::bolt_hedger::BoltHedger;
use tokio::sync::mpsc;
use log::{info, error, debug, warn};
use model::orderbook::OrderBooks;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging

    let (okx_tx, okx_rx) = mpsc::channel::<OrderBooks>(1000);
    let (binance_tx, binance_rx) = mpsc::channel::<OrderBooks>(1000);

    let pairs = vec!["BTC-USDT-SWAP".to_string(), "BTC-USDC-SWAP".to_string(), "ETH-USDT-SWAP".to_string(), "ETH-USDC-SWAP".to_string()];
    let mut okx_connector = OkxMarketDataWebSocketConnector::new(pairs.clone(),);
    okx_connector.set_sender(okx_tx);

    let symbols = vec!["btcusdt".to_string(), "ethusdt".to_string(), "solusdt".to_string(), "bnbusdt".to_string(), "suibusdt".to_string()];
    let binance_connector = BinanceFuturesWebSocketConnector::new(symbols.clone());

    // Share OrderBooks if needed

    let okx_task = tokio::spawn(async move {
        info!("Connecting to OKX WebSocket...");
        okx_connector.connect_and_subscribe().await
    });

    let binance_task = tokio::spawn(async move {
        info!("Connecting to Binance Futures WebSocket...");
        binance_connector.connect_and_subscribe().await
    });

    let bolt_hedge_task = tokio::spawn(async move {
        // Implement your bolt hedge logic here
        // You can use okx_order_book and binance_order_book here if needed
    });

    // Wait for all tasks to complete
    let (okx_result, binance_result, bolt_hedge_result) = tokio::join!(okx_task, binance_task, bolt_hedge_task);

    // Handle results
    if let Err(e) = okx_result? {
        error!("OKX task error: {}", e);
    }
    if let Err(e) = binance_result? {
        error!("Binance task error: {}", e);
    }
    if let Err(e) = bolt_hedge_result {
        error!("Bolt hedge task error: {}", e);
    }

    Ok(())
}