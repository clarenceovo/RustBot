mod connector;
pub mod util;
use connector::OkxMarketDataWebSocketConnector;
use connector::BinanceFuturesWebSocketConnector;
pub mod transport;
use std::error::Error;
pub mod model;
//use transport::redis::RedisClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create OKX connector task
    let okx_task = tokio::spawn(async {
        let connector = OkxMarketDataWebSocketConnector::new();
        let pairs = vec!["BTC-USDT-SWAP".to_string(), "BTC-USDC-SWAP".to_string()];
        connector.connect_and_subscribe(&pairs).await
    });

    // Create Binance Futures connector task
    let binance_task = tokio::spawn(async {
        let connector = BinanceFuturesWebSocketConnector::new();
        println!("Connecting to Binance Futures WebSocket...");
        let symbols = vec!["btcusdt".to_string()];
        connector.connect_and_subscribe(&symbols).await
    });

    // Wait for both tasks to complete
    let (okx_result, binance_result) = tokio::join!(okx_task, binance_task);

    // Handle results
    okx_result??;
    binance_result??;

    Ok(())
}