mod connector;
pub mod util;
use connector::OkxMarketDataWebSocketConnector;
use connector::BinanceFuturesWebSocketConnector;
pub mod transport;
use std::error::Error;
pub mod model;
use tokio::sync::mpsc;
//use transport::redis::RedisClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    //let (okx_data_tx, okx_data_rx) = mpsc::channel(100);
    //let (binance_data_tx, binance_data_rx) = mpsc::channel(100);
    // Create OKX connector task
    let okx_task = tokio::spawn(async {

        let pairs = vec!["BTC-USDT-SWAP".to_string(), "BTC-USDC-SWAP".to_string(), "ETH-USDT-SWAP".to_string(), "ETH-USDC-SWAP".to_string()];
        let connector = OkxMarketDataWebSocketConnector::new(pairs.clone());
      
        connector.connect_and_subscribe().await
    });

    // Create Binance Futures connector task
    let binance_task = tokio::spawn(async {

        println!("Connecting to Binance Futures WebSocket...");
        let symbols = vec!["btcusdt".to_string(),"ethusdt".to_string(),"solusdt".to_string(),"bnbusdt".to_string(),"suibusdt".to_string()];
        let connector = BinanceFuturesWebSocketConnector::new(symbols.clone());
        connector.connect_and_subscribe().await
    });

    let bolt_hedge_task = tokio::spawn(async {
         
    });


    // Wait for both tasks to complete
    let (okx_result, binance_result,bolt_hedge_task) = tokio::join!(okx_task, binance_task,bolt_hedge_task);

    // Handle results
    okx_result??;
    binance_result??;

    Ok(())
}