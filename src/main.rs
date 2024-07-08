mod connector;
pub mod util;
use base64::read;
use tokio::fs::File;
use tokio::io::{AsyncReadExt};
use connector::OkxMarketDataWebSocketConnector;
use connector::BinanceFuturesWebSocketConnector;
pub mod transport;
use std::error::Error;
pub mod model;
pub mod Bolt_Hedger;
use Bolt_Hedger::bolt_hedger::BoltHedger;
use model::okx_order::OkxOrder;
use tokio::sync::mpsc;
use serde_json::Value;
use log::{info, error, debug, warn};
use model::orderbook::OrderBooks;
use tokio::time::{sleep, Duration};
use std::path::Path;
use connector::okx_trade_client::OkxTradeClient;

async fn read_config(file_path: &str) -> Result<Value, Box<dyn Error>> {
    match File::open(file_path).await {
        Ok(mut file) => {
            let mut contents = Vec::new();
            file.read_to_end(&mut contents).await?;
            let config: Value = serde_json::from_slice(&contents)?;
            Ok(config)
        },
        Err(e) => {
            eprintln!("Failed to open file {}: {}", file_path, e);
            Err(Box::new(e))
        }
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    let credential_config = read_config("config/credential.json").await.unwrap();

    let (okx_tx, okx_rx) = mpsc::channel::<OrderBooks>(1000);
    let (binance_tx, binance_rx) = mpsc::channel::<OrderBooks>(1000);
    //let (okx_private_tx , okx_private_rx) = mpsc::channel::<>(1000);
    let (okx_trade_tx, okx_trade_rx) = mpsc::channel::<OkxOrder>(1000);
    let pairs = vec!["BTC-USDT-SWAP".to_string()];
    let mut okx_connector = OkxMarketDataWebSocketConnector::new(pairs.clone(),);

    okx_connector.set_sender(okx_tx);

    let symbols = vec!["btcusdt".to_string()];
    let binance_connector = BinanceFuturesWebSocketConnector::new(symbols.clone());

    // Start the connector tasks

    let okx_task = tokio::spawn(async move {
        info!("Connecting to OKX WebSocket...");
        okx_connector.connect_and_subscribe().await
    });

    let binance_task = tokio::spawn(async move {
        info!("Connecting to Binance Futures WebSocket...");
        binance_connector.connect_and_subscribe().await
    });
    // 

    // Start the BoltHedger task
    sleep(Duration::from_secs(1)).await;
    let bolt_hedge_task = tokio::spawn(async move {
        let mut bolt_hedger = BoltHedger::new(okx_rx);
        bolt_hedger.start().await;
    });
    
    
    let okx_trade_task = tokio::spawn(async move {
        info!("Connecting to OKX Trade WebSocket...");
        let okx_trade_client = OkxTradeClient::new(
            credential_config["okx"]["api_key"].as_str().unwrap().to_string(),
            credential_config["okx"]["api_secret"].as_str().unwrap().to_string(),
            credential_config["okx"]["passphrase"].as_str().unwrap().to_string(),
        ).await;
        okx_trade_client.start_pinging().await;
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