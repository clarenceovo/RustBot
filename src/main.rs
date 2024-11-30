mod connector;
pub mod util;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use connector::{OkxMarketDataWebSocketConnector, BinanceFuturesWebSocketConnector};
pub mod transport;
use std::error::Error;
pub mod model;
pub mod Bolt_Hedger;
use Bolt_Hedger::bolt_hedger::BoltHedger;
use model::{okx_order::OkxOrder, okx_order_message::OrderFillData};
use tokio::sync::mpsc;
use serde_json::Value;
use std::sync::Arc;
use transport::redis::RedisClient;
use model::orderbook::OrderBooks;
use tokio::time::{sleep, Duration};
use log::{info, LevelFilter, error, log};
use env_logger::Env;
use connector::okx_trade_client::OkxTradeClient;
use util::time_util::Utils;
async fn read_config(file_path: &str) -> Result<Value, Box<dyn Error + Send + Sync>> {
    match File::open(file_path).await {
        Ok(mut file) => {
            let mut contents = Vec::new();
            file.read_to_end(&mut contents).await?;
            let config: Value = serde_json::from_slice(&contents)?;
            log::info!("Config loaded from {}: {:?}", file_path, config);
            println!("Config loaded from {}: {:?} @ {}", file_path, config, Utils::get_current_time().to_string());
            Ok(config)
        },
        Err(e) => {
            eprintln!("Failed to open file {}: {}", file_path, e);
            Err(Box::new(e) as Box<dyn Error + Send + Sync>)
        }
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
    .format_timestamp_secs()
    .init();

    // Initialize logging
    let credential_config = read_config("config/credential.json").await.unwrap();
    let redis_config = read_config("config/redis.json").await.unwrap();
    let redis_client = Arc::new(RedisClient::new(
        redis_config["host"].as_str().unwrap(),
        redis_config["port"].as_u64().unwrap() as u16,
        redis_config["password"].as_str()
    ).await?);
    /* 
    match redis_client.set("rust", "test_value").await {
        Ok(_) => println!("SET operation successful"),
        Err(e) => println!("SET operation failed: {}", e)
    } */

    let (okx_tx, okx_rx) = mpsc::channel::<OrderBooks>(1000);
    let (binance_tx, binance_rx) = mpsc::channel::<OrderBooks>(1000);
    let (okx_trade_tx, okx_trade_rx) = mpsc::channel::<OkxOrder>(1000);
    let (okx_fill_tx, okx_fill_rx) = mpsc::channel::<OrderFillData>(1000);
    let pairs = vec!["BTC-USDT-SWAP".to_string(),"BTC-USDC-SWAP".to_string()];
    let mut okx_connector = OkxMarketDataWebSocketConnector::new(&redis_client,pairs.clone());

    okx_connector.set_sender(okx_tx);

    let ticker_list = redis_client.lrange("MktData:Binance:TickerList").await;
    let ticker_list: Vec<String> = match ticker_list {
        Ok(ticker_list) => ticker_list,
        Err(e) => {
            error!("Failed to get ticker list from Redis: {:?}", e);
            return Err(Box::new(e) as Box<dyn Error + Send + Sync>);
        }
    };

    //ticker_list to symbol list
    let mut symbols = Vec::new();
    for ticker in ticker_list {
        symbols.push(format!("{}",ticker.to_lowercase()));
    }
    info!("Symbols: {:?}", symbols);
    let mut binance_connector = BinanceFuturesWebSocketConnector::new(&redis_client,symbols.clone());

    // Start the connector tasks
    let okx_task = tokio::spawn(async move {
        info!("Connecting to OKX WebSocket...");
        if let Err(e) = okx_connector.connect_and_subscribe().await {
            error!("Error in OKX connector: {:?}", e);
        }
    });
    
    let binance_task = tokio::spawn(async move {
        info!("Connecting to Binance Futures WebSocket...");
        if let Err(e) = binance_connector.connect_and_subscribe().await {
            error!("Error in Binance connector: {:?}", e);
        }
    });
    


    /*

    let okx_ping = tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(30)).await;
            info!("Sending ping to OKX WebSocket...");
            if let Err(e) = okx_connector.send_ping().await {
                error!("Error sending ping to OKX WebSocket: {:?}", e);
            }
        }
    });


    let okx_trade_task = tokio::spawn(async move {
        info!("Connecting to OKX Trade WebSocket...");
        let okx_trade_client = match OkxTradeClient::new(
            credential_config["okx"]["api_key"].as_str().unwrap().to_string(),
            credential_config["okx"]["api_secret"].as_str().unwrap().to_string(),
            credential_config["okx"]["passphrase"].as_str().unwrap().to_string(),
            okx_fill_tx
        ).await {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to create OKX Trade Client: {:?}", e);
                return;
            }
        };



        //okx_trade_client.start_pinging().await;
        sleep(Duration::from_secs(2)).await;
        let fill_topic = vec!["BTC-USDT-SWAP".to_string(),"BTC-USDC-SWAP".to_string()];
        if let Err(e) = okx_trade_client.subscribe(fill_topic).await {
            error!("Failed to subscribe to OKX Trade Client: {:?}", e);
            return;
        }
    
    });

        // Start the BoltHedger task
    sleep(Duration::from_secs(1)).await;
    let bolt_hedge_task = tokio::spawn(async move {
        let mut bolt_hedger = BoltHedger::new(okx_rx,okx_trade_tx,okx_fill_rx);
        if let Err(e) = bolt_hedger.start().await {
            error!("Error in BoltHedger: {:?}", e);
        }
    });
    */

    // Wait for all tasks to complete
    let (okx_result,binance_result) = tokio::join!(okx_task,binance_task);

    // Handle results


    Ok(())
}