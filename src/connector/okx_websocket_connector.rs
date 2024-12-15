use std::time::Duration;
use tokio::time::{timeout, sleep};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use serde_json::Value;
use crate::model::orderbook::{OrderBookLevel, OrderBooks};
use crate::util::time_util::Utils;
use std::error::Error;
use std::fmt;
use log::{info, error, debug, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use crate::transport::redis::RedisClient;
use crate::model::liquidation_level;

const OKX_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const MAX_RECONNECT_ATTEMPTS: u32 = 5;
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

pub struct OkxMarketDataWebSocketConnector {
    redis_conn: Arc<RedisClient>,
    topic_list: Vec<String>,
    order_book: Arc<Mutex<OrderBooks>>,
    tx: Sender<OrderBooks>,
    config: ConnectorConfig,
    last_ping: Arc<Mutex<u128>>,
}

pub struct ConnectorConfig {
    reconnect_attempts: u32,
    reconnect_delay: Duration,
    timeout_duration: Duration,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        ConnectorConfig {
            reconnect_attempts: MAX_RECONNECT_ATTEMPTS,
            reconnect_delay: RECONNECT_DELAY,
            timeout_duration: Duration::from_secs(5),
        }
    }
}

#[derive(Debug)]
pub enum WebSocketError {
    Timeout,
    WebSocketError(tokio_tungstenite::tungstenite::Error),
    JsonError(serde_json::Error),
    SendError(String),
    UrlParseError(url::ParseError),
    OrderBookError(String),
    ParseError(String),
    ReconnectError,
}

impl fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WebSocketError::Timeout => write!(f, "No message received for 10 seconds"),
            WebSocketError::WebSocketError(e) => write!(f, "WebSocket error: {}", e),
            WebSocketError::JsonError(e) => write!(f, "JSON parsing error: {}", e),
            WebSocketError::SendError(e) => write!(f, "Send error: {}", e),
            WebSocketError::UrlParseError(e) => write!(f, "URL parse error: {}", e),
            WebSocketError::OrderBookError(e) => write!(f, "OrderBook error: {}", e),
            WebSocketError::ParseError(e) => write!(f, "Parse error: {}", e),
            WebSocketError::ReconnectError => write!(f, "Failed to reconnect after maximum attempts"),
        }
    }
}

impl Error for WebSocketError {}

impl From<tokio_tungstenite::tungstenite::Error> for WebSocketError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        WebSocketError::WebSocketError(err)
    }
}

impl From<serde_json::Error> for WebSocketError {
    fn from(err: serde_json::Error) -> Self {
        WebSocketError::JsonError(err)
    }
}

impl From<url::ParseError> for WebSocketError {
    fn from(err: url::ParseError) -> Self {
        WebSocketError::UrlParseError(err)
    }
}

impl From<std::num::ParseFloatError> for WebSocketError {
    fn from(err: std::num::ParseFloatError) -> Self {
        WebSocketError::ParseError(err.to_string())
    }
}

impl OkxMarketDataWebSocketConnector {
    pub fn new(redis_conn: &Arc<RedisClient>, topic_list: Vec<String>) -> Self {
        let redis_conn = redis_conn.clone();
        let order_book = Arc::new(Mutex::new(OrderBooks::new("OKX".to_string())));
        let config = ConnectorConfig::default();
        let last_ping = Arc::new(Mutex::new(Utils::get_current_timestamp_ms()));
        let (tx, _rx) = mpsc::channel::<OrderBooks>(1000);

        let connector = OkxMarketDataWebSocketConnector {
            redis_conn,
            topic_list: topic_list.clone(),
            order_book: order_book.clone(),
            tx,
            config,
            last_ping
        };

        tokio::spawn(async move {
            let mut order_book = order_book.lock().await;
            for topic in topic_list.iter() {
                order_book.register_orderbook(topic);
            }
        });

        connector
    }

    pub fn set_sender(&mut self, new_tx: Sender<OrderBooks>) {
        self.tx = new_tx;
    }

    pub async fn connect_and_subscribe(&self) -> Result<(), WebSocketError> {
        let mut attempts = 0;
        loop {
            match self.attempt_connection().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= self.config.reconnect_attempts {
                        error!("Failed to reconnect after {} attempts", attempts);
                        return Err(WebSocketError::ReconnectError);
                    }
                    warn!("Connection attempt {} failed: {}. Retrying in {:?}...", attempts, e, self.config.reconnect_delay);
                    sleep(self.config.reconnect_delay).await;
                }
            }
        }
    }


    async fn attempt_connection(&self) -> Result<(), WebSocketError> {
        let url = Url::parse(OKX_WS_URL)?;
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to ticker channels
        for ticker in self.topic_list.iter() {
            let subscribe_message = json!({
                "op": "subscribe",
                "args": [{
                    "channel": "tickers",
                    "instId": ticker
                }]
            });
            write.send(Message::Text(subscribe_message.to_string())).await?;
        }

        loop {
            if {
                let mut last_ping = self.last_ping.lock().await;
                if *last_ping + 3000 < Utils::get_current_timestamp_ms() {
                    *last_ping = Utils::get_current_timestamp_ms();
                    true
                } else {
                    false
                }
            } {
                let ping_message = json!({
                    "op": "ping"
                });
                write.send(Message::Text(ping_message.to_string())).await?;
            }
            match timeout(self.config.timeout_duration, read.next()).await {
                Ok(Some(message)) => {


                    match message? {
                        Message::Text(text) => {
                            if let Err(e) = self.process_text_message(&text).await {
                                error!("Error processing text message: {}", e);
                            }
                        },
                        Message::Binary(binary) => {
                            debug!("Received binary message: {:?}", binary);
                        },
                        Message::Ping(ping) => {
                            info!("Received ping message: {:?}", ping);
                            write.send(Message::Pong(ping)).await?;
                        },
                        Message::Pong(pong) => {
                            debug!("Received pong message: {:?}", pong);
                        },
                        Message::Frame(frame) => {
                            debug!("Received frame message: {:?}", frame);
                        },
                        Message::Close(close) => {
                            info!("Connection closed: {:?}", close);
                            return Ok(());
                        },
                    }
                },
                Ok(None) => {
                    info!("WebSocket stream ended");
                    return Ok(());
                },
                Err(_) => {
                    error!("WebSocket timeout error");
                    return Err(WebSocketError::Timeout);
                }
            }
        }
    }

    async fn process_text_message(&self, text: &str) -> Result<(), WebSocketError> {
        //ignore pong
        println!("Received message: {}", text);
        if text == "pong" {
            info!("Received pong");
            return Ok(());
        }
        let json_data: Value = serde_json::from_str(text)?;
        if let Some(ts_difference) = Self::get_ts_difference(&json_data) {
            let server_ts = Utils::get_current_timestamp_ms() as i64;
            let latency = server_ts - ts_difference;
            debug!("Latency: {} ms", latency);

            debug!("Received message: {}", json_data.to_string());
            let ticker = &json_data["data"][0];
            let topic = format!("okx_ticker:{}", ticker["instId"].as_str().unwrap());

            let bid_order = Self::parse_order(ticker, "bidPx", "bidSz")?;
            let ask_order = Self::parse_order(ticker, "askPx", "askSz")?;

            let mut order_book = self.order_book.lock().await;
            if let Some(orderbook) = order_book.get_orderbook_mut(ticker["instId"].as_str().unwrap()) {
                orderbook.set_bids_on_snapshot(vec![bid_order]);
                orderbook.set_asks_on_snapshot(vec![ask_order]);

                let updated_order_books = (*order_book).clone();
                drop(order_book);

                if let Err(e) = self.tx.send(updated_order_books).await {
                    error!("Failed to send updated OrderBooks: {}", e);
                    return Err(WebSocketError::SendError(e.to_string()));
                }
            } else {
                error!("Orderbook not found for instrument: {}", ticker["instId"].as_str().unwrap());
                return Err(WebSocketError::OrderBookError("Orderbook not found".to_string()));
            }
        }
        Ok(())
    }

    fn parse_order(ticker: &Value, price_key: &str, size_key: &str) -> Result<OrderBookLevel, WebSocketError> {
        let price = ticker[price_key].as_str()
            .ok_or_else(|| WebSocketError::ParseError("Missing price".to_string()))?
            .parse::<f64>()?;

        let amount = ticker[size_key].as_str()
            .ok_or_else(|| WebSocketError::ParseError("Missing amount".to_string()))?
            .parse::<f64>()?;

        Ok(OrderBookLevel::new(price, amount))
    }

    fn get_ts_difference(json_data: &Value) -> Option<i64> {
        json_data["data"].as_array()?.first()?.get("ts")?.as_str()?.parse::<i64>().ok()
    }

    pub fn get_order_book(&self) -> Arc<Mutex<OrderBooks>> {
        Arc::clone(&self.order_book)
    }
}