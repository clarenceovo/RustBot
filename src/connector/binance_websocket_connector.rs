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

const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/ws";
const MAX_RECONNECT_ATTEMPTS: u32 = 5;
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

pub struct BinanceFuturesWebSocketConnector {
    topic_list: Vec<String>,
    order_book: Arc<Mutex<OrderBooks>>,
    tx: Sender<OrderBooks>,
    config: ConnectorConfig,
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
            timeout_duration: Duration::from_secs(10),
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

impl BinanceFuturesWebSocketConnector {
    pub fn new(topic_list: Vec<String>) -> Self {
        let order_book = Arc::new(Mutex::new(OrderBooks::new("BinanceFutures".to_string())));
        let config = ConnectorConfig::default();
        
        // Dummy channel
        let (tx, _rx) = mpsc::channel::<OrderBooks>(1);
        
        let connector = BinanceFuturesWebSocketConnector { 
            topic_list: topic_list.clone(),
            order_book: order_book.clone(),
            tx,
            config,
        };
        
        tokio::spawn(async move {
            let mut order_book = order_book.lock().await;
            for topic in topic_list.iter() {
                order_book.register_orderbook(topic.to_uppercase().as_str());
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
        let url = Url::parse(BINANCE_FUTURES_WS_URL)?;
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to ticker channels
        let subscribe_message = json!({
            "method": "SUBSCRIBE",
            "params": self.topic_list.iter().map(|symbol| format!("{}@bookTicker", symbol.to_lowercase())).collect::<Vec<String>>(),
            "id": 1
        });
        write.send(Message::Text(subscribe_message.to_string())).await?;

        loop {
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
                            debug!("Received ping message: {:?}", ping);
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
        let json_data: Value = serde_json::from_str(text)?;
        if json_data["e"].as_str() == Some("bookTicker") {
            let server_ts = Utils::get_current_timestamp_ms() as i64;
            let event_ts = json_data["E"].as_i64().ok_or(WebSocketError::ParseError("Missing event time".to_string()))?;
            let latency = server_ts - event_ts;
            debug!("Latency: {} ms", latency);

            let bid_order = Self::parse_order(&json_data, "b", "B")?;
            let ask_order = Self::parse_order(&json_data, "a", "A")?;
            println!("Symbol: {}, Bid: {}, Ask: {} | BSize {} , ASize {} @ {} | Latency {}", json_data["s"].as_str().unwrap_or("Unknown"), bid_order.price, ask_order.price,bid_order.quantity , ask_order.quantity,Utils::get_current_time()
            ,latency);
            let mut order_book = self.order_book.lock().await;

            if let Some(orderbook) = order_book.get_orderbook_mut(json_data["s"].as_str().unwrap()) {
                orderbook.set_bids_on_snapshot(vec![bid_order]);
                orderbook.set_asks_on_snapshot(vec![ask_order]);
                let updated_order_books = (*order_book).clone();

                drop(order_book);
    
                if let Err(e) = self.tx.send(updated_order_books).await {
                    error!("Failed to send updated OrderBooks: {}", e);
                    return Err(WebSocketError::SendError(e.to_string()));
                }
            } else {
                error!("Orderbook not found for instrument: {}", json_data["s"].as_str().unwrap());
                return Err(WebSocketError::OrderBookError("Orderbook not found".to_string()));
            }
        }
        Ok(())
    }

    fn parse_order(json_data: &Value, price_key: &str, size_key: &str) -> Result<OrderBookLevel, WebSocketError> {
        let price = json_data[price_key].as_str()
            .ok_or_else(|| WebSocketError::ParseError("Missing price".to_string()))?
            .parse::<f64>()?;

        let amount = json_data[size_key].as_str()
            .ok_or_else(|| WebSocketError::ParseError("Missing amount".to_string()))?
            .parse::<f64>()?;

        Ok(OrderBookLevel::new(price, amount))
    }
    
    pub fn get_order_book(&self) -> Arc<Mutex<OrderBooks>> {
        Arc::clone(&self.order_book)
    }
}