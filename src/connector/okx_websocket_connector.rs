use std::time::Duration;
use tokio::time::timeout;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::tungstenite;
use url::Url;
use serde_json::Value;
use crate::model::orderbook::OrderBookLevel;
use crate::util::time_util::Utils;
use std::sync::mpsc::Sender;
//use crate::model::message::Message as BoltMessage;

const OKX_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

pub struct OkxMarketDataWebSocketConnector;

#[derive(Debug)]
pub enum WebSocketError {
    Timeout,
    WebSocketError(tokio_tungstenite::tungstenite::Error),
    JsonError(serde_json::Error),
    SendError(String),
    UrlParseError(url::ParseError),
}

impl std::error::Error for WebSocketError {}

impl std::fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebSocketError::Timeout => write!(f, "No message received for 10 seconds"),
            WebSocketError::WebSocketError(e) => write!(f, "WebSocket error: {}", e),
            WebSocketError::JsonError(e) => write!(f, "JSON parsing error: {}", e),
            WebSocketError::SendError(e) => write!(f, "Send error: {}", e),
            WebSocketError::UrlParseError(e) => write!(f, "URL parse error: {}", e),
        }
    }
}

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

impl OkxMarketDataWebSocketConnector {
    pub fn new() -> Self {
        OkxMarketDataWebSocketConnector
    }

    pub async fn connect_and_subscribe(&self, instrument_id: &Vec<String>) -> Result<(), WebSocketError> {
        let url = Url::parse(OKX_WS_URL)?;
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to ticker channel
        for ticker in instrument_id {
            let subscribe_message = json!({
                "op": "subscribe",
                "args": [{
                    "channel": "tickers",
                    "instId": ticker
                }]
            });
            write.send(Message::Text(subscribe_message.to_string())).await?;
        }

        let timeout_duration = Duration::from_secs(10);
        loop {
            match timeout(timeout_duration, read.next()).await {
                Ok(Some(message)) => {
                    match message? {
                        Message::Text(text) => {
                            let json_data: Value = serde_json::from_str(&text)?;
                            if let Some(ts_difference) = Self::get_ts_difference(&json_data) {
                                let server_ts = Utils::get_current_timestamp_ms() as i64;
                                let latency = server_ts - ts_difference;
                                //println!("Latency: {} ms", latency); //check the latency of the websocket connection
                                //let pretty_json = serde_json::to_string_pretty(&json_data["data"][0]).unwrap();
                                //println!("Received ticker:\n{}", pretty_json);
                                let ticker = &json_data["data"][0];
                                let bidOrder = match (ticker["bidPx"].as_str(), ticker["bidSz"].as_str()) {
                                    (Some(price_str), Some(amount_str)) => {
                                        let price = price_str.parse::<f64>().expect("OKX Failed to parse bid price");
                                        let amount = amount_str.parse::<f64>().expect("OKX Failed to parse bid amount");
                                        OrderBookLevel::new(price, amount)
                                    },
                                    _ => panic!("Invalid bid data"),
                                };
                                
                                let askOrder = match (ticker["askPx"].as_str(), ticker["askSz"].as_str()) {
                                    (Some(price_str), Some(amount_str)) => {
                                        let price = price_str.parse::<f64>().expect("OKX Failed to parse ask price");
                                        let amount = amount_str.parse::<f64>().expect("OKX Failed to parse ask amount");
                                        OrderBookLevel::new(price, amount)
                                    },
                                    _ => panic!("Invalid ask data"),
                                };
                                println!("Symbol: {}, Bid: {}, Ask: {}", ticker["instId"].as_str().unwrap_or("Unknown"), bidOrder.price, askOrder.price);
                            }
                        },
                        Message::Binary(binary) => {
                            println!("Received binary message: {:?}", binary);
                        },
                        Message::Ping(ping) => {
                            println!("Received ping message: {:?}", ping);
                            write.send(Message::Pong(ping)).await?;
                        },
                        Message::Pong(pong) => {
                            println!("Received pong message: {:?}", pong);
                        },
                        Message::Frame(frame) => {
                            println!("Received frame message: {:?}", frame);
                        },
                        Message::Close(close) => {
                            println!("Connection closed: {:?}", close);
                            break;
                        },
                    }
                },
                Ok(None) => {
                    println!("WebSocket stream ended");
                    break;
                },
                Err(_) => {
                    print!("OKX websocket error");
                    return Err(WebSocketError::Timeout);
                }
            }
        }

        Ok(())
    }
    fn get_ts_difference(json_data: &Value) -> Option<i64> {
        let data = json_data["data"].as_array()?;
        if data.is_empty() {
            return None;
        }

        // We only need the first (and likely only) item in the data array
        let item = data.first()?;

        // Extract the 'ts' field as a string and parse it
        let ts_str = item["ts"].as_str()?;
        ts_str.parse::<i64>().ok()
    }
}