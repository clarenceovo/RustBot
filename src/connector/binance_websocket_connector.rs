use std::time::Duration;
use tokio::time::timeout;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::tungstenite;
use url::Url;
use serde_json::Value;
use crate::util::time_util::Utils;

const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/ws";

pub struct BinanceFuturesWebSocketConnector;

#[derive(Debug)]
pub enum WebSocketError {
    Timeout,
    WebSocketError(tokio_tungstenite::tungstenite::Error),
    JsonError(serde_json::Error),
    UrlParseError(url::ParseError),
}

impl std::error::Error for WebSocketError {}

impl std::fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebSocketError::Timeout => write!(f, "No message received for 10 seconds"),
            WebSocketError::WebSocketError(e) => write!(f, "WebSocket error: {}", e),
            WebSocketError::JsonError(e) => write!(f, "JSON parsing error: {}", e),
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

impl BinanceFuturesWebSocketConnector {
    pub fn new() -> Self {
        BinanceFuturesWebSocketConnector
    }

    pub async fn connect_and_subscribe(&self, symbols: &Vec<String>) -> Result<(), WebSocketError> {
        let url = Url::parse(BINANCE_FUTURES_WS_URL)?;
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Create subscription message
        let streams: Vec<String> = symbols.iter().map(|symbol| format!("{}@bookTicker", symbol.to_lowercase())).collect();
        let subscribe_message = json!({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        });

        // Send subscription message
        write.send(Message::Text(subscribe_message.to_string())).await?;

        let timeout_duration = Duration::from_secs(10);
        loop {
            match timeout(timeout_duration, read.next()).await {
                Ok(Some(message)) => {
                    match message? {
                        Message::Text(text) => {
                            let json_data: Value = serde_json::from_str(&text)?;
                            if let Some(event_time) = Self::get_event_time(&json_data) {
                                let server_ts = Utils::get_current_timestamp_ms() as i64;
                                let latency = server_ts - event_time;
                                //println!("Symbol: {}, Latency: {} ms", json_data["s"].as_str().unwrap_or("Unknown"), latency);
                                
                                // Here you can process the ticker data as needed
                                println!("Received ticker: {:?}", json_data);
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
                    print!("Binance websocket error");
                    return Err(WebSocketError::Timeout);
                }
            }
        }

        Ok(())
    }

    fn get_event_time(json_data: &Value) -> Option<i64> {
        json_data["E"].as_i64()
    }
}