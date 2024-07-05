use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use serde_json::Value;
use crate::util::time_util::Utils;

const OKX_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

pub struct OkxWebSocketConnector;

impl OkxWebSocketConnector {
    pub fn new() -> Self {
        OkxWebSocketConnector
    }

    pub async fn connect_and_subscribe(&self, instrument_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let url = Url::parse(OKX_WS_URL)?;
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to ticker channel
        let subscribe_message = json!({
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": instrument_id
            }]
        });

        write.send(Message::Text(subscribe_message.to_string())).await?;

        while let Some(message) = read.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    match serde_json::from_str::<Value>(&text) {
                        Ok(json_data) => {
                            println!("Parsed JSON: {}", serde_json::to_string_pretty(&json_data).unwrap_or_else(|_| "Error formatting JSON".to_string()));
                            match Self::get_ts_difference(&json_data) {
                                Some(ts) => {
                                    let current_ts = Utils::get_current_timestamp_ms() as i64;
                                    let ts_diff =  ts - current_ts;
                                    println!("Timestamp difference: {} milliseconds", ts_diff);
                                },
                                None => println!("Unable to extract timestamp"),
                            }
                        },
                        Err(e) => eprintln!("Error parsing JSON: {:?}", e),
                    }
                },
                Ok(Message::Binary(binary)) => {
                    println!("Received binary message: {:?}", binary);
                },
                Ok(Message::Ping(ping)) => {
                    println!("Received ping message: {:?}", ping);
                    write.send(Message::Pong(ping)).await?;
                },
                Ok(Message::Pong(pong)) => {
                    println!("Received pong message: {:?}", pong);
                },
                Ok(Message::Frame(frame)) => {
                    println!("Received frame message: {:?}", frame);
                },
                Ok(Message::Close(close)) => {
                    println!("Connection closed: {:?}", close);
                    break;
                },
                Err(e) => {
                    eprintln!("Error reading message: {:?}", e);
                },
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