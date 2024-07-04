use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use serde_json::Value;

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

        let mut previous_ts: Option<i64> = None;

        while let Some(message) = read.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    match serde_json::from_str::<Value>(&text) {
                        Ok(json_data) => {
                            println!("Parsed JSON: {}", serde_json::to_string_pretty(&json_data).unwrap_or_else(|_| "Error formatting JSON".to_string()));
                            match Self::get_ts_difference(&json_data) {
                                Some(current_ts) => {
                                    if let Some(prev_ts) = previous_ts {
                                        let diff = current_ts - prev_ts;
                                        println!("Time difference from previous message: {}ms", diff);
                                    } else {
                                        println!("First timestamp received: {}", current_ts);
                                    }
                                    previous_ts = Some(current_ts);
                                },
                                None => println!("Unable to extract timestamp"),
                            }
                        },
                        Err(e) => eprintln!("Error parsing JSON: {:?}", e),
                    }
                }
                Ok(Message::Close(..)) => break,
                Err(e) => eprintln!("Error receiving message: {:?}", e),
                _ => {}
            }
        }

        Ok(())
    }

    fn get_ts_difference(json_data: &Value) -> Option<i64> {
        let data = json_data["data"].as_array()?;
        
        // We only need the first (and likely only) item in the data array
        let item = data.first()?;
        
        // Extract the 'ts' field as a string and parse it
        let ts = item["ts"].as_str()?
            .parse::<i64>().ok()?;

        Some(ts)
    }
}