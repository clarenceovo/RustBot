use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::interval;
use crate::util::okx_auth::OkxAuth;

#[derive(Debug, Serialize, Deserialize)]
struct AuthRequest {
    op: String,
    args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderChannel {
    pub channel: String,
    pub inst_type: String,
    pub inst_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SubscriptionRequest {
    op: String,
    args: Vec<OrderChannel>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OkxMessage {
    event: String,
    channel: String,
    data: serde_json::Value,
}

pub struct OkxTradeClient {
    ws_sender: mpsc::Sender<Message>,
}

impl OkxTradeClient {
    pub async fn new(api_key: String, api_secret: String, passphrase: String) -> Result<Self, Box<dyn std::error::Error>> {
        let (ws_sender, mut ws_receiver) = mpsc::channel(100);
        let okx_auth = OkxAuth::new(&api_key, &api_secret, &passphrase);
        
        tokio::spawn(async move {
            let url = "wss://ws.okx.com:8443/ws/v5/private";
            match connect_async(url).await {
                Ok((ws_stream, _)) => {
                    let (mut write, mut read) = ws_stream.split();

                    // Get Auth message
                    let auth_message = serde_json::to_string(&okx_auth.okx_login_params()).unwrap();

                    if let Err(e) = write.send(Message::Text(auth_message)).await {
                        eprintln!("Failed to send auth message: {}", e);
                        return;
                    }

                    // Handle incoming messages
                    while let Some(message) = read.next().await {
                        match message {
                            Ok(msg) => match msg {
                                Message::Text(text) => {
                                    println!("Received: {}", text);
                                }
                                Message::Binary(bin) => {
                                    println!("Received binary: {:?}", bin);
                                }
                                _ => {}
                            },
                            Err(e) => {
                                eprintln!("WebSocket error: {}", e);
                                break;
                            }
                        }
                    }
                },
                Err(e) => {
                    eprintln!("Failed to connect: {}", e);
                }
            }
        });

        Ok(OkxTradeClient { ws_sender })
    }

    pub async fn subscribe(&self, channel: &str) -> Result<(), mpsc::error::SendError<Message>> {
        let channel = OrderChannel {
            channel: "orders".to_string(),
            inst_type: "SWAP".to_string(),
            inst_id: channel.to_string(),
        };
        let subscription_request = SubscriptionRequest {
            op: "subscribe".to_string(),
            args: vec![channel],
        };
        let message = serde_json::to_string(&subscription_request).map_err(|e| {
            mpsc::error::SendError(Message::Text(format!("Serialization error: {}", e)))
        })?;
        println!("Sending Subscription: {}", message);
        self.ws_sender.send(Message::Text(message)).await
    }

    pub async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        self.ws_sender.send(message).await
    }

    pub async fn start_pinging(&self) {
        let mut interval = interval(Duration::from_secs(1));
        let ws_sender = self.ws_sender.clone();
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                match ws_sender.send(Message::Text("ping".to_string())).await {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("Failed to send ping: {}. Stopping ping service.", e);
                        break;
                    }
                }
            }
        });
    }
}