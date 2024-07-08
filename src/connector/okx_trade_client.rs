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

#[derive(Debug, Serialize, Deserialize)]
struct SubscriptionRequest {
    op: String,
    args: Vec<String>,
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
    pub async fn new(api_key: String, api_secret: String, passphrase: String) -> Self {
        let (ws_sender, mut ws_receiver) = mpsc::channel(100);
        let okx_auth = OkxAuth::new(&api_key, &api_secret, &passphrase);
        
        tokio::spawn(async move {
            let url = "wss://ws.okx.com:8443/ws/v5/private";
            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            // Get Auth message
            let auth_message = serde_json::to_string(&okx_auth.okx_login_params()).unwrap();

            write.send(Message::Text(auth_message)).await.unwrap();

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
        });

        OkxTradeClient { ws_sender }
    }

    pub async fn subscribe(&self, channel: &str) {
        let subscription_request = SubscriptionRequest {
            op: "subscribe".to_string(),
            args: vec![channel.to_string()],
        };
        let message = serde_json::to_string(&subscription_request).unwrap();
        self.ws_sender.send(Message::Text(message)).await.unwrap();
    }

    pub async fn send(&self, message: Message) {
        self.ws_sender.send(message).await.unwrap();
    }

    pub async fn start_pinging(&self) {
        let mut interval = interval(Duration::from_secs(5));
        let ws_sender = self.ws_sender.clone();
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                println!("Sending ping");
                ws_sender.send(Message::Ping(vec![])).await.unwrap();
            }
        });
    }
}
