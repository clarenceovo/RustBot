use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::interval;
use serde_json::Value;
use crate::model::okx_order::OkxOrder;
use crate::util::okx_auth::OkxAuth;
use crate::model::okx_order_message::{OkxOrderMessage, OrderFillData};
use crate::model::okx_position::OKXPositionData;
use crate::util::time_util;
use std::sync::Arc;
use std::error::Error;

#[derive(Debug, Serialize, Deserialize)]
struct AuthRequest {
    op: String,
    args: Vec<OrderChannel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderChannel {
    pub channel: String,
    pub instType: String,
    pub instId: String,

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

#[derive(Clone)]
pub struct OkxTradeClient {
    ws_sender: Sender<Message>,
    fill_sender: Sender<OrderFillData>,
}

impl OkxTradeClient {
    pub async fn new(api_key: String, api_secret: String, passphrase: String, fill_sender: Sender<OrderFillData>) -> Result<Self, Box<dyn std::error::Error>> {
        let okx_auth = OkxAuth::new(&api_key, &api_secret, &passphrase);

        let (ws_sender, mut ws_receiver) = tokio::sync::mpsc::channel(100);

        let url = "wss://wsaws.okx.com:8443/ws/v5/private";
        let (ws_stream, _) = connect_async(url).await?;
        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));
        
        let client = OkxTradeClient { ws_sender: ws_sender.clone(), fill_sender };

        // Handle WebSocket connection
        let write_clone = write.clone();
        let client_clone = client.clone();
        tokio::spawn(async move {
            // Authentication
            let auth_message = serde_json::to_string(&okx_auth.okx_login_params()).unwrap();
            if let Err(e) = write_clone.lock().await.send(Message::Text(auth_message)).await {
                eprintln!("Failed to send auth message: {}", e);
                return;
            }

            // Handle incoming messages
            while let Some(message) = read.next().await {
                match message {
                    Ok(msg) => match msg {
                        Message::Text(text) => {
                            if let Err(e) = client_clone.handle_message(&text).await {
                                eprintln!("Failed to handle message: {}", e);
                                break;
                            }
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

        let write_clone = write.clone();
        // Send message from TX RX channel
        tokio::spawn(async move {
            while let Some(message) = ws_receiver.recv().await {
                if let Err(e) = write_clone.lock().await.send(message).await {
                    eprintln!("Failed to send message: {}", e);
                    // ProcessUtils::kill_process();
                    break;
                }
            }
        });

        Ok(client)
    }

    async fn handle_message(&self, message: &str) -> Result<(), Box<dyn Error>> {
        let json_data: Value = serde_json::from_str(message)?;
        //println!("Received message: {}", json_data.to_string());
        if let Some(event) = json_data.get("event") {
            if event == "login" {
                println!("OKX Login successful");
                return Ok(());
            }
        }

        if json_data.get("data").is_some() {
            let data = json_data.get("data").unwrap();
            /* 
            match serde_json::from_str::<OKXPositionData>(json_data.get("data")) {
                Ok(okx_message) => {
                    print!("Received order message: {:?}", okx_message);
                }
                Err(e) => {
                    eprintln!("Failed to deserialize message: {}", e);
                    return Err(e.into());
                }
            }
         */   
        }
        
        Ok(())
    }


    pub async fn subscribe(&self, channels: Vec<String>) -> Result<(), tokio::sync::mpsc::error::SendError<Message>> {

        let mut payload = Vec::new();
        
        
        for topic in channels.iter() {
            let channel = OrderChannel {
                channel: "positions".to_string(),
                instType: "SWAP".to_string(),
                instId: topic.to_string(), // Ensure this field is included
            };
            payload.push(channel);
        } 
        
        let subscription_request = SubscriptionRequest {
            op: "subscribe".to_string(),
            args: payload,
        };
        
        let message = serde_json::to_string(&subscription_request).map_err(|e| {
            tokio::sync::mpsc::error::SendError(Message::Text(format!("Serialization error: {}", e)))
        })?;
        println!("Sending subscription message: {}", message);

        self.send(Message::Text(message)).await
    }

    pub async fn send(&self, message: Message) -> Result<(), tokio::sync::mpsc::error::SendError<Message>> {
        self.ws_sender.send(message).await
    }

    pub async fn start_pinging(&self) {
        let mut interval = interval(Duration::from_secs(1));
        let ws_sender = self.ws_sender.clone();
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                match ws_sender.send(Message::Text("ping".to_string())).await {
                    Ok(_) => { println!("Sent ping"); }
                    Err(e) => {
                        eprintln!("Failed to send ping: {}. Stopping ping service.", e);
                        break;
                    }
                }
            }
        });
    }
}