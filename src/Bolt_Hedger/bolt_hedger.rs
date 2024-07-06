use serde_json::json;
use tracing::{info, error, span, Level};
use tracing_subscriber;
use crate::connector::OkxWebSocketConnector;
use std::sync::mpsc::{channel, Receiver, Sender};
use transport::redis::RedisClient;

pub struct BoltHedger{
    connector: OkxWebSocketConnector,
    message_rx: Receiver<Message>,

}

impl BoltHedger{
    pub fn new() -> Self {
        BoltHedger
    }

    pub fn start(&self) {
        let span = span!(Level::INFO, "bolt_hedger");
        let _enter = span.enter();
        info!("Starting Bolt Hedger");
        let message = json!({
            "message": "Bolt Hedger started"
        });
        info!("{}", message);
    }

    pub fn stop(&self) {
        let span = span!(Level::INFO, "bolt_hedger");
        let _enter = span.enter();
        info!("Stopping Bolt Hedger");
        let message = json!({
            "message": "Bolt Hedger stopped"
        });
        info!("{}", message);
    }


}