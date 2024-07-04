mod connector;

use connector::OkxWebSocketConnector;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connector = OkxWebSocketConnector::new();
    connector.connect_and_subscribe("BTC-USDT-SWAP").await?;
    Ok(())
}