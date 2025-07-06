use rafka::network::server::KafkaServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = KafkaServer::new("127.0.0.1:9092")?;
    server.run().await?;
    Ok(())
}