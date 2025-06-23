use rafka::kafka_server::KafkaServer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = KafkaServer::new("127.0.0.1:9092")?;
    server.run()?;
    Ok(())
}