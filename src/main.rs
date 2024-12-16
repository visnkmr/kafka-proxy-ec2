use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use futures::stream::StreamExt;
use kafka::producer::{Producer, Record, RequiredAcks};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    data: String,
}

async fn send_to_kafka(messages: Vec<Message>) -> Result<(), kafka::error::Error> {
    let mut producer = Producer::from_hosts(vec!("localhost:9092".to_owned()))
        .with_ack_timeout(Duration::from_secs(5))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    for message in messages {
        let record = Record::from_key_value("my-topic", "key", serde_json::to_string(&message).unwrap());
        producer.send(&record)?;
    }

    Ok(())
}

#[post("/send")]
async fn handle_request(
    body: web::Json<Vec<Message>>,
    tx: web::Data<mpsc::Sender<Vec<Message>>>,
) -> impl Responder {
    let messages = body.into_inner();

    if let Err(_) = tx.send(messages).await {
        return HttpResponse::InternalServerError().body("Failed to send messages to buffer");
    }

    HttpResponse::Ok().body("Messages buffered for Kafka")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let (tx, mut rx) = mpsc::channel::<Vec<Message>>(1000); // Channel with buffer size 1000

    tokio::spawn(async move {
        while let Some(messages) = rx.recv().await {
            if let Err(e) = send_to_kafka(messages).await {
                eprintln!("Failed to send to Kafka: {}", e);
                // Implement more sophisticated error handling here (e.g., retries, dead-letter queue)
            }
        }
    });

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(tx.clone())) // Share the sender
            .service(handle_request)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}