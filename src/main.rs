use lapin::{
    Connection, ConnectionProperties, Result,
    message::DeliveryResult,
    options::{BasicAckOptions, BasicConsumeOptions},
    types::FieldTable,
};

#[tokio::main]
async fn main() -> Result<()> {
    let address = "amqp://admin:development@localhost:5672/tol";
    let options =
        ConnectionProperties::default().with_executor(tokio_executor_trait::Tokio::current());
    let conn = Connection::connect(&address, options).await?;

    let channel = conn.create_channel().await?;
    let consumer = channel
        .basic_consume(
            "logs.traction",
            "audit-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    consumer.set_delegate(move |delivery: DeliveryResult| async move {
        let delivery = match delivery {
            Ok(Some(delivery)) => {
                // Print the message
                println!("{:?}", String::from_utf8(delivery.data.clone()));
                delivery
            }
            Ok(None) => return,
            Err(_) => {
                return;
            }
        };

        delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("Failed to ack message");
    });

    std::future::pending::<()>().await;
    Ok(())
}
