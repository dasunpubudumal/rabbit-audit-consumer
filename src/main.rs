use clap::Parser;
use lapin::{
    Connection, ConnectionProperties, Result,
    message::DeliveryResult,
    options::{BasicAckOptions, BasicConsumeOptions},
    types::FieldTable,
};

#[derive(Parser)]
#[command(version, about, long_about=None)]
pub struct Cli {
    #[arg(long)]
    pub host: String,
    #[arg(long)]
    pub port: String,
    #[arg(long)]
    pub vhost: String,
    #[arg(long)]
    pub username: String,
    #[arg(long)]
    pub password: String,
    #[arg(long)]
    pub queue_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Read from CLI
    let username = cli.username;
    let password = cli.password;
    let host = cli.host;
    let port = cli.port;
    let vhost = cli.vhost;
    let queue_name = cli.queue_name;

    let address = format!("amqp://{username}:{password}@{host}:{port}/{vhost}");
    let options =
        ConnectionProperties::default().with_executor(tokio_executor_trait::Tokio::current());
    let conn = Connection::connect(&address, options).await?;

    let channel = conn.create_channel().await?;
    let consumer = channel
        .basic_consume(
            &queue_name,
            "psd-rabbitmq-audit-consumer",
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
