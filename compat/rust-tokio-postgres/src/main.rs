#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "host=127.0.0.1 port=58432 user=postgres password=postgres dbname=pgkinetic".to_string()
    });

    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("connection error: {error}");
        }
    });

    let statement = client
        .prepare("select balance_cents from accounts where email = $1")
        .await?;
    let row = client
        .query_one(&statement, &[&"alice@example.com"])
        .await?;
    let balance: i64 = row.get(0);

    if balance != 1000 {
        return Err(format!("expected balance 1000, got {balance}").into());
    }

    println!("rust tokio-postgres smoke passed");
    Ok(())
}
