use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    broke_rs::run_broker().await
}
