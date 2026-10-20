#[tokio::main]
async fn main() {
    if let Err(error) = saas_notification_ws::run_from_env().await {
        eprintln!("notification service failed: {error}");
        std::process::exit(1);
    }
}
