use std::env;
use tokio::net::TcpListener;
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let port = env::var("PORT")
        .expect("Missing required environment variable: PORT")
        .parse::<u16>()
        .expect("Invalid value for environment variable: PORT");

    let addr = format!("0.0.0.0:{}", port);
    
    info!("Starting doc-uni-proc on {}", addr);

    let listener = TcpListener::bind(&addr).await.unwrap_or_else(|e| {
        error!("Failed to bind to {}: {}", addr, e);
        std::process::exit(1);
    });

    info!("Listening on {}", addr);
    
    // Simple placeholder for the actual server logic
    // This would normally include the actual HTTP server setup
    println!("doc-uni-proc is running on {}", addr);
    
    // Keep the process alive
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}