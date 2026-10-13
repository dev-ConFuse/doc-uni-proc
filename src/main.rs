use std::net::SocketAddr;

use tokio::net::TcpListener;
use tracing::{error, info};
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

    // ── Config + processor ──────────────────────────────────────────────────
    // init_processor loads .map.env → .secret.env → .env.local and fails fast
    // on missing config (see .cline/rules/env-policy.md).
    let processor = match unified_processor_lib::init_processor().await {
        Ok(processor) => processor,
        Err(e) => {
            error!("Startup failed: {e}");
            std::process::exit(1);
        }
    };
    let config = unified_processor_lib::get_config()
        .expect("config is set when init_processor succeeds")
        .clone();

    let addr = SocketAddr::from(([0, 0, 0, 0], config.server.port));
    info!("Starting doc-uni-proc on {}", addr);

    // ── Step 1: Bind TCP port FIRST ─────────────────────────────────────────
    // Binding here makes the container reachable immediately even while the
    // background consumer initializes.
    let listener = TcpListener::bind(addr).await.unwrap_or_else(|e| {
        error!("Failed to bind to {}: {}", addr, e);
        std::process::exit(1);
    });
    info!(bound_addr = %addr, "TCP listener bound — service accepting connections");

    // ── Step 2: Kafka consumer (background) ─────────────────────────────────
    let consumer_processor = processor.clone();
    tokio::spawn(async move {
        info!("Initializing Kafka event consumer...");
        let consumer =
            unified_processor_lib::graph::consumer::UnifiedEventConsumer::new(consumer_processor);
        if let Err(e) = consumer.start().await {
            error!("Kafka consumer failed to start: {}", e);
        }
    });

    // ── Step 3: Router + middleware ─────────────────────────────────────────
    let auth_layer = unified_processor_lib::infra::middleware::AxumAuthLayer::new(
        config.server.auth_middleware_url.clone(),
    );
    let rate_limit =
        unified_processor_lib::infra::middleware::AxumRateLimitConfig::default_for_service(10000);
    let app =
        unified_processor_lib::core::routes::build_app_router(processor, auth_layer, rate_limit);

    // ── Step 4: Serve ───────────────────────────────────────────────────────
    info!(addr = %addr, "Serving");
    axum::serve(listener, app).await.expect("HTTP server error");
}