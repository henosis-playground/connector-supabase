//! Supabase connector service process.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::routing::get;
use connectrpc::Router;
use henosis_supabase_reconciler::ConnectorHandler;
use henosis_supabase_reconciler::journal::JournalConfig;
use henosis_supabase_reconciler::journal::OperationJournal;
use henosis_supabase_reconciler::reconciler::CoreReporter;
use henosis_supabase_reconciler::reconciler::Reconciler;
use henosis_supabase_reconciler::reconciler::ReconcilerConfig;
use henosis_supabase_reconciler::target::Target;
use henosis_supabase_reconciler::target::TargetConfig;
use http::Uri;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("henosis=info")),
        )
        .with_span_events(FmtSpan::CLOSE)
        .try_init()?;

    let state_dir = path_env(
        "HENOSIS_STATE_DIR",
        "/var/lib/henosis-connector-supabase/state",
    );
    let target = Target::new(TargetConfig {
        host: string_env("HENOSIS_SUPABASE_HOST", "supabase-db"),
        port: string_env("HENOSIS_SUPABASE_PORT", "5432").parse()?,
        user: string_env("HENOSIS_SUPABASE_USER", "postgres"),
        database: string_env("HENOSIS_SUPABASE_DATABASE", "postgres"),
        password_file: path_env(
            "HENOSIS_SUPABASE_PASSWORD_FILE",
            "/run/secrets/supabase-postgres-password",
        ),
        api_url: string_env("HENOSIS_SUPABASE_API_URL", "http://127.0.0.1:4484"),
        database_url_ref: string_env(
            "HENOSIS_SUPABASE_DATABASE_URL_REF",
            "docker-secret://supabase-connection-url",
        ),
        anon_key_ref: string_env(
            "HENOSIS_SUPABASE_ANON_KEY_REF",
            "docker-secret://supabase-anon-key",
        ),
    });
    let journal = OperationJournal::connect(&JournalConfig {
        access_token: required("S2_ACCESS_TOKEN")?,
        account_endpoint: required("S2_ACCOUNT_ENDPOINT")?,
        basin_endpoint: required("S2_BASIN_ENDPOINT")?,
        basin: required("S2_BASIN")?,
        stream: string_env(
            "HENOSIS_SUPABASE_JOURNAL_STREAM",
            "connector-supabase-local-v1",
        ),
    })?;
    let core_uri = string_env("HENOSIS_CORE_URL", "http://core:8080").parse::<Uri>()?;
    let core_token = env::var("HENOSIS_CORE_TOKEN")
        .ok()
        .filter(|value| !value.is_empty());
    let reporter = Arc::new(CoreReporter::new(core_uri, core_token));
    let reconciler = Arc::new(Reconciler::new(
        ReconcilerConfig {
            state_dir,
            connector_build: env!("CARGO_PKG_VERSION").into(),
        },
        target,
        journal,
        reporter,
    )?);
    reconciler.resume().await?;

    let connect = Router::new().add_service(Arc::new(ConnectorHandler::new(reconciler)));
    let router = axum::Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .fallback_service(connect.into_axum_service());
    let listener =
        tokio::net::TcpListener::bind(string_env("HENOSIS_BIND", "0.0.0.0:8082")).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

fn string_env(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}

fn path_env(name: &str, default: &str) -> PathBuf {
    PathBuf::from(string_env(name, default))
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    env::var(name).map_err(|_| format!("required environment variable {name} is missing").into())
}
