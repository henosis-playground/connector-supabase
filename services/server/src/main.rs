//! Supabase connector target configuration and SDK bootstrap.

use std::env;
use std::path::PathBuf;

use connector_sdk::RuntimeConfig;
use connector_sdk::ServeConfig;
use henosis_supabase_reconciler::ConnectorConfig;
use henosis_supabase_reconciler::SupabaseConnector;
use henosis_supabase_reconciler::journal::JournalConfig;
use henosis_supabase_reconciler::journal::OperationJournal;
use henosis_supabase_reconciler::target::Target;
use henosis_supabase_reconciler::target::TargetConfig;
use http::Uri;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    let connector = SupabaseConnector::new(
        ConnectorConfig {
            connector_build: env!("CARGO_PKG_VERSION").into(),
        },
        target,
        journal,
    );
    let core_token = env::var("HENOSIS_CORE_TOKEN")
        .ok()
        .filter(|value| !value.is_empty());
    connector_sdk::serve(
        ServeConfig {
            bind: string_env("HENOSIS_BIND", "0.0.0.0:8082"),
            core_uri: string_env("HENOSIS_CORE_URL", "http://core:8080").parse::<Uri>()?,
            core_token,
            runtime: RuntimeConfig::new(path_env(
                "HENOSIS_STATE_DIR",
                "/var/lib/henosis-connector-supabase/state-sdk-v1",
            )),
            telemetry_filter: "henosis=info,connector_sdk=info".into(),
        },
        connector,
    )
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
