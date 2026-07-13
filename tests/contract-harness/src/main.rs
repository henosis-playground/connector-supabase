//! Live core-to-connector-to-Supabase proof driver.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use buffa::MessageField;
use connectrpc::client::ClientConfig;
use connectrpc::client::HttpClient;
use henosis_proto::connect::henosis::v1::GraphServiceClient;
use henosis_proto::proto::henosis::v1::ComponentDispositionKind;
use henosis_proto::proto::henosis::v1::ComponentSpec;
use henosis_proto::proto::henosis::v1::CreateGraphRequest;
use henosis_proto::proto::henosis::v1::GetGraphRequest;
use henosis_proto::proto::henosis::v1::GraphState;
use henosis_proto::proto::henosis::v1::RegisterComponentSpecRequest;
use henosis_supabase_authoring::derive_component;
use henosis_supabase_reconciler::CONNECTOR_NAME;
use http::Uri;
use serde::Serialize;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Mode {
    Ready,
    PlanFailure,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Evidence {
    mode: Mode,
    graph_id: String,
    component_spec_hash: String,
    source_repository: String,
    schema: String,
    terminal_state: GraphState,
    target_assertion: String,
    api_assertion: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mode = match env::var("HENOSIS_PROOF_MODE").as_deref() {
        Ok("plan-failure") => Mode::PlanFailure,
        Ok("ready") | Err(_) => Mode::Ready,
        Ok(value) => return Err(format!("unknown HENOSIS_PROOF_MODE {value:?}").into()),
    };
    let core_uri = env::var("HENOSIS_CORE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:4481".into())
        .parse::<Uri>()?;
    let token = env::var("HENOSIS_CORE_TOKEN").unwrap_or_else(|_| "core-dev-token".into());
    let config =
        ClientConfig::new(core_uri).with_default_header("authorization", format!("Bearer {token}"));
    let client = GraphServiceClient::new(HttpClient::plaintext(), config);

    let source_repository = env::var("HENOSIS_PROOF_REPO")
        .unwrap_or_else(|_| "/home/null/Work/henosis/repos/service-d".into());
    let derived = derive_component(&source_repository)?;
    let schema = derived.connector_context.target.schema.clone();
    let migration_id = derived
        .connector_context
        .migrations
        .first()
        .ok_or("proof repository has no native migration")?
        .id
        .clone();
    let outputs_schema = derived.outputs_schema_bytes()?;
    let connector_context = derived.connector_context_bytes()?;
    let resumed_graph = env::var("HENOSIS_PROOF_GRAPH_ID").ok();
    let graph_id = resumed_graph
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()?
        .unwrap_or_else(Uuid::now_v7);
    let spec = ComponentSpec {
        name: Some(derived.name),
        connector: Some(derived.connector),
        outputs_schema: Some(outputs_schema),
        depends_on: derived.depends_on,
        connector_context: Some(connector_context),
        ..Default::default()
    };
    let registered = client
        .register_component_spec(RegisterComponentSpecRequest {
            spec: MessageField::some(spec.clone()),
            ..Default::default()
        })
        .await?
        .into_owned()
        .component
        .into_option()
        .ok_or("core omitted registered component")?;
    if registered.spec.into_option().as_ref() != Some(&spec) {
        return Err("core returned a different immutable component spec".into());
    }
    let hash = registered.hash.ok_or("core omitted component spec hash")?;
    if resumed_graph.is_none() {
        client
            .create_graph(CreateGraphRequest {
                graph_id: Some(graph_id.as_bytes().to_vec()),
                component_spec_hashes: vec![hash.clone()],
                request_id: Some(Uuid::now_v7().as_bytes().to_vec()),
                ..Default::default()
            })
            .await?;
    }

    let terminal_state = tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            let response = client
                .get_graph(GetGraphRequest {
                    graph_id: Some(graph_id.as_bytes().to_vec()),
                    ..Default::default()
                })
                .await?
                .into_owned();
            let state = response
                .state
                .into_option()
                .ok_or("core omitted graph state")?;
            if let Some(report) = state.reports.iter().find(|report| {
                report.connector.as_deref() == Some(CONNECTOR_NAME)
                    && report.generation == Some(1)
                    && !report.dispositions.is_empty()
            }) {
                let expected = match mode {
                    Mode::Ready => ComponentDispositionKind::Ready,
                    Mode::PlanFailure => ComponentDispositionKind::Failed,
                };
                if report.dispositions.iter().all(|disposition| {
                    disposition
                        .kind
                        .as_ref()
                        .is_some_and(|kind| kind.to_i32() == expected as i32)
                }) {
                    assert_terminal(mode, report)?;
                    break Ok::<GraphState, Box<dyn std::error::Error + Send + Sync>>(state);
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await??;

    let (db_client, connection) = postgres().await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let schema_exists: bool = db_client
        .query_one(
            "select exists(select 1 from pg_namespace where nspname = $1)",
            &[&schema],
        )
        .await?
        .get(0);
    let (target_assertion, api_assertion) = match mode {
        Mode::PlanFailure => {
            if schema_exists {
                return Err(
                    "misconfigured component changed the target before plan failure".into(),
                );
            }
            (
                "owned schema is absent; no target mutation occurred".into(),
                None,
            )
        }
        Mode::Ready => {
            if !schema_exists {
                return Err("READY target schema does not exist".into());
            }
            let receipt: bool = db_client
                .query_one(
                    "select exists(select 1 from henosis_connector.migration_receipts where \
                     schema_name = $1 and migration_id = $2)",
                    &[&schema, &migration_id],
                )
                .await?
                .get(0);
            if !receipt {
                return Err("READY migration receipt does not exist".into());
            }
            let api = verify_api(&schema).await?;
            (
                "schema, table, and immutable migration receipt observed".into(),
                Some(api),
            )
        }
    };

    let evidence_path = PathBuf::from(
        env::var("HENOSIS_EVIDENCE_PATH")
            .unwrap_or_else(|_| "/tmp/henosis-supabase-proof.json".into()),
    );
    if let Some(parent) = evidence_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        evidence_path,
        serde_json::to_vec_pretty(&Evidence {
            mode,
            graph_id: graph_id.to_string(),
            component_spec_hash: hex::encode(hash),
            source_repository,
            schema,
            terminal_state,
            target_assertion,
            api_assertion,
        })?,
    )?;
    Ok(())
}

fn assert_terminal(
    mode: Mode,
    report: &henosis_proto::proto::henosis::v1::SliceReport,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match mode {
        Mode::Ready => {
            if report.outputs.len() != 1 || !report.diagnostics.is_empty() {
                return Err("READY report is not an atomic one-output clean result".into());
            }
            let values = serde_json::from_slice::<serde_json::Value>(
                report.outputs[0].values_json.as_deref().unwrap_or_default(),
            )?;
            if values
                .get("databaseUrlRef")
                .and_then(|value| value.as_str())
                != Some("docker-secret://supabase-connection-url")
                || values.get("anonKeyRef").and_then(|value| value.as_str())
                    != Some("docker-secret://supabase-anon-key")
            {
                return Err("READY outputs did not contain exact secret references".into());
            }
        }
        Mode::PlanFailure => {
            let diagnostic = report
                .diagnostics
                .iter()
                .find(|diagnostic| {
                    diagnostic.code.as_deref() == Some("supabase.plan.destructive-unsupported")
                })
                .ok_or("plan failure omitted native destructive-SQL diagnostic")?;
            if diagnostic.pointer.as_deref() != Some("/migrations/0/sql")
                || diagnostic.help.as_deref().unwrap_or_default().is_empty()
                || diagnostic.message.as_deref().unwrap_or_default().is_empty()
                || !report.outputs.is_empty()
            {
                return Err(
                    "plan failure diagnostic lost pointer/help/detail or published outputs".into(),
                );
            }
        }
    }
    Ok(())
}

async fn postgres() -> Result<
    (
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let password_file = env::var("HENOSIS_SUPABASE_PASSWORD_FILE").unwrap_or_else(|_| {
        "/home/null/Work/henosis/infra/secrets/supabase-postgres-password.txt".into()
    });
    let password = fs::read_to_string(password_file)?;
    let mut config = tokio_postgres::Config::new();
    config
        .host(env::var("HENOSIS_SUPABASE_HOST").unwrap_or_else(|_| "127.0.0.1".into()))
        .port(
            env::var("HENOSIS_SUPABASE_PORT")
                .unwrap_or_else(|_| "4485".into())
                .parse()?,
        )
        .user("postgres")
        .password(password.trim())
        .dbname("postgres");
    Ok(config.connect(NoTls).await?)
}

async fn verify_api(schema: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let api_url =
        env::var("HENOSIS_SUPABASE_API_URL").unwrap_or_else(|_| "http://127.0.0.1:4484".into());
    let anon_file = env::var("HENOSIS_SUPABASE_ANON_KEY_FILE")
        .unwrap_or_else(|_| "/home/null/Work/henosis/infra/secrets/supabase-anon-key.txt".into());
    let anon = fs::read_to_string(anon_file)?;
    let client = reqwest::Client::new();
    for _ in 0..30 {
        let response = client
            .get(format!("{api_url}/rest/v1/items?select=id,name"))
            .header("apikey", anon.trim())
            .header("Authorization", format!("Bearer {}", anon.trim()))
            .header("Accept-Profile", schema)
            .send()
            .await?;
        if response.status().is_success() {
            let body = response.text().await?;
            return Ok(format!("PostgREST returned 200 with body {body}"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err("PostgREST did not expose the reconciled schema within 30 seconds".into())
}
