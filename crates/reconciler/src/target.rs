//! Native PostgreSQL/PostgREST observation and one-operation application.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use tokio_postgres::Client;
use tokio_postgres::GenericClient;
use tokio_postgres::IsolationLevel;
use tokio_postgres::NoTls;

use crate::context::AnonAccess;
use crate::plan::ExecutablePlan;
use crate::plan::Operation;
use crate::plan::PlannedOperation;
use crate::slice::DesiredSlice;

const TARGET_LOCK_KEY: i64 = 7_219_384_115_691_047_989;

/// Configured trusted target boundary.
#[derive(Clone, Debug)]
pub struct TargetConfig {
    /// `PostgreSQL` host on the compose network.
    pub host: String,
    /// `PostgreSQL` port.
    pub port: u16,
    /// Login role.
    pub user: String,
    /// Database name.
    pub database: String,
    /// Docker secret containing only the database password.
    pub password_file: PathBuf,
    /// Public credential-free API origin.
    pub api_url: String,
    /// Secret reference published instead of a database URL.
    pub database_url_ref: String,
    /// Secret reference published instead of an anonymous key.
    pub anon_key_ref: String,
}

/// Real local Supabase target adapter.
#[derive(Clone, Debug)]
pub struct Target {
    config: TargetConfig,
}

/// Canonical observed facts relevant to the desired slice.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ObservedTarget {
    /// Database identity and version, never credentials.
    pub identity: DatabaseIdentity,
    /// Whether the connector target ledger exists.
    pub metadata_exists: bool,
    /// Desired schemas currently present.
    pub schemas: BTreeSet<String>,
    /// Target-side migration receipts in canonical identity/ID order.
    pub migrations: Vec<ObservedMigration>,
    /// Current `PostgREST` exposed schema set.
    pub exposed_schemas: BTreeSet<String>,
    /// Whether each desired schema currently has anonymous read access.
    pub anon_read: BTreeMap<String, bool>,
}

/// One non-secret receipt from the target-authoritative migration ledger.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ObservedMigration {
    /// Stable authored resource identity.
    pub resource_id: String,
    /// Stable append-only migration ID.
    pub migration_id: String,
    /// Hash of the exact applied SQL payload.
    pub checksum: String,
}

/// Stable non-secret database identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatabaseIdentity {
    /// Current database name.
    pub database: String,
    /// `PostgreSQL` database OID rendered as a string ID.
    pub database_oid: String,
    /// `PostgreSQL` server version number.
    pub server_version: String,
}

/// Canonical provider evidence.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{detail}")]
pub struct ProviderDiagnostic {
    /// Stable connector/provider code.
    pub code: String,
    /// Verbatim provider fields in deterministic order.
    pub detail: String,
    /// Provider hint when present.
    pub help: Option<String>,
}

/// Target access failure.
#[derive(Debug, Error)]
pub enum TargetError {
    /// Target could not be reached or observed.
    #[error("Supabase target unavailable: {0}")]
    Unavailable(String),
    /// `PostgreSQL` rejected an exact planned operation.
    #[error(transparent)]
    Provider(#[from] ProviderDiagnostic),
}

/// Result of an apply transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyResult {
    /// Exact preconditions held and the operation committed.
    Applied {
        /// Target digest after commit and re-observation.
        observed_digest: String,
    },
    /// Transactional re-observation contradicted the plan before mutation.
    Stale {
        /// Fresh target digest.
        observed_digest: String,
    },
}

impl Target {
    /// Construct a target adapter without reading secrets yet.
    pub fn new(config: TargetConfig) -> Self {
        Self { config }
    }

    /// Public API URL used in non-secret outputs.
    pub fn api_url(&self) -> &str {
        &self.config.api_url
    }

    /// Stable database URL reference.
    pub fn database_url_ref(&self) -> &str {
        &self.config.database_url_ref
    }

    /// Stable anonymous key reference.
    pub fn anon_key_ref(&self) -> &str {
        &self.config.anon_key_ref
    }

    /// Observe all target facts that can affect a plan.
    pub async fn observe(&self, desired: &DesiredSlice) -> Result<ObservedTarget, TargetError> {
        let client = self.connect().await?;
        observe_with(&client, desired).await
    }

    /// Check an interrupted operation's postcondition from current truth.
    pub fn operation_satisfied(&self, operation: &Operation, observed: &ObservedTarget) -> bool {
        match operation {
            Operation::EstablishBinding { .. } => false,
            Operation::EnsureMetadata => observed.metadata_exists,
            Operation::EnsureSchema { schema, .. } => observed.schemas.contains(schema),
            Operation::ApplyMigration {
                resource_id,
                migration,
                ..
            } => observed
                .migration_checksum(resource_id, &migration.id)
                .is_some_and(|checksum| checksum == migration.checksum),
            Operation::ConfigureApi {
                exposed_schemas,
                policies,
            } => {
                observed.exposed_schemas == exposed_schemas.iter().cloned().collect()
                    && policies.iter().all(|policy| {
                        observed
                            .anon_read
                            .get(&policy.schema)
                            .is_some_and(|actual| {
                                *actual == (policy.expose && policy.anon_access == AnonAccess::Read)
                            })
                    })
            }
        }
    }

    /// Apply one exact operation after a serializable, locked freshness check.
    pub async fn apply(
        &self,
        desired: &DesiredSlice,
        plan: &ExecutablePlan,
        authoritative_plan_id: &str,
        operation: &PlannedOperation,
    ) -> Result<ApplyResult, TargetError> {
        let mut client = self.connect().await?;
        let transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await
            .map_err(unavailable)?;
        transaction
            .query_one("select pg_advisory_xact_lock($1)", &[&TARGET_LOCK_KEY])
            .await
            .map_err(unavailable)?;
        let observed = observe_with(&transaction, desired).await?;
        let digest = format!("blake3:{}", hex::encode(observed.digest()));
        if digest != plan.observed_digest {
            transaction.rollback().await.map_err(unavailable)?;
            return Ok(ApplyResult::Stale {
                observed_digest: digest,
            });
        }

        if let Err(error) = execute(&transaction, authoritative_plan_id, operation).await {
            let _ = transaction.rollback().await;
            return Err(error);
        }
        transaction
            .commit()
            .await
            .map_err(provider_or_unavailable)?;
        let after = self.observe(desired).await?;
        Ok(ApplyResult::Applied {
            observed_digest: format!("blake3:{}", hex::encode(after.digest())),
        })
    }

    async fn connect(&self) -> Result<Client, TargetError> {
        let password = fs::read_to_string(&self.config.password_file)
            .map_err(|error| unavailable(anyhow::anyhow!(error)))?;
        let mut config = tokio_postgres::Config::new();
        config
            .host(&self.config.host)
            .port(self.config.port)
            .user(&self.config.user)
            .password(password.trim())
            .dbname(&self.config.database)
            .application_name("henosis-connector-supabase");
        let (client, connection) = config.connect(NoTls).await.map_err(unavailable)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }
}

impl ObservedTarget {
    /// Find a receipt without exposing an unserializable tuple-key map.
    pub fn migration_checksum(&self, resource_id: &str, migration_id: &str) -> Option<&str> {
        self.migrations
            .iter()
            .find(|receipt| {
                receipt.resource_id == resource_id && receipt.migration_id == migration_id
            })
            .map(|receipt| receipt.checksum.as_str())
    }

    /// Deterministic digest of re-observable target facts.
    pub fn digest(&self) -> [u8; 32] {
        let bytes = serde_json::to_vec(self).expect("observed target is JSON");
        *blake3::hash(&bytes).as_bytes()
    }
}

async fn observe_with<C>(client: &C, desired: &DesiredSlice) -> Result<ObservedTarget, TargetError>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_one(
            "select current_database(), (select oid::text from pg_database where datname = \
             current_database()), current_setting('server_version_num')",
            &[],
        )
        .await
        .map_err(provider_or_unavailable)?;
    let identity = DatabaseIdentity {
        database: row.get(0),
        database_oid: row.get(1),
        server_version: row.get(2),
    };
    let metadata_exists: bool = client
        .query_one(
            "select to_regclass('henosis_connector.migration_receipts') is not null",
            &[],
        )
        .await
        .map_err(provider_or_unavailable)?
        .get(0);
    let mut schemas = BTreeSet::new();
    let mut anon_read = BTreeMap::new();
    for component in desired.components.iter() {
        let schema = &component.context.target.schema;
        let exists: bool = client
            .query_one(
                "select exists(select 1 from pg_namespace where nspname = $1)",
                &[schema],
            )
            .await
            .map_err(provider_or_unavailable)?
            .get(0);
        if exists {
            schemas.insert(schema.clone());
        }
        let access: bool = if exists {
            client
                .query_one(
                    "select has_schema_privilege('anon', $1, 'USAGE') and not exists (select 1 \
                     from pg_tables where schemaname = $1 and not has_table_privilege('anon', \
                     quote_ident(schemaname) || '.' || quote_ident(tablename), 'SELECT'))",
                    &[schema],
                )
                .await
                .map_err(provider_or_unavailable)?
                .get(0)
        } else {
            false
        };
        anon_read.insert(schema.clone(), access);
    }
    let mut migrations = Vec::new();
    if metadata_exists {
        for row in client
            .query(
                "select resource_id, migration_id, checksum from \
                 henosis_connector.migration_receipts order by resource_id, migration_id",
                &[],
            )
            .await
            .map_err(provider_or_unavailable)?
        {
            migrations.push(ObservedMigration {
                resource_id: row.get(0),
                migration_id: row.get(1),
                checksum: row.get(2),
            });
        }
    }
    let configured: String = client
        .query_one(
            "select coalesce((select split_part(setting, '=', 2) from pg_roles cross join lateral \
             unnest(coalesce(rolconfig, '{}'::text[])) setting where rolname = 'postgres' and \
             setting like 'pgrst.db_schemas=%' limit 1), 'public')",
            &[],
        )
        .await
        .map_err(provider_or_unavailable)?
        .get(0);
    let exposed_schemas = configured
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect();
    Ok(ObservedTarget {
        identity,
        metadata_exists,
        schemas,
        migrations,
        exposed_schemas,
        anon_read,
    })
}

async fn execute<C>(
    transaction: &C,
    authoritative_plan_id: &str,
    planned: &PlannedOperation,
) -> Result<(), TargetError>
where
    C: GenericClient + Sync,
{
    match &planned.operation {
        Operation::EstablishBinding { .. } => Ok(()),
        Operation::EnsureMetadata => transaction
            .batch_execute(
                "create schema if not exists henosis_connector;create table if not exists \
                 henosis_connector.migration_receipts (resource_id text not null, migration_id \
                 text not null, checksum text not null,schema_name text not null, plan_id text \
                 not null, operation_id text not null,applied_at timestamptz not null default \
                 now(),primary key (resource_id, migration_id));revoke all on schema \
                 henosis_connector from public, anon, authenticated;",
            )
            .await
            .map_err(provider_or_unavailable),
        Operation::EnsureSchema { schema, .. } => transaction
            .batch_execute(&format!(
                "create schema if not exists {}; revoke all on schema {} from public;",
                quote_identifier(schema),
                quote_identifier(schema)
            ))
            .await
            .map_err(provider_or_unavailable),
        Operation::ApplyMigration {
            resource_id,
            schema,
            migration,
        } => {
            transaction
                .batch_execute(&format!(
                    "set local search_path = {}, public, extensions;",
                    quote_identifier(schema)
                ))
                .await
                .map_err(provider_or_unavailable)?;
            transaction
                .batch_execute(&migration.sql)
                .await
                .map_err(provider_or_unavailable)?;
            transaction
                .execute(
                    "insert into henosis_connector.migration_receipts (resource_id, migration_id, \
                     checksum, schema_name, plan_id, operation_id) values ($1, $2, $3, $4, $5, $6)",
                    &[
                        resource_id,
                        &migration.id,
                        &migration.checksum,
                        schema,
                        &authoritative_plan_id,
                        &planned.id,
                    ],
                )
                .await
                .map_err(provider_or_unavailable)?;
            Ok(())
        }
        Operation::ConfigureApi {
            exposed_schemas,
            policies,
        } => {
            let schemas = exposed_schemas.join(",");
            transaction
                .batch_execute(&format!(
                    "alter role postgres set pgrst.db_schemas = {};",
                    quote_literal(&schemas)
                ))
                .await
                .map_err(provider_or_unavailable)?;
            for policy in policies {
                let schema = quote_identifier(&policy.schema);
                if policy.expose && policy.anon_access == AnonAccess::Read {
                    transaction
                        .batch_execute(&format!(
                            "grant usage on schema {schema} to anon;grant select on all tables in \
                             schema {schema} to anon;alter default privileges in schema {schema} \
                             grant select on tables to anon;"
                        ))
                        .await
                        .map_err(provider_or_unavailable)?;
                } else {
                    transaction
                        .batch_execute(&format!(
                            "revoke select on all tables in schema {schema} from anon;revoke \
                             usage on schema {schema} from anon;alter default privileges in \
                             schema {schema} revoke select on tables from anon;"
                        ))
                        .await
                        .map_err(provider_or_unavailable)?;
                }
            }
            transaction
                .batch_execute("notify pgrst, 'reload config'; notify pgrst, 'reload schema';")
                .await
                .map_err(provider_or_unavailable)
        }
    }
}

fn unavailable(error: impl std::fmt::Display) -> TargetError {
    TargetError::Unavailable(error.to_string())
}

fn provider_or_unavailable(error: tokio_postgres::Error) -> TargetError {
    let Some(database) = error.as_db_error() else {
        return unavailable(error);
    };
    let mut lines = vec![
        format!("severity: {}", database.severity()),
        format!("code: {}", database.code().code()),
        format!("message: {}", database.message()),
    ];
    if let Some(detail) = database.detail() {
        lines.push(format!("detail: {detail}"));
    }
    if let Some(hint) = database.hint() {
        lines.push(format!("hint: {hint}"));
    }
    if let Some(position) = database.position() {
        lines.push(format!("position: {position:?}"));
    }
    if let Some(context) = database.where_() {
        lines.push(format!("where: {context}"));
    }
    TargetError::Provider(ProviderDiagnostic {
        code: format!("supabase.postgres.{}", database.code().code()),
        detail: lines.join("\n"),
        help: database.hint().map(str::to_owned),
    })
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_digest_serializes_canonical_migration_receipts() {
        let observed = ObservedTarget {
            identity: DatabaseIdentity {
                database: "postgres".into(),
                database_oid: "5".into(),
                server_version: "170006".into(),
            },
            metadata_exists: true,
            schemas: BTreeSet::from(["catalog".into()]),
            migrations: vec![ObservedMigration {
                resource_id: "catalog".into(),
                migration_id: "001".into(),
                checksum: "sha256:abc".into(),
            }],
            exposed_schemas: BTreeSet::from(["public".into()]),
            anon_read: BTreeMap::from([("catalog".into(), false)]),
        };

        assert_ne!(observed.digest(), [0; 32]);
        assert_eq!(
            observed.migration_checksum("catalog", "001"),
            Some("sha256:abc")
        );
    }
}
