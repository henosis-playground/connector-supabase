//! Native Supabase project to Henosis component-spec derivation.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use henosis_supabase_reconciler::CONNECTOR_NAME;
use henosis_supabase_reconciler::context::API_VERSION as CONTEXT_API_VERSION;
use henosis_supabase_reconciler::context::AnonAccess;
use henosis_supabase_reconciler::context::ApiContext;
use henosis_supabase_reconciler::context::ComponentContext;
use henosis_supabase_reconciler::context::Migration;
use henosis_supabase_reconciler::context::TargetContext;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest as _;
use sha2::Sha256;
use thiserror::Error;

/// Minimal marker format understood by this authoring integration.
pub const MARKER_API_VERSION: &str = "henosis.dev/supabase-component/v1";

/// Fully derived material needed to construct a core `ComponentSpec`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedComponent {
    /// Component name derived from native `project_id`.
    pub name: String,
    /// Connector assignment implied by the marker version.
    pub connector: String,
    /// Connector-defined, fixed output contract.
    pub outputs_schema: serde_json::Value,
    /// Resolved immutable component-spec hashes from graph metadata.
    pub depends_on: Vec<Vec<u8>>,
    /// Exact opaque connector context derived from native files.
    pub connector_context: ComponentContext,
}

impl DerivedComponent {
    /// Serialize the opaque connector-owned context bytes sent to core.
    pub fn connector_context_bytes(&self) -> Result<Vec<u8>, DeriveError> {
        serde_json::to_vec(&self.connector_context)
            .map_err(|source| DeriveError::Serialize { source })
    }

    /// Serialize the fixed output JSON Schema sent to core.
    pub fn outputs_schema_bytes(&self) -> Result<Vec<u8>, DeriveError> {
        serde_json::to_vec(&self.outputs_schema).map_err(|source| DeriveError::Serialize { source })
    }
}

/// Native project or marker error discovered before component registration.
#[derive(Debug, Error)]
pub enum DeriveError {
    /// A required file or directory cannot be read.
    #[error("cannot read {path}: {source}")]
    Read {
        /// Native path that failed.
        path: PathBuf,
        /// Filesystem error.
        source: std::io::Error,
    },
    /// A TOML source is malformed.
    #[error("invalid TOML in {path}: {source}")]
    Toml {
        /// Source path.
        path: PathBuf,
        /// Parser detail.
        source: toml::de::Error,
    },
    /// Authoring configuration is coherent TOML but violates the contract.
    #[error("invalid Supabase component at {path}: {message}")]
    Invalid {
        /// Most relevant source path.
        path: PathBuf,
        /// Actionable detail.
        message: String,
    },
    /// Derived values unexpectedly cannot be serialized.
    #[error("cannot serialize derived component: {source}")]
    Serialize {
        /// JSON serialization error.
        source: serde_json::Error,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    api_version: String,
    schema: String,
    #[serde(default)]
    depends_on: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SupabaseConfig {
    project_id: String,
    api: SupabaseApi,
    #[serde(default)]
    db: SupabaseDatabase,
}

#[derive(Debug, Deserialize)]
struct SupabaseApi {
    #[serde(default = "enabled")]
    enabled: bool,
    #[serde(default)]
    schemas: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct SupabaseDatabase {
    #[serde(default)]
    migrations: SupabaseMigrations,
}

#[derive(Debug, Deserialize)]
struct SupabaseMigrations {
    #[serde(default = "enabled")]
    enabled: bool,
}

impl Default for SupabaseMigrations {
    fn default() -> Self {
        Self { enabled: true }
    }
}

const fn enabled() -> bool {
    true
}

/// Derive a complete component authoring result from a repository root.
pub fn derive_component(repository: impl AsRef<Path>) -> Result<DerivedComponent, DeriveError> {
    let root = repository.as_ref();
    let marker_path = root.join("henosis.toml");
    let config_path = root.join("supabase/config.toml");
    let marker = parse_toml::<Marker>(&marker_path)?;
    let config = parse_toml::<SupabaseConfig>(&config_path)?;

    if marker.api_version != MARKER_API_VERSION {
        return invalid(
            marker_path,
            format!(
                "api_version must be {MARKER_API_VERSION:?}, got {:?}",
                marker.api_version
            ),
        );
    }
    if config.project_id.is_empty() {
        return invalid(config_path, "project_id must not be empty");
    }
    if !config.db.migrations.enabled {
        return invalid(
            config_path,
            "[db.migrations].enabled must remain true because native migrations are the component \
             source",
        );
    }

    let api_expose = config.api.enabled && config.api.schemas.iter().any(|v| v == &marker.schema);
    let migrations = derive_migrations(&root.join("supabase/migrations"))?;
    let anon_access = derive_anon_access(&migrations, &marker.schema, &marker_path)?;
    let depends_on = marker
        .depends_on
        .iter()
        .enumerate()
        .map(|(index, value)| parse_dependency(value, index, &marker_path))
        .collect::<Result<Vec<_>, _>>()?;
    let context = ComponentContext {
        api_version: CONTEXT_API_VERSION.into(),
        resource_id: marker.schema.clone(),
        target: TargetContext {
            stack: "local".into(),
            project: "henosis-local".into(),
            database: "postgres".into(),
            schema: marker.schema,
        },
        migrations,
        api: ApiContext {
            expose: api_expose,
            anon_access,
        },
    };
    // Reuse the connector boundary validation rather than maintaining a
    // second spelling of its schema/identity rules.
    ComponentContext::from_bytes(
        &serde_json::to_vec(&context).map_err(|source| DeriveError::Serialize { source })?,
    )
    .map_err(|source| DeriveError::Invalid {
        path: marker_path,
        message: source.to_string(),
    })?;

    Ok(DerivedComponent {
        name: config.project_id,
        connector: CONNECTOR_NAME.into(),
        outputs_schema: output_schema(),
        depends_on,
        connector_context: context,
    })
}

fn derive_migrations(directory: &Path) -> Result<Vec<Migration>, DeriveError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(DeriveError::Read {
                path: directory.into(),
                source,
            });
        }
    };
    let mut paths = entries
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|source| DeriveError::Read {
                    path: directory.into(),
                    source,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.retain(|path| path.extension().is_some_and(|extension| extension == "sql"));
    paths.sort();

    paths
        .into_iter()
        .map(|path| {
            let id = path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| DeriveError::Invalid {
                    path: path.clone(),
                    message: "migration filename must be UTF-8 and end in .sql".into(),
                })?
                .to_owned();
            let bytes = fs::read(&path).map_err(|source| DeriveError::Read {
                path: path.clone(),
                source,
            })?;
            let sql = String::from_utf8(bytes).map_err(|source| DeriveError::Invalid {
                path: path.clone(),
                message: format!("migration must be UTF-8: {source}"),
            })?;
            Ok(Migration {
                id,
                checksum: format!("sha256:{}", hex::encode(Sha256::digest(sql.as_bytes()))),
                sql,
            })
        })
        .collect()
}

fn parse_dependency(value: &str, index: usize, path: &Path) -> Result<Vec<u8>, DeriveError> {
    let bytes = hex::decode(value).map_err(|source| DeriveError::Invalid {
        path: path.into(),
        message: format!(
            "depends_on[{index}] must be a 64-character lowercase hex spec hash: {source}"
        ),
    })?;
    if value.len() != 64 || value.bytes().any(|byte| byte.is_ascii_uppercase()) || bytes.len() != 32
    {
        return invalid(
            path.into(),
            format!("depends_on[{index}] must be a 64-character lowercase hex spec hash"),
        );
    }
    Ok(bytes)
}

fn derive_anon_access(
    migrations: &[Migration],
    schema: &str,
    marker_path: &Path,
) -> Result<AnonAccess, DeriveError> {
    let sql = migrations
        .iter()
        .map(|migration| migration.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let uncommented = sql
        .lines()
        .map(|line| line.split_once("--").map(|(sql, _)| sql).unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");
    let statements = uncommented
        .split(';')
        .map(|statement| {
            statement
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_lowercase()
        })
        .collect::<Vec<_>>();
    let usage = statements
        .iter()
        .any(|statement| statement == &format!("grant usage on schema {schema} to anon"));
    let select = statements.iter().any(|statement| {
        statement == &format!("grant select on all tables in schema {schema} to anon")
    });
    match (usage, select) {
        (true, true) => Ok(AnonAccess::Read),
        (false, false) => Ok(AnonAccess::None),
        _ => invalid(
            marker_path.into(),
            format!(
                "native anon-read policy for schema {schema:?} is incomplete: migrations must \
                 contain both `grant usage on schema {schema} to anon` and `grant select on all \
                 tables in schema {schema} to anon`, or neither"
            ),
        ),
    }
}

fn parse_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, DeriveError> {
    let source = fs::read_to_string(path).map_err(|source| DeriveError::Read {
        path: path.into(),
        source,
    })?;
    toml::from_str(&source).map_err(|source| DeriveError::Toml {
        path: path.into(),
        source,
    })
}

fn invalid<T>(path: PathBuf, message: impl Into<String>) -> Result<T, DeriveError> {
    Err(DeriveError::Invalid {
        path,
        message: message.into(),
    })
}

fn output_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": false,
        "required": ["project", "database", "schema", "apiUrl", "restUrl", "databaseUrlRef", "anonKeyRef"],
        "properties": {
            "project": {"type": "string"},
            "database": {"type": "string"},
            "schema": {"type": "string"},
            "apiUrl": {"type": "string", "format": "uri"},
            "restUrl": {"type": "string", "format": "uri"},
            "databaseUrlRef": {"type": "string"},
            "anonKeyRef": {"type": "string"}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native_project() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("supabase/migrations")).unwrap();
        fs::write(
            root.path().join("henosis.toml"),
            r#"api_version = "henosis.dev/supabase-component/v1"
schema = "catalog"
depends_on = []
"#,
        )
        .unwrap();
        fs::write(
            root.path().join("supabase/config.toml"),
            r#"project_id = "service-d"
[api]
enabled = true
schemas = ["public", "graphql_public", "catalog"]
[db.migrations]
enabled = true
"#,
        )
        .unwrap();
        fs::write(
            root.path()
                .join("supabase/migrations/202607130001_create_items.sql"),
            "create table items (id bigint primary key);\ngrant usage on schema catalog to \
             anon;\ngrant select on all tables in schema catalog to anon;\n",
        )
        .unwrap();
        root
    }

    #[test]
    fn derives_native_files_without_copying_them() {
        let root = native_project();
        let derived = derive_component(root.path()).unwrap();
        assert_eq!(derived.name, "service-d");
        assert_eq!(derived.connector_context.resource_id, "catalog");
        assert!(derived.connector_context.api.expose);
        assert_eq!(derived.connector_context.migrations.len(), 1);
        assert_eq!(
            derived.connector_context.migrations[0].sql,
            "create table items (id bigint primary key);\ngrant usage on schema catalog to \
             anon;\ngrant select on all tables in schema catalog to anon;\n"
        );
        assert_eq!(derived.connector_context.api.anon_access, AnonAccess::Read);
    }

    #[test]
    fn exposure_is_derived_from_native_api_schemas() {
        let root = native_project();
        fs::write(
            root.path().join("supabase/config.toml"),
            "project_id = \"service-d\"\n[api]\nschemas = [\"public\"]\n",
        )
        .unwrap();
        assert!(
            !derive_component(root.path())
                .unwrap()
                .connector_context
                .api
                .expose
        );
    }

    #[test]
    fn native_migration_checksum_cannot_drift() {
        let root = native_project();
        let first = derive_component(root.path()).unwrap();
        fs::write(
            root.path()
                .join("supabase/migrations/202607130001_create_items.sql"),
            "create table items (id uuid primary key);\n",
        )
        .unwrap();
        let second = derive_component(root.path()).unwrap();
        assert_ne!(
            first.connector_context.migrations[0].checksum,
            second.connector_context.migrations[0].checksum
        );
    }

    #[test]
    fn partial_or_commented_grants_cannot_claim_read_policy() {
        let root = native_project();
        fs::write(
            root.path()
                .join("supabase/migrations/202607130001_create_items.sql"),
            "-- grant select on all tables in schema catalog to anon;\ngrant usage on schema \
             catalog to anon;\n",
        )
        .unwrap();
        let error = derive_component(root.path()).unwrap_err().to_string();
        assert!(error.contains("native anon-read policy"));
        assert!(error.contains("both"));
    }
}
