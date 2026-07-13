//! Native Supabase project to Henosis component-spec derivation.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use henosis_supabase_reconciler::CONNECTOR_NAME;
use henosis_supabase_reconciler::context::API_VERSION as CONTEXT_API_VERSION;
use henosis_supabase_reconciler::context::AnonAccess;
use henosis_supabase_reconciler::context::ApiContext;
use henosis_supabase_reconciler::context::ComponentContext;
use henosis_supabase_reconciler::context::InputSlot;
use henosis_supabase_reconciler::context::Migration;
use henosis_supabase_reconciler::context::TargetContext;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest as _;
use sha2::Sha256;
use thiserror::Error;

/// Fully derived material needed to construct a core `ComponentSpec`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedComponent {
    /// Component name derived from native `project_id`.
    pub name: String,
    /// Connector assignment implied by the native `supabase/` project.
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

/// Native project error discovered before component registration.
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
struct SupabaseConfig {
    #[serde(default)]
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
    let config_path = root.join("supabase/config.toml");
    let config = parse_toml::<SupabaseConfig>(&config_path)?;
    let name = if config.project_id.trim().is_empty() {
        root.file_name()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| DeriveError::Invalid {
                path: config_path.clone(),
                message: "project_id is empty and the repository directory has no UTF-8 name"
                    .into(),
            })?
            .to_owned()
    } else {
        config.project_id.clone()
    };
    let schema = native_schema(&name, &config_path)?;
    if !config.db.migrations.enabled {
        return invalid(
            config_path,
            "[db.migrations].enabled must remain true because native migrations are the component \
             source",
        );
    }

    let api_expose = config.api.enabled && config.api.schemas.iter().any(|value| value == &schema);
    let migrations = derive_migrations(&root.join("supabase/migrations"))?;
    let anon_access = derive_anon_access(&migrations, &schema, &config_path)?;
    let depends_on = migrations
        .iter()
        .flat_map(|migration| migration.inputs.iter())
        .map(|input| input.producer_component_spec_hash.to_vec())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let context = ComponentContext {
        api_version: CONTEXT_API_VERSION.into(),
        resource_id: schema.clone(),
        target: TargetContext {
            stack: "local".into(),
            project: "henosis-local".into(),
            database: "postgres".into(),
            schema,
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
        path: config_path,
        message: source.to_string(),
    })?;

    Ok(DerivedComponent {
        name,
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
            let inputs = derive_inputs(&sql, &path)?;
            Ok(Migration {
                id,
                checksum: format!("sha256:{}", hex::encode(Sha256::digest(sql.as_bytes()))),
                sql,
                inputs,
            })
        })
        .collect()
}

// === Native identity and input derivation ===

fn native_schema(name: &str, path: &Path) -> Result<String, DeriveError> {
    let schema = name.replace('-', "_");
    if schema.is_empty()
        || schema.len() > 63
        || !schema.as_bytes()[0].is_ascii_lowercase()
        || !schema
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return invalid(
            path.into(),
            format!(
                "project_id {name:?} cannot derive a PostgreSQL schema; use lowercase letters, \
                 digits, hyphens, or underscores"
            ),
        );
    }
    Ok(schema)
}

fn derive_inputs(sql: &str, path: &Path) -> Result<Vec<InputSlot>, DeriveError> {
    sql.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            line.trim()
                .strip_prefix("-- henosis:input ")
                .map(|declaration| (index + 1, declaration))
        })
        .map(|(line, declaration)| parse_input(declaration, line, path))
        .collect()
}

fn parse_input(declaration: &str, line: usize, path: &Path) -> Result<InputSlot, DeriveError> {
    let (name, placeholder) = declaration
        .split_once('=')
        .ok_or_else(|| DeriveError::Invalid {
            path: path.into(),
            message: format!(
                "line {line}: input declaration must be NAME=${{henosis:HASH.output}}"
            ),
        })?;
    let body = placeholder
        .strip_prefix("${henosis:")
        .and_then(|value| value.strip_suffix('}'))
        .ok_or_else(|| DeriveError::Invalid {
            path: path.into(),
            message: format!("line {line}: input value must use ${{henosis:HASH.output}}"),
        })?;
    let (reference, default) = body
        .split_once(":-")
        .map(|(reference, default)| (reference, Some(default)))
        .unwrap_or((body, None));
    let (producer, output) = reference
        .rsplit_once('.')
        .ok_or_else(|| DeriveError::Invalid {
            path: path.into(),
            message: format!("line {line}: input reference must contain HASH.output"),
        })?;
    let bytes = hex::decode(producer).map_err(|source| DeriveError::Invalid {
        path: path.into(),
        message: format!("line {line}: producer hash is invalid: {source}"),
    })?;
    let producer_component_spec_hash = bytes.try_into().map_err(|_| DeriveError::Invalid {
        path: path.into(),
        message: format!(
            "line {line}: producer hash must contain 64 lowercase hexadecimal characters"
        ),
    })?;
    if producer.len() != 64 || producer.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return invalid(
            path.into(),
            format!("line {line}: producer hash must contain 64 lowercase hexadecimal characters"),
        );
    }
    let default = default
        .map(|value| {
            serde_json::from_str(value).map_err(|source| DeriveError::Invalid {
                path: path.into(),
                message: format!("line {line}: input default must be JSON: {source}"),
            })
        })
        .transpose()?;
    Ok(InputSlot {
        name: name.trim().into(),
        producer_component_spec_hash,
        output: output.into(),
        default,
    })
}

fn derive_anon_access(
    migrations: &[Migration],
    schema: &str,
    config_path: &Path,
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
            config_path.into(),
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
            root.path().join("supabase/config.toml"),
            r#"project_id = "service-d"
[api]
enabled = true
schemas = ["public", "graphql_public", "service_d"]
[db.migrations]
enabled = true
"#,
        )
        .unwrap();
        fs::write(
            root.path()
                .join("supabase/migrations/202607130001_create_items.sql"),
            "create table items (id bigint primary key);\ngrant usage on schema service_d to \
             anon;\ngrant select on all tables in schema service_d to anon;\n",
        )
        .unwrap();
        root
    }

    #[test]
    fn derives_native_files_without_copying_them() {
        let root = native_project();
        let derived = derive_component(root.path()).unwrap();
        assert_eq!(derived.name, "service-d");
        assert_eq!(derived.connector_context.resource_id, "service_d");
        assert!(derived.connector_context.api.expose);
        assert_eq!(derived.connector_context.migrations.len(), 1);
        assert_eq!(
            derived.connector_context.migrations[0].sql,
            "create table items (id bigint primary key);\ngrant usage on schema service_d to \
             anon;\ngrant select on all tables in schema service_d to anon;\n"
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
    fn derives_dependencies_and_slots_from_native_migration_comments() {
        let root = native_project();
        let producer = "09".repeat(32);
        fs::write(
            root.path()
                .join("supabase/migrations/202607130001_create_items.sql"),
            format!(
                "-- henosis:input upstream_url=${{henosis:{producer}.apiUrl}}\ncreate table items \
                 (url text default current_setting('henosis.input.upstream_url'));\n"
            ),
        )
        .unwrap();
        let derived = derive_component(root.path()).unwrap();
        assert_eq!(derived.depends_on, vec![vec![9; 32]]);
        assert_eq!(
            derived.connector_context.migrations[0].inputs[0].name,
            "upstream_url"
        );
        assert_eq!(
            derived.connector_context.migrations[0].inputs[0].output,
            "apiUrl"
        );
    }

    #[test]
    fn partial_or_commented_grants_cannot_claim_read_policy() {
        let root = native_project();
        fs::write(
            root.path()
                .join("supabase/migrations/202607130001_create_items.sql"),
            "-- grant select on all tables in schema service_d to anon;\ngrant usage on schema \
             service_d to anon;\n",
        )
        .unwrap();
        let error = derive_component(root.path()).unwrap_err().to_string();
        assert!(error.contains("native anon-read policy"));
        assert!(error.contains("both"));
    }
}
