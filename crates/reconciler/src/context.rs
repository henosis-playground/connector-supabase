//! Strict connector-owned authoring context.

use serde::Deserialize;
use serde::Serialize;
use sha2::Digest as _;
use sha2::Sha256;
use thiserror::Error;

/// Current component-context discriminator.
pub const API_VERSION: &str = "henosis.dev/supabase-component-context/v1";

/// Strict JSON object carried by `ComponentSpec.connector_context`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ComponentContext {
    /// Versioned schema discriminator.
    pub api_version: String,
    /// Stable author-owned logical identity.
    pub resource_id: String,
    /// Configured native target selection.
    pub target: TargetContext,
    /// Ordered transitions, oldest first.
    pub migrations: Vec<Migration>,
    /// Desired `PostgREST` exposure and grants.
    pub api: ApiContext,
}

/// Local self-hosted target coordinates.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetContext {
    /// Configured stack key.
    pub stack: String,
    /// Self-hosted single-project identity.
    pub project: String,
    /// `PostgreSQL` database name.
    pub database: String,
    /// Connector-owned `PostgreSQL` schema.
    pub schema: String,
}

/// One immutable ordered SQL transition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Migration {
    /// Strictly increasing durable transition identity.
    pub id: String,
    /// `sha256:` digest over the exact SQL bytes.
    pub checksum: String,
    /// Non-secret additive SQL.
    pub sql: String,
    /// Typed upstream values exposed to this migration through
    /// transaction-local settings.
    #[serde(default)]
    pub inputs: Vec<InputSlot>,
}

/// One native migration input declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InputSlot {
    /// `PostgreSQL` setting suffix available as `henosis.input.<name>`.
    pub name: String,
    /// Immutable producer component-spec hash.
    pub producer_component_spec_hash: [u8; 32],
    /// Top-level producer output property.
    pub output: String,
    /// Optional value used when the producer output is absent.
    pub default: Option<serde_json::Value>,
}

/// Reconciled `PostgREST` configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ApiContext {
    /// Whether `PostgREST` exposes this schema.
    pub expose: bool,
    /// Anonymous access policy.
    pub anon_access: AnonAccess,
}

/// V1 anonymous API access policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AnonAccess {
    /// No anonymous grants.
    None,
    /// Schema usage and SELECT on all current tables.
    Read,
}

/// Context decoding or boundary validation failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ContextError {
    /// Bytes are not strict v1 JSON.
    #[error("invalid {API_VERSION} JSON: {0}")]
    Json(String),
    /// A boundary field is outside the v1 vocabulary.
    #[error("invalid {API_VERSION}: {0}")]
    Invalid(String),
}

/// A diagnostic discovered in the mutation-free planning phase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanIssue {
    /// Stable diagnostic code.
    pub code: &'static str,
    /// Canonical safe detail.
    pub message: String,
    /// RFC 6901 path rooted at connector context.
    pub pointer: String,
    /// Actionable author guidance.
    pub help: String,
}

impl ComponentContext {
    /// Decode strict JSON and validate identity/target fields needed for safe
    /// durable acceptance. Migration-content checks intentionally happen in
    /// the plan phase so failures can be reported atomically to core.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ContextError> {
        let value = serde_json::from_slice::<Self>(bytes)
            .map_err(|error| ContextError::Json(error.to_string()))?;
        value.validate_boundary()?;
        Ok(value)
    }

    fn validate_boundary(&self) -> Result<(), ContextError> {
        if self.api_version != API_VERSION {
            return Err(ContextError::Invalid(format!(
                "unsupported apiVersion {:?}; expected {API_VERSION:?}",
                self.api_version
            )));
        }
        validate_logical_id(&self.resource_id, "resourceId")?;
        if self.target.stack != "local" {
            return Err(ContextError::Invalid(
                "target.stack must be the configured v1 stack \"local\"".into(),
            ));
        }
        if self.target.project != "henosis-local" {
            return Err(ContextError::Invalid(
                "target.project must be the self-hosted v1 project \"henosis-local\"".into(),
            ));
        }
        if self.target.database != "postgres" {
            return Err(ContextError::Invalid(
                "target.database must be the v1 database \"postgres\"".into(),
            ));
        }
        validate_schema(&self.target.schema)?;
        Ok(())
    }

    /// Accumulate content problems without touching the target.
    pub fn plan_issues(&self) -> Vec<PlanIssue> {
        let mut issues = Vec::new();
        let mut previous = None::<&str>;
        for (index, migration) in self.migrations.iter().enumerate() {
            if !is_migration_id(&migration.id) {
                issues.push(PlanIssue {
                    code: "supabase.plan.migration-id",
                    message: format!("migration id {:?} is invalid", migration.id),
                    pointer: format!("/migrations/{index}/id"),
                    help: "Use 1-96 lowercase letters, digits, underscores, or hyphens, beginning \
                           with a letter or digit."
                        .into(),
                });
            }
            if previous.is_some_and(|value| value >= migration.id.as_str()) {
                issues.push(PlanIssue {
                    code: "supabase.plan.migration-order",
                    message: format!(
                        "migration {:?} is not strictly after the preceding migration",
                        migration.id
                    ),
                    pointer: format!("/migrations/{index}/id"),
                    help: "Keep migration IDs unique and in strictly increasing lexical order."
                        .into(),
                });
            }
            previous = Some(&migration.id);
            let actual = format!(
                "sha256:{}",
                hex::encode(Sha256::digest(migration.sql.as_bytes()))
            );
            if migration.checksum != actual {
                issues.push(PlanIssue {
                    code: "supabase.plan.migration-checksum",
                    message: format!(
                        "migration {:?} declares checksum {:?}, but its exact SQL hashes to \
                         {actual:?}",
                        migration.id, migration.checksum
                    ),
                    pointer: format!("/migrations/{index}/checksum"),
                    help: "Regenerate checksum as SHA-256 over the exact UTF-8 SQL bytes; never \
                           edit an already-applied migration ID."
                        .into(),
                });
            }
            let mut input_names = std::collections::BTreeSet::new();
            for (input_index, input) in migration.inputs.iter().enumerate() {
                if !is_input_name(&input.name) || !is_input_name(&input.output) {
                    issues.push(PlanIssue {
                        code: "supabase.input.invalid",
                        message: format!(
                            "migration {:?} input {:?} has an invalid name or output property",
                            migration.id, input.name
                        ),
                        pointer: format!("/migrations/{index}/inputs/{input_index}"),
                        help: "Use lowercase letters, digits, and underscores, beginning with a \
                               letter."
                            .into(),
                    });
                }
                if !input_names.insert(&input.name) {
                    issues.push(PlanIssue {
                        code: "supabase.input.duplicate",
                        message: format!(
                            "migration {:?} declares input {:?} more than once",
                            migration.id, input.name
                        ),
                        pointer: format!("/migrations/{index}/inputs/{input_index}/name"),
                        help: "Give each migration input a unique setting name.".into(),
                    });
                }
            }
            if migration.sql.trim().is_empty() {
                issues.push(PlanIssue {
                    code: "supabase.plan.migration-empty",
                    message: format!("migration {:?} has no SQL", migration.id),
                    pointer: format!("/migrations/{index}/sql"),
                    help: "Remove the migration or provide one additive SQL transition.".into(),
                });
            } else if let Some(verb) = forbidden_sql_verb(&migration.sql) {
                issues.push(PlanIssue {
                    code: "supabase.plan.destructive-unsupported",
                    message: format!(
                        "migration {:?} contains v1-unsupported verb {verb:?}",
                        migration.id
                    ),
                    pointer: format!("/migrations/{index}/sql"),
                    help: "V1 auto-applies additive SQL only; split out destructive or \
                           data-rewriting work for a future approval-capable policy."
                        .into(),
                });
            }
        }
        issues
    }
}

fn validate_logical_id(value: &str, field: &str) -> Result<(), ContextError> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && value.as_bytes()[0].is_ascii_lowercase()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(ContextError::Invalid(format!(
            "{field} must match [a-z][a-z0-9_-]{{0,62}}"
        )))
    }
}

fn validate_schema(value: &str) -> Result<(), ContextError> {
    validate_logical_id(value, "target.schema")?;
    const RESERVED: &[&str] = &[
        "auth",
        "extensions",
        "graphql",
        "graphql_public",
        "henosis_connector",
        "information_schema",
        "pg_catalog",
        "public",
        "realtime",
        "storage",
        "supabase_functions",
        "vault",
    ];
    if value.starts_with("pg_") || RESERVED.contains(&value) {
        Err(ContextError::Invalid(format!(
            "target.schema {value:?} is reserved"
        )))
    } else {
        Ok(())
    }
}

fn is_input_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_migration_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0].is_ascii_digit())
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn forbidden_sql_verb(sql: &str) -> Option<&'static str> {
    const FORBIDDEN: &[&str] = &[
        "ALTER", "CALL", "COPY", "DELETE", "DO", "DROP", "MERGE", "REVOKE", "TRUNCATE", "UPDATE",
    ];
    let tokens = sql
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_uppercase)
        .collect::<Vec<_>>();
    if tokens
        .windows(3)
        .any(|window| window == ["CREATE", "OR", "REPLACE"])
    {
        return Some("CREATE OR REPLACE");
    }
    FORBIDDEN
        .iter()
        .copied()
        .find(|verb| tokens.iter().any(|token| token == verb))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> ComponentContext {
        let sql = "create table items (id bigint primary key);".to_owned();
        ComponentContext {
            api_version: API_VERSION.into(),
            resource_id: "catalog".into(),
            target: TargetContext {
                stack: "local".into(),
                project: "henosis-local".into(),
                database: "postgres".into(),
                schema: "catalog".into(),
            },
            migrations: vec![Migration {
                id: "202607130001_create_items".into(),
                checksum: format!("sha256:{}", hex::encode(Sha256::digest(sql.as_bytes()))),
                sql,
                inputs: Vec::new(),
            }],
            api: ApiContext {
                expose: true,
                anon_access: AnonAccess::Read,
            },
        }
    }

    #[test]
    fn valid_context_has_no_plan_issues() {
        let context = context();
        context.validate_boundary().unwrap();
        assert!(context.plan_issues().is_empty());
    }

    #[test]
    fn checksum_mismatch_is_a_plan_issue() {
        let mut context = context();
        context.migrations[0].checksum = format!("sha256:{}", "0".repeat(64));
        assert_eq!(
            context.plan_issues()[0].code,
            "supabase.plan.migration-checksum"
        );
    }

    #[test]
    fn destructive_sql_is_rejected_during_plan() {
        let mut context = context();
        context.migrations[0].sql = "drop table items;".into();
        context.migrations[0].checksum = format!(
            "sha256:{}",
            hex::encode(Sha256::digest(context.migrations[0].sql.as_bytes()))
        );
        assert_eq!(
            context.plan_issues()[0].code,
            "supabase.plan.destructive-unsupported"
        );
    }

    #[test]
    fn unknown_fields_fail_closed_at_boundary() {
        let mut value = serde_json::to_value(context()).unwrap();
        value["secret"] = serde_json::json!("must-not-pass");
        assert!(ComponentContext::from_bytes(&serde_json::to_vec(&value).unwrap()).is_err());
    }
}
