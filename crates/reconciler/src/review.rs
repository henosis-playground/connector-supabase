//! Atomic private-plan and redacted review-projection persistence.

use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;

use serde::Serialize;
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::plan::ExecutablePlan;
use crate::plan::PlannedOperation;

/// Persisted plan/review coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReviewArtifacts {
    /// Private executable payload path.
    pub plan_path: PathBuf,
    /// Redacted canonical JSON projection.
    pub json_path: PathBuf,
    /// Human review projection.
    pub markdown_path: PathBuf,
}

/// Plan artifact persistence failure.
#[derive(Debug, Error)]
pub enum ReviewError {
    /// Filesystem operation failed.
    #[error("review artifact I/O failed: {0}")]
    Io(String),
    /// Plan JSON could not be encoded/decoded.
    #[error("review plan JSON failed: {0}")]
    Json(String),
    /// A private plan failed its content digest check.
    #[error("private executable plan digest does not match planId")]
    Digest,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReviewProjection<'a> {
    api_version: &'a str,
    plan_id: &'a str,
    executable_digest: &'a str,
    connector: &'a str,
    connector_build: &'a str,
    policy_version: &'a str,
    graph_id: &'a str,
    generation: &'a str,
    slice_sequence: &'a str,
    target: &'a str,
    desired_digest: &'a str,
    observed_digest: &'a str,
    journal_tail: &'a str,
    operations: Vec<ReviewOperation<'a>>,
    planned_outputs: &'a [crate::plan::PlannedOutput],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReviewOperation<'a> {
    id: &'a str,
    address: &'a str,
    component_scopes: &'a [String],
    execution_class: crate::plan::ExecutionClass,
    action: crate::plan::Action,
    before: &'a serde_json::Value,
    after: &'a serde_json::Value,
    destructive_level: crate::plan::DestructiveLevel,
    dependencies: &'a [String],
    preconditions: &'a [String],
}

/// Atomically persist the exact executable and its two review views.
pub fn persist(state_dir: &Path, plan: &ExecutablePlan) -> Result<ReviewArtifacts, ReviewError> {
    if !plan.verify_digest() {
        return Err(ReviewError::Digest);
    }
    let token = plan.plan_id.replace(':', "-");
    let plans = state_dir.join("plans");
    let reviews = state_dir.join("reviews").join(&token);
    fs::create_dir_all(&plans).map_err(io)?;
    fs::create_dir_all(&reviews).map_err(io)?;
    let plan_path = plans.join(format!("{token}.json"));
    let json_path = reviews.join("plan.json");
    let markdown_path = reviews.join("plan.md");

    atomic_write(
        &plan_path,
        &serde_json::to_vec(plan).map_err(|error| ReviewError::Json(error.to_string()))?,
    )?;
    let projection = projection(plan);
    atomic_write(
        &json_path,
        &serde_json::to_vec_pretty(&projection)
            .map_err(|error| ReviewError::Json(error.to_string()))?,
    )?;
    atomic_write(&markdown_path, markdown(plan).as_bytes())?;
    Ok(ReviewArtifacts {
        plan_path,
        json_path,
        markdown_path,
    })
}

/// Load and verify one exact private executable payload.
pub fn load(path: &Path) -> Result<ExecutablePlan, ReviewError> {
    let bytes = fs::read(path).map_err(io)?;
    let plan = serde_json::from_slice::<ExecutablePlan>(&bytes)
        .map_err(|error| ReviewError::Json(error.to_string()))?;
    if plan.verify_digest() {
        Ok(plan)
    } else {
        Err(ReviewError::Digest)
    }
}

fn projection(plan: &ExecutablePlan) -> ReviewProjection<'_> {
    ReviewProjection {
        api_version: &plan.api_version,
        plan_id: &plan.plan_id,
        executable_digest: &plan.plan_id,
        connector: &plan.connector,
        connector_build: &plan.connector_build,
        policy_version: &plan.policy_version,
        graph_id: &plan.graph_id,
        generation: &plan.generation,
        slice_sequence: &plan.slice_sequence,
        target: &plan.target,
        desired_digest: &plan.desired_digest,
        observed_digest: &plan.observed_digest,
        journal_tail: &plan.journal_tail,
        operations: plan.operations.iter().map(review_operation).collect(),
        planned_outputs: &plan.planned_outputs,
    }
}

fn review_operation(operation: &PlannedOperation) -> ReviewOperation<'_> {
    ReviewOperation {
        id: &operation.id,
        address: &operation.address,
        component_scopes: &operation.component_scopes,
        execution_class: operation.execution_class,
        action: operation.action,
        before: &operation.before,
        after: &operation.after,
        destructive_level: operation.destructive_level,
        dependencies: &operation.dependencies,
        preconditions: &operation.preconditions,
    }
}

fn markdown(plan: &ExecutablePlan) -> String {
    let mut document = format!(
        "# Supabase plan\n\n- Plan: `{}`\n- Graph: `{}` generation `{}` / slice `{}`\n- Target: \
         `{}`\n- Desired: `{}`\n- Observed: `{}`\n- Journal tail fence: `{}`\n\n| Action | Class \
         | Address | Safety |\n|---|---|---|---|\n",
        plan.plan_id,
        plan.graph_id,
        plan.generation,
        plan.slice_sequence,
        plan.target,
        plan.desired_digest,
        plan.observed_digest,
        plan.journal_tail,
    );
    for operation in &plan.operations {
        document.push_str(&format!(
            "| `{:?}` | `{:?}` | `{}` | `{:?}` |\n",
            operation.action,
            operation.execution_class,
            operation.address,
            operation.destructive_level
        ));
    }
    document.push_str("\n## Planned outputs\n\n```json\n");
    document.push_str(
        &serde_json::to_string_pretty(&plan.planned_outputs).expect("planned outputs are JSON"),
    );
    document.push_str("\n```\n");
    document
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), ReviewError> {
    let parent = path
        .parent()
        .ok_or_else(|| ReviewError::Io("artifact path has no parent".into()))?;
    let mut temporary = NamedTempFile::new_in(parent).map_err(io)?;
    temporary.write_all(bytes).map_err(io)?;
    temporary.as_file_mut().sync_all().map_err(io)?;
    temporary.persist(path).map_err(|error| io(error.error))?;
    sync_directory(parent)?;
    Ok(())
}

fn sync_directory(directory: &Path) -> Result<(), ReviewError> {
    fs::File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(io)
}

fn io(error: impl std::fmt::Display) -> ReviewError {
    ReviewError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::Action;
    use crate::plan::DestructiveLevel;
    use crate::plan::ExecutionClass;
    use crate::plan::Operation;
    use crate::plan::PlannedOperation;

    #[test]
    fn redacted_projection_does_not_contain_executable_sql() {
        let mut plan = ExecutablePlan {
            plan_id: String::new(),
            api_version: crate::plan::API_VERSION.into(),
            connector: crate::CONNECTOR_NAME.into(),
            connector_build: "test".into(),
            policy_version: crate::plan::POLICY_VERSION.into(),
            graph_id: "01".repeat(16),
            generation: "1".into(),
            slice_sequence: "2".into(),
            target: "local/henosis-local/postgres".into(),
            desired_digest: "blake3:desired".into(),
            observed_digest: "blake3:observed".into(),
            journal_tail: "3".into(),
            operations: vec![PlannedOperation {
                id: "sha256:operation".into(),
                address: "schema.catalog.migration.one".into(),
                component_scopes: vec!["07".repeat(32)],
                execution_class: ExecutionClass::OrderedTransition,
                action: Action::Transition,
                before: serde_json::Value::Null,
                after: serde_json::json!({"migrationId":"one","checksum":"sha256:checksum"}),
                destructive_level: DestructiveLevel::NonDestructive,
                dependencies: Vec::new(),
                preconditions: vec!["receipt absent".into()],
                operation: Operation::ApplyMigration {
                    resource_id: "catalog".into(),
                    schema: "catalog".into(),
                    migration: crate::context::Migration {
                        id: "one".into(),
                        checksum: "sha256:checksum".into(),
                        sql: "create table secret_shape (id bigint);".into(),
                        inputs: Vec::new(),
                    },
                    inputs: std::collections::BTreeMap::new(),
                },
            }],
            planned_outputs: Vec::new(),
            notices: Vec::new(),
        };
        plan.refresh_id();
        let root = tempfile::tempdir().unwrap();
        let artifacts = persist(root.path(), &plan).unwrap();
        let review = fs::read_to_string(artifacts.json_path).unwrap();
        let executable = fs::read_to_string(artifacts.plan_path).unwrap();
        assert!(!review.contains("secret_shape"));
        assert!(executable.contains("secret_shape"));
        assert!(review.contains(&plan.plan_id));
    }
}
