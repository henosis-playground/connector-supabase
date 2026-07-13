//! Redacted review projection for an SDK-persisted private plan.

use connector_sdk::ReviewProjection;
use serde::Serialize;

use crate::plan::ExecutablePlan;
use crate::plan::PlannedOperation;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MachineProjection<'a> {
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

/// Build safe machine and human views. The SDK atomically persists these next
/// to its integrity-checked private executable plan.
pub fn project(plan: &ExecutablePlan) -> ReviewProjection {
    let machine = MachineProjection {
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
    };
    ReviewProjection {
        json: serde_json::to_value(machine).expect("review projection is JSON"),
        markdown: markdown(plan),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Migration;
    use crate::plan::Action;
    use crate::plan::DestructiveLevel;
    use crate::plan::ExecutionClass;
    use crate::plan::Operation;

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
                    migration: Migration {
                        id: "one".into(),
                        checksum: "sha256:checksum".into(),
                        sql: "create table secret_shape (id bigint);".into(),
                    },
                },
            }],
            planned_outputs: Vec::new(),
        };
        plan.refresh_id();
        let review = project(&plan);
        assert!(!review.json.to_string().contains("secret_shape"));
        assert!(
            serde_json::to_string(&plan)
                .unwrap()
                .contains("secret_shape")
        );
        assert!(review.json.to_string().contains(&plan.plan_id));
    }
}
