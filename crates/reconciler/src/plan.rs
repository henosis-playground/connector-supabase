//! Mutation-free complete-slice planning and immutable executable plans.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use henosis_proto::proto::henosis::v1::ComponentOutputs;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest as _;
use sha2::Sha256;

use crate::context::AnonAccess;
use crate::context::Migration;
use crate::journal::Binding;
use crate::journal::JournalSnapshot;
use crate::slice::DesiredSlice;
use crate::target::ObservedTarget;

/// Plan/review format discriminator.
pub const API_VERSION: &str = "henosis.dev/supabase-review-plan/v1";
/// Policy identity bound into every plan.
pub const POLICY_VERSION: &str = "supabase-local-v1";

/// Exact private executable payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecutablePlan {
    /// Content digest of the payload with this field omitted.
    pub plan_id: String,
    /// Format discriminator.
    pub api_version: String,
    /// Connector registry key.
    pub connector: String,
    /// Connector build identity.
    pub connector_build: String,
    /// Planning/apply policy identity.
    pub policy_version: String,
    /// Graph UUID as lowercase hexadecimal.
    pub graph_id: String,
    /// Desired generation as a string ID.
    pub generation: String,
    /// Exact slice sequence as a string ID.
    pub slice_sequence: String,
    /// Configured target identity.
    pub target: String,
    /// Complete desired BLAKE3 digest.
    pub desired_digest: String,
    /// Exact observed-target BLAKE3 digest.
    pub observed_digest: String,
    /// Required journal tail after `PlanCreated`.
    pub journal_tail: String,
    /// Complete ordered native operation proposal.
    pub operations: Vec<PlannedOperation>,
    /// Fully known outputs, with secrets represented only by references.
    pub planned_outputs: Vec<PlannedOutput>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PlanPayload<'a> {
    api_version: &'a str,
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
    operations: &'a [PlannedOperation],
    planned_outputs: &'a [PlannedOutput],
}

/// One operation in the complete proposal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PlannedOperation {
    /// Stable semantic operation identity.
    pub id: String,
    /// Stable logical/native address.
    pub address: String,
    /// Component spec hashes affected by the operation.
    pub component_scopes: Vec<String>,
    /// Reconciled object or ordered transition.
    pub execution_class: ExecutionClass,
    /// Review action.
    pub action: Action,
    /// Canonical redacted prior value.
    pub before: serde_json::Value,
    /// Canonical redacted proposed value.
    pub after: serde_json::Value,
    /// Safety classification.
    pub destructive_level: DestructiveLevel,
    /// Stable predecessor operation IDs.
    pub dependencies: Vec<String>,
    /// Exact operation-specific target preconditions.
    pub preconditions: Vec<String>,
    /// Private executable operation.
    pub operation: Operation,
}

/// Native execution class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionClass {
    /// Repeated observation and idempotent convergence.
    ReconciledObject,
    /// Immutable ordered SQL transition.
    OrderedTransition,
}

/// Semantic review action.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Action {
    /// Create a missing object/receipt.
    Create,
    /// Change reversible configuration.
    Update,
    /// Claim a stable existing remote identity.
    Bind,
    /// Apply one immutable ordered transition.
    Transition,
}

/// Review safety classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DestructiveLevel {
    /// No deletion or replacement is proposed.
    NonDestructive,
}

/// Private executable operation variants.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Operation {
    /// Persist one stable identity binding in S2.
    EstablishBinding {
        /// Binding to append.
        binding: Binding,
    },
    /// Create the connector's target-side migration receipt ledger.
    EnsureMetadata,
    /// Create one owned `PostgreSQL` schema.
    EnsureSchema {
        /// Stable resource identity.
        resource_id: String,
        /// Schema native identity.
        schema: String,
    },
    /// Apply one exact immutable SQL transition.
    ApplyMigration {
        /// Stable resource identity.
        resource_id: String,
        /// Schema search-path identity.
        schema: String,
        /// Exact migration payload.
        migration: Migration,
    },
    /// Converge `PostgREST` exposed schemas and anonymous grants.
    ConfigureApi {
        /// Complete exposed schema set, including unmanaged preserved values.
        exposed_schemas: Vec<String>,
        /// Desired policy for every connector-owned schema in this slice.
        policies: Vec<ApiPolicy>,
    },
}

/// One owned schema's API policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ApiPolicy {
    /// Schema native identity.
    pub schema: String,
    /// Whether `PostgREST` exposes the schema.
    pub expose: bool,
    /// Anonymous grant policy.
    pub anon_access: AnonAccess,
}

/// Redacted fully known output proposal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PlannedOutput {
    /// Producer spec hash.
    pub component_spec_hash: String,
    /// Canonical output JSON.
    pub values: serde_json::Value,
}

/// One plan-phase failure attached to its component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanDiagnostic {
    /// Producer spec hash.
    pub component_spec_hash: [u8; 32],
    /// Stable code.
    pub code: String,
    /// Canonical safe detail.
    pub message: String,
    /// RFC 6901 connector-context pointer.
    pub pointer: String,
    /// Actionable guidance.
    pub help: String,
}

/// Connector/runtime facts bound into a plan but not desired by authoring.
#[derive(Clone, Copy, Debug)]
pub struct PlanContext<'a> {
    /// Tail required immediately after appending `PlanCreated`.
    pub journal_tail_after_plan: u64,
    /// Connector build identity.
    pub connector_build: &'a str,
    /// Credential-free public API origin.
    pub api_url: &'a str,
    /// Database URL secret reference.
    pub database_url_ref: &'a str,
    /// Anonymous key secret reference.
    pub anon_key_ref: &'a str,
}

/// Validate desired content before any target observation or journal append.
pub fn validate_desired(desired: &DesiredSlice, journal: &JournalSnapshot) -> Vec<PlanDiagnostic> {
    let graph = hex::encode(desired.graph_id);
    let mut diagnostics = Vec::new();
    if let Some(binding) = journal
        .bindings
        .values()
        .find(|binding| binding.graph_id != graph)
    {
        diagnostics.push(PlanDiagnostic {
            component_spec_hash: desired
                .components
                .iter()
                .next()
                .map(|component| component.spec_hash)
                .unwrap_or([0; 32]),
            code: "supabase.plan.target-owned".into(),
            message: format!(
                "local Supabase target is already owned by graph {} through resource {:?}",
                binding.graph_id, binding.resource_id
            ),
            pointer: "/target".into(),
            help: "Retire/release the owning graph before assigning the exclusive v1 local target \
                   to another graph."
                .into(),
        });
    }
    for component in desired.components.iter() {
        diagnostics.extend(component.context.plan_issues().into_iter().map(|issue| {
            PlanDiagnostic {
                component_spec_hash: component.spec_hash,
                code: issue.code.into(),
                message: issue.message,
                pointer: issue.pointer,
                help: issue.help,
            }
        }));
        if let Some(binding) = journal.bindings.get(&component.context.resource_id)
            && (binding.graph_id != graph
                || binding.project != component.context.target.project
                || binding.database != component.context.target.database
                || binding.schema != component.context.target.schema)
        {
            diagnostics.push(PlanDiagnostic {
                component_spec_hash: component.spec_hash,
                code: "supabase.plan.binding-conflict".into(),
                message: format!(
                    "resourceId {:?} is already bound to {}/{}/{} for graph {}",
                    binding.resource_id,
                    binding.project,
                    binding.database,
                    binding.schema,
                    binding.graph_id
                ),
                pointer: "/resourceId".into(),
                help: "Keep resourceId and target schema stable across revisions; ownership moves \
                       require an explicit future handover operation."
                    .into(),
            });
        }
    }
    diagnostics
}

/// Produce a complete immutable plan from desired, journal, and target truth.
pub fn build_plan(
    desired: &DesiredSlice,
    journal: &JournalSnapshot,
    observed: &ObservedTarget,
    context: PlanContext<'_>,
) -> Result<ExecutablePlan, Vec<PlanDiagnostic>> {
    let mut diagnostics = Vec::new();
    let graph_id = hex::encode(desired.graph_id);
    let desired_digest = format!("blake3:{}", hex::encode(desired.desired_digest()));
    let observed_digest = format!("blake3:{}", hex::encode(observed.digest()));
    let mut operations = Vec::new();
    let mut last_by_resource = BTreeMap::<String, String>::new();

    for component in desired.components.iter() {
        let context = &component.context;
        let hash = hex::encode(component.spec_hash);
        let binding = Binding {
            graph_id: graph_id.clone(),
            resource_id: context.resource_id.clone(),
            project: context.target.project.clone(),
            database: context.target.database.clone(),
            schema: context.target.schema.clone(),
        };
        if !journal.bindings.contains_key(&context.resource_id) {
            let operation = Operation::EstablishBinding {
                binding: binding.clone(),
            };
            let id = operation_id(&desired_digest, &operation);
            operations.push(PlannedOperation {
                id: id.clone(),
                address: format!("binding.{}", context.resource_id),
                component_scopes: vec![hash.clone()],
                execution_class: ExecutionClass::ReconciledObject,
                action: Action::Bind,
                before: serde_json::Value::Null,
                after: serde_json::to_value(&binding).expect("binding is JSON"),
                destructive_level: DestructiveLevel::NonDestructive,
                dependencies: Vec::new(),
                preconditions: vec!["resourceId is unbound".into()],
                operation,
            });
            last_by_resource.insert(context.resource_id.clone(), id);
        }
    }

    if !observed.metadata_exists {
        let operation = Operation::EnsureMetadata;
        let id = operation_id(&desired_digest, &operation);
        operations.push(PlannedOperation {
            id: id.clone(),
            address: "database.henosis_connector.migration_receipts".into(),
            component_scopes: desired
                .components
                .iter()
                .map(|component| hex::encode(component.spec_hash))
                .collect(),
            execution_class: ExecutionClass::ReconciledObject,
            action: Action::Create,
            before: serde_json::Value::Null,
            after: serde_json::json!({"schema":"henosis_connector","table":"migration_receipts"}),
            destructive_level: DestructiveLevel::NonDestructive,
            dependencies: operations
                .iter()
                .filter(|operation| {
                    matches!(operation.operation, Operation::EstablishBinding { .. })
                })
                .map(|operation| operation.id.clone())
                .collect(),
            preconditions: vec!["receipt ledger does not exist".into()],
            operation,
        });
        for component in desired.components.iter() {
            last_by_resource.insert(component.context.resource_id.clone(), id.clone());
        }
    }

    for component in desired.components.iter() {
        let context = &component.context;
        let hash = hex::encode(component.spec_hash);
        if !observed.schemas.contains(&context.target.schema) {
            let operation = Operation::EnsureSchema {
                resource_id: context.resource_id.clone(),
                schema: context.target.schema.clone(),
            };
            let id = operation_id(&desired_digest, &operation);
            operations.push(PlannedOperation {
                id: id.clone(),
                address: format!("schema.{}", context.target.schema),
                component_scopes: vec![hash.clone()],
                execution_class: ExecutionClass::ReconciledObject,
                action: Action::Create,
                before: serde_json::Value::Null,
                after: serde_json::json!({"schema":context.target.schema}),
                destructive_level: DestructiveLevel::NonDestructive,
                dependencies: last_by_resource
                    .get(&context.resource_id)
                    .cloned()
                    .into_iter()
                    .collect(),
                preconditions: vec!["schema does not exist".into()],
                operation,
            });
            last_by_resource.insert(context.resource_id.clone(), id);
        }

        for migration in &context.migrations {
            match observed.migration_checksum(&context.resource_id, &migration.id) {
                Some(checksum) if checksum == migration.checksum => continue,
                Some(checksum) => {
                    diagnostics.push(PlanDiagnostic {
                        component_spec_hash: component.spec_hash,
                        code: "supabase.plan.migration-mutated".into(),
                        message: format!(
                            "migration {:?} was applied with checksum {checksum:?}, but desired \
                             declares {:?}",
                            migration.id, migration.checksum
                        ),
                        pointer: format!(
                            "/migrations/{}/checksum",
                            context
                                .migrations
                                .iter()
                                .position(|candidate| candidate.id == migration.id)
                                .unwrap_or_default()
                        ),
                        help: "Never edit an applied migration ID; append a new corrective \
                               migration."
                            .into(),
                    });
                    continue;
                }
                None => {}
            }
            let operation = Operation::ApplyMigration {
                resource_id: context.resource_id.clone(),
                schema: context.target.schema.clone(),
                migration: migration.clone(),
            };
            let id = operation_id(&desired_digest, &operation);
            operations.push(PlannedOperation {
                id: id.clone(),
                address: format!(
                    "schema.{}.migration.{}",
                    context.target.schema, migration.id
                ),
                component_scopes: vec![hash.clone()],
                execution_class: ExecutionClass::OrderedTransition,
                action: Action::Transition,
                before: serde_json::Value::Null,
                after: serde_json::json!({
                    "migrationId":migration.id,
                    "checksum":migration.checksum,
                }),
                destructive_level: DestructiveLevel::NonDestructive,
                dependencies: last_by_resource
                    .get(&context.resource_id)
                    .cloned()
                    .into_iter()
                    .collect(),
                preconditions: vec![format!("migration receipt {:?} is absent", migration.id)],
                operation,
            });
            last_by_resource.insert(context.resource_id.clone(), id);
        }
    }

    let owned_schemas = desired
        .components
        .iter()
        .map(|component| component.context.target.schema.clone())
        .collect::<BTreeSet<_>>();
    let mut exposed = observed
        .exposed_schemas
        .iter()
        .filter(|schema| {
            !journal
                .bindings
                .values()
                .any(|binding| &binding.schema == *schema)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    exposed.insert("public".into());
    let policies = desired
        .components
        .iter()
        .map(|component| {
            if component.context.api.expose {
                exposed.insert(component.context.target.schema.clone());
            }
            ApiPolicy {
                schema: component.context.target.schema.clone(),
                expose: component.context.api.expose,
                anon_access: component.context.api.anon_access,
            }
        })
        .collect::<Vec<_>>();
    let grants_match = policies.iter().all(|policy| {
        observed
            .anon_read
            .get(&policy.schema)
            .is_some_and(|actual| {
                *actual == (policy.expose && policy.anon_access == AnonAccess::Read)
            })
    });
    if exposed != observed.exposed_schemas || !grants_match {
        let exposed_schemas = exposed.into_iter().collect::<Vec<_>>();
        let operation = Operation::ConfigureApi {
            exposed_schemas: exposed_schemas.clone(),
            policies,
        };
        let id = operation_id(&desired_digest, &operation);
        operations.push(PlannedOperation {
            id,
            address: "postgrest.exposed-schemas".into(),
            component_scopes: desired
                .components
                .iter()
                .map(|component| hex::encode(component.spec_hash))
                .collect(),
            execution_class: ExecutionClass::ReconciledObject,
            action: Action::Update,
            before: serde_json::json!({
                "exposedSchemas":observed.exposed_schemas,
                "anonRead":observed.anon_read,
            }),
            after: serde_json::json!({
                "exposedSchemas":exposed_schemas,
                "anonReadSchemas":owned_schemas,
            }),
            destructive_level: DestructiveLevel::NonDestructive,
            dependencies: last_by_resource.values().cloned().collect(),
            preconditions: vec!["PostgREST configuration matches observed digest".into()],
            operation,
        });
    }

    if !diagnostics.is_empty() {
        return Err(diagnostics);
    }

    let base = context.api_url.trim_end_matches('/');
    let planned_outputs = desired
        .components
        .iter()
        .map(|component| PlannedOutput {
            component_spec_hash: hex::encode(component.spec_hash),
            values: serde_json::json!({
                "project":component.context.target.project,
                "database":component.context.target.database,
                "schema":component.context.target.schema,
                "apiUrl":base,
                "restUrl":format!("{base}/rest/v1"),
                "databaseUrlRef":context.database_url_ref,
                "anonKeyRef":context.anon_key_ref,
            }),
        })
        .collect::<Vec<_>>();
    let mut plan = ExecutablePlan {
        plan_id: String::new(),
        api_version: API_VERSION.into(),
        connector: crate::CONNECTOR_NAME.into(),
        connector_build: context.connector_build.into(),
        policy_version: POLICY_VERSION.into(),
        graph_id,
        generation: desired.generation.to_string(),
        slice_sequence: desired.sequence.to_string(),
        target: "local/henosis-local/postgres".into(),
        desired_digest,
        observed_digest,
        journal_tail: context.journal_tail_after_plan.to_string(),
        operations,
        planned_outputs,
    };
    plan.refresh_id();
    Ok(plan)
}

impl ExecutablePlan {
    fn payload(&self) -> PlanPayload<'_> {
        PlanPayload {
            api_version: &self.api_version,
            connector: &self.connector,
            connector_build: &self.connector_build,
            policy_version: &self.policy_version,
            graph_id: &self.graph_id,
            generation: &self.generation,
            slice_sequence: &self.slice_sequence,
            target: &self.target,
            desired_digest: &self.desired_digest,
            observed_digest: &self.observed_digest,
            journal_tail: &self.journal_tail,
            operations: &self.operations,
            planned_outputs: &self.planned_outputs,
        }
    }

    /// Recompute and verify the payload digest before apply.
    pub fn verify_digest(&self) -> bool {
        let bytes = serde_json::to_vec(&self.payload()).expect("plan payload is JSON");
        self.plan_id == format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
    }

    pub(crate) fn refresh_id(&mut self) {
        let bytes = serde_json::to_vec(&self.payload()).expect("plan payload is JSON");
        self.plan_id = format!("sha256:{}", hex::encode(Sha256::digest(bytes)));
    }

    /// Convert fully known planned outputs to shared-contract output values.
    pub fn contract_outputs(&self) -> Vec<ComponentOutputs> {
        self.planned_outputs
            .iter()
            .map(|output| {
                ComponentOutputs::default()
                    .with_component_spec_hash(
                        hex::decode(&output.component_spec_hash)
                            .expect("planned component hash is canonical"),
                    )
                    .with_values_json(
                        serde_json::to_vec(&output.values).expect("planned output is JSON"),
                    )
            })
            .collect()
    }
}

fn operation_id(desired_digest: &str, operation: &Operation) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"henosis.dev/supabase-operation/v1\0");
    hasher.update(desired_digest.as_bytes());
    hasher.update(
        serde_json::to_vec(operation).expect("operation payload has deterministic JSON fields"),
    );
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use iddqd::IdOrdMap;

    use super::*;
    use crate::context::ApiContext;
    use crate::context::ComponentContext;
    use crate::context::TargetContext;
    use crate::slice::ComponentPin;
    use crate::target::DatabaseIdentity;

    fn desired() -> DesiredSlice {
        let sql = "create table items (id bigint primary key);".to_owned();
        let migration = Migration {
            id: "202607130001_create_items".into(),
            checksum: format!("sha256:{}", hex::encode(Sha256::digest(sql.as_bytes()))),
            sql,
        };
        let mut components = IdOrdMap::new();
        components
            .insert_unique(ComponentPin {
                spec_hash: [7; 32],
                name: "catalog".into(),
                context: ComponentContext {
                    api_version: crate::context::API_VERSION.into(),
                    resource_id: "catalog".into(),
                    target: TargetContext {
                        stack: "local".into(),
                        project: "henosis-local".into(),
                        database: "postgres".into(),
                        schema: "catalog".into(),
                    },
                    migrations: vec![migration],
                    api: ApiContext {
                        expose: true,
                        anon_access: AnonAccess::Read,
                    },
                },
            })
            .unwrap();
        DesiredSlice {
            graph_id: [3; 16],
            generation: 4,
            sequence: 9,
            components,
            upstream_outputs: IdOrdMap::new(),
        }
    }

    fn observed() -> ObservedTarget {
        ObservedTarget {
            identity: DatabaseIdentity {
                database: "postgres".into(),
                database_oid: "5".into(),
                server_version: "170006".into(),
            },
            metadata_exists: false,
            schemas: BTreeSet::new(),
            migrations: Vec::new(),
            exposed_schemas: BTreeSet::from(["public".into()]),
            anon_read: BTreeMap::from([("catalog".into(), false)]),
        }
    }

    #[test]
    fn complete_plan_begins_with_binding_and_binds_every_freshness_identity() {
        let desired = desired();
        let plan = build_plan(
            &desired,
            &JournalSnapshot::default(),
            &observed(),
            PlanContext {
                journal_tail_after_plan: 1,
                connector_build: "test-build",
                api_url: "http://localhost:4484",
                database_url_ref: "docker-secret://db",
                anon_key_ref: "docker-secret://anon",
            },
        )
        .unwrap();
        assert!(matches!(
            plan.operations[0].operation,
            Operation::EstablishBinding { .. }
        ));
        assert_eq!(plan.graph_id, hex::encode([3; 16]));
        assert_eq!(plan.generation, "4");
        assert_eq!(plan.slice_sequence, "9");
        assert_eq!(plan.journal_tail, "1");
        assert_eq!(plan.connector_build, "test-build");
        assert!(plan.verify_digest());
        assert_eq!(
            plan.planned_outputs[0].values["anonKeyRef"],
            "docker-secret://anon"
        );
    }

    #[test]
    fn executable_digest_detects_payload_substitution() {
        let desired = desired();
        let mut plan = build_plan(
            &desired,
            &JournalSnapshot::default(),
            &observed(),
            PlanContext {
                journal_tail_after_plan: 1,
                connector_build: "test-build",
                api_url: "http://localhost:4484",
                database_url_ref: "docker-secret://db",
                anon_key_ref: "docker-secret://anon",
            },
        )
        .unwrap();
        plan.observed_digest = "blake3:substituted".into();
        assert!(!plan.verify_digest());
    }
}
