//! Supabase target lifecycle implemented against the shared connector SDK.

use std::time::Duration;

use connector_sdk::ApplyOutcome;
use connector_sdk::Approved;
use connector_sdk::ConcurrencyScope;
use connector_sdk::Connector;
use connector_sdk::ContractError;
use connector_sdk::Diagnostic;
use connector_sdk::PassContext;
use connector_sdk::PlanOutcome;
use connector_sdk::PlanProposal;
use connector_sdk::Publication;
use connector_sdk::RetireContext;
use connector_sdk::RetireOutcome;
use connector_sdk::Retry;
use connector_sdk::TargetSlice;
use faultline::Error as Fault;

use crate::journal::JournalError;
use crate::journal::JournalEvent;
use crate::journal::JournalSnapshot;
use crate::journal::OperationJournal;
use crate::journal::OperationState;
use crate::plan::ExecutablePlan;
use crate::plan::Operation;
use crate::plan::PlanContext;
use crate::plan::PlanDiagnostic;
use crate::plan::PlannedOperation;
use crate::plan::build_plan;
use crate::plan::validate_desired;
use crate::review;
use crate::slice::DesiredSlice;
use crate::target::ApplyResult;
use crate::target::ObservedTarget;
use crate::target::Target;
use crate::target::TargetError;

/// Target lifecycle configuration not owned by the SDK.
#[derive(Clone, Debug)]
pub struct ConnectorConfig {
    /// Connector build identity bound into private plans.
    pub connector_build: String,
}

/// Fresh target and operation-journal truth used by one plan.
pub struct Observation {
    journal: JournalSnapshot,
    target: ObservedTarget,
}

/// Supabase-specific plan/apply/observe implementation.
pub struct SupabaseConnector {
    config: ConnectorConfig,
    target: Target,
    journal: OperationJournal,
}

impl SupabaseConnector {
    /// Construct target lifecycle hooks around explicit native adapters.
    pub fn new(config: ConnectorConfig, target: Target, journal: OperationJournal) -> Self {
        Self {
            config,
            target,
            journal,
        }
    }
}

#[async_trait::async_trait]
impl Connector for SupabaseConnector {
    type Desired = DesiredSlice;
    type Observation = Observation;
    type Plan = ExecutablePlan;

    fn name(&self) -> &'static str {
        crate::CONNECTOR_NAME
    }

    fn decode(&self, slice: &TargetSlice) -> Result<Self::Desired, ContractError> {
        DesiredSlice::decode(slice)
    }

    fn concurrency_scope(&self, _desired: &Self::Desired) -> ConcurrencyScope {
        // The local v1 stack is one native database/PostgREST configuration.
        ConcurrencyScope::Connector
    }

    async fn observe(
        &self,
        _context: PassContext<'_>,
        desired: &Self::Desired,
    ) -> Result<Self::Observation, PlanOutcome<Self::Plan>> {
        let journal = load_for_plan(&self.journal).await?;
        let diagnostics = validate_desired(desired, &journal);
        if !diagnostics.is_empty() {
            return Err(PlanOutcome::Failed(plan_diagnostics(diagnostics)));
        }
        let target = self
            .target
            .observe(desired)
            .await
            .map_err(|error| PlanOutcome::Waiting {
                diagnostics: vec![
                    Diagnostic::warning("supabase.target.observe", error.to_string())
                        .help("The connector will retry observation without applying anything."),
                ],
                retry: Retry::after(Duration::from_secs(2)),
            })?;
        Ok(Observation { journal, target })
    }

    async fn plan(
        &self,
        _context: PassContext<'_>,
        desired: &Self::Desired,
        observed: &Self::Observation,
    ) -> PlanOutcome<Self::Plan> {
        let plan = match build_plan(
            desired,
            &observed.journal,
            &observed.target,
            PlanContext {
                journal_tail_after_plan: observed.journal.tail.saturating_add(1),
                connector_build: &self.config.connector_build,
                api_url: self.target.api_url(),
                database_url_ref: self.target.database_url_ref(),
                anon_key_ref: self.target.anon_key_ref(),
            },
        ) {
            Ok(plan) => plan,
            Err(diagnostics) => return PlanOutcome::Failed(plan_diagnostics(diagnostics)),
        };
        if plan.operations.is_empty() {
            return PlanOutcome::Ready {
                outputs: plan.outputs(),
                diagnostics: Vec::new(),
                publication: Some(Publication {
                    revision: plan.observed_digest,
                    uri: self.journal.evidence_uri(observed.journal.tail),
                }),
            };
        }
        let event = JournalEvent::PlanCreated {
            plan_id: plan.plan_id.clone(),
            graph_id: plan.graph_id.clone(),
            generation: plan.generation.clone(),
            slice_sequence: plan.slice_sequence.clone(),
            desired_digest: plan.desired_digest.clone(),
            observed_digest: plan.observed_digest.clone(),
        };
        match self.journal.append(observed.journal.tail, event).await {
            Ok(tail) if tail.to_string() == plan.journal_tail => PlanOutcome::Apply(PlanProposal {
                review: review::project(&plan),
                plan,
            }),
            Ok(_) => PlanOutcome::Failed(vec![Diagnostic::error(
                "supabase.journal.plan-fence",
                "PlanCreated tail differs from the executable freshness fence",
            )]),
            Err(Fault::Domain(JournalError::CasConflict { .. })) => PlanOutcome::Waiting {
                diagnostics: vec![Diagnostic::info(
                    "supabase.plan.stale",
                    "operation journal advanced while the plan was being persisted",
                )],
                retry: Retry::immediate(),
            },
            Err(Fault::Transient(error)) => PlanOutcome::Waiting {
                diagnostics: vec![Diagnostic::warning(
                    "supabase.journal.unavailable",
                    error.to_string(),
                )],
                retry: Retry::after(Duration::from_secs(2)),
            },
            Err(Fault::Invariant(error)) => PlanOutcome::Failed(vec![Diagnostic::error(
                "supabase.journal.invariant",
                error.to_string(),
            )]),
        }
    }

    async fn apply(
        &self,
        _context: PassContext<'_>,
        desired: &Self::Desired,
        approved: &Approved<Self::Plan>,
    ) -> ApplyOutcome {
        let plan = approved.plan();
        if !plan.verify_digest() || !plan_matches(plan, desired) {
            return ApplyOutcome::Failed(vec![Diagnostic::error(
                "supabase.plan.identity",
                "persisted executable plan does not describe the current desired level",
            )]);
        }
        let journal = match load_for_apply(&self.journal).await {
            Ok(snapshot) => snapshot,
            Err(outcome) => return outcome,
        };
        let Some(operation) = plan.operations.first() else {
            return ApplyOutcome::Failed(vec![Diagnostic::error(
                "supabase.plan.empty",
                "approved executable plan has no operation",
            )]);
        };
        let expected_tail = match plan.journal_tail.parse::<u64>() {
            Ok(tail) => tail,
            Err(error) => {
                return ApplyOutcome::Failed(vec![Diagnostic::error(
                    "supabase.plan.journal-tail",
                    error.to_string(),
                )]);
            }
        };
        let operation_state = journal.operations.get(&operation.id).cloned();
        if let Some(OperationState::Succeeded { .. }) = operation_state {
            return ApplyOutcome::Progress(vec![operation_complete(operation, plan, true)]);
        }
        if let Some(OperationState::Failed { code, detail, .. }) = &operation_state {
            return ApplyOutcome::Failed(vec![Diagnostic::error(code, detail)]);
        }
        let interrupted = matches!(
            operation_state,
            Some(OperationState::Started { ref plan_id }) if plan_id == &plan.plan_id
        );
        if journal.tail != expected_tail && !interrupted {
            if let Err(outcome) = append_for_apply(
                &self.journal,
                journal.tail,
                JournalEvent::PlanStale {
                    plan_id: plan.plan_id.clone(),
                    reason: "journal-fence-changed".into(),
                },
            )
            .await
            {
                return outcome;
            }
            return ApplyOutcome::Stale(vec![Diagnostic::info(
                "supabase.plan.stale",
                "operation journal advanced after this plan was reviewed; no target mutation was \
                 attempted",
            )]);
        }

        if let Operation::EstablishBinding { binding } = &operation.operation {
            let mut tail = journal.tail;
            if !interrupted {
                tail = match append_started(&self.journal, tail, plan, operation).await {
                    Ok(tail) => tail,
                    Err(outcome) => return outcome,
                };
            }
            tail = match append_for_apply(
                &self.journal,
                tail,
                JournalEvent::BindingEstablished {
                    binding: binding.clone(),
                },
            )
            .await
            {
                Ok(tail) => tail,
                Err(outcome) => return outcome,
            };
            if let Err(outcome) = append_succeeded(
                &self.journal,
                tail,
                plan,
                operation,
                &plan.observed_digest,
                interrupted,
            )
            .await
            {
                return outcome;
            }
            return ApplyOutcome::Progress(vec![operation_complete(operation, plan, interrupted)]);
        }

        let current = match self.target.observe(desired).await {
            Ok(current) => current,
            Err(error) => return target_waiting(error),
        };
        if interrupted
            && self
                .target
                .operation_satisfied(&operation.operation, &current)
        {
            let digest = format!("blake3:{}", hex::encode(current.digest()));
            if let Err(outcome) =
                append_succeeded(&self.journal, journal.tail, plan, operation, &digest, true).await
            {
                return outcome;
            }
            return ApplyOutcome::Progress(vec![operation_complete(operation, plan, true)]);
        }

        let mut tail = journal.tail;
        if !interrupted {
            tail = match append_started(&self.journal, tail, plan, operation).await {
                Ok(tail) => tail,
                Err(outcome) => return outcome,
            };
        }
        match self.target.apply(desired, plan, operation).await {
            Ok(ApplyResult::Applied { observed_digest }) => {
                if let Err(outcome) = append_succeeded(
                    &self.journal,
                    tail,
                    plan,
                    operation,
                    &observed_digest,
                    interrupted,
                )
                .await
                {
                    return outcome;
                }
                ApplyOutcome::Progress(vec![operation_complete(operation, plan, interrupted)])
            }
            Ok(ApplyResult::Stale { observed_digest }) => {
                let snapshot = match load_for_apply(&self.journal).await {
                    Ok(snapshot) => snapshot,
                    Err(outcome) => return outcome,
                };
                if let Err(outcome) = append_for_apply(
                    &self.journal,
                    snapshot.tail,
                    JournalEvent::PlanStale {
                        plan_id: plan.plan_id.clone(),
                        reason: format!("target-changed:{observed_digest}"),
                    },
                )
                .await
                {
                    return outcome;
                }
                ApplyOutcome::Stale(vec![
                    Diagnostic::info(
                        "supabase.plan.stale",
                        "target observation changed before apply; the exact reviewed plan was \
                         rejected",
                    )
                    .help("A new plan will be derived from current target truth."),
                ])
            }
            Err(TargetError::Provider(provider)) => {
                let snapshot = match load_for_apply(&self.journal).await {
                    Ok(snapshot) => snapshot,
                    Err(outcome) => return outcome,
                };
                if let Err(outcome) = append_for_apply(
                    &self.journal,
                    snapshot.tail,
                    JournalEvent::OperationFailed {
                        plan_id: plan.plan_id.clone(),
                        operation_id: operation.id.clone(),
                        code: provider.code.clone(),
                        detail: provider.detail.clone(),
                    },
                )
                .await
                {
                    return outcome;
                }
                let mut diagnostic = Diagnostic::error(provider.code, provider.detail);
                if let Some(hash) = operation
                    .component_scopes
                    .first()
                    .and_then(|hash| hex::decode(hash).ok())
                    .and_then(|hash| hash.try_into().ok())
                {
                    diagnostic = diagnostic.component(hash);
                }
                if let Some(help) = provider.help {
                    diagnostic = diagnostic.help(help);
                }
                ApplyOutcome::Failed(vec![diagnostic])
            }
            Err(TargetError::Unavailable(detail)) => {
                target_waiting(TargetError::Unavailable(detail))
            }
        }
    }

    async fn retire(
        &self,
        context: RetireContext<'_>,
        desired: Option<&Self::Desired>,
    ) -> RetireOutcome {
        if desired.is_none() {
            return RetireOutcome::Blocked(vec![Diagnostic::error(
                "supabase.retire.unknown",
                "cannot release ownership for an unknown graph",
            )]);
        }
        let snapshot = match self.journal.load().await {
            Ok(snapshot) => snapshot,
            Err(Fault::Transient(_)) => {
                return RetireOutcome::Waiting(Retry::after(Duration::from_secs(2)));
            }
            Err(Fault::Invariant(error)) => {
                return RetireOutcome::Blocked(vec![Diagnostic::error(
                    "supabase.journal.invariant",
                    error.to_string(),
                )]);
            }
        };
        match self
            .journal
            .append(
                snapshot.tail,
                JournalEvent::GraphReleased {
                    graph_id: hex::encode(context.graph_id),
                },
            )
            .await
        {
            Ok(_) => RetireOutcome::Orphaned,
            Err(Fault::Domain(JournalError::CasConflict { .. })) => {
                RetireOutcome::Waiting(Retry::immediate())
            }
            Err(Fault::Transient(_)) => {
                RetireOutcome::Waiting(Retry::after(Duration::from_secs(2)))
            }
            Err(Fault::Invariant(error)) => RetireOutcome::Blocked(vec![Diagnostic::error(
                "supabase.journal.invariant",
                error.to_string(),
            )]),
        }
    }
}

async fn load_for_plan(
    journal: &OperationJournal,
) -> Result<JournalSnapshot, PlanOutcome<ExecutablePlan>> {
    match journal.load().await {
        Ok(snapshot) => Ok(snapshot),
        Err(Fault::Transient(error)) => Err(PlanOutcome::Waiting {
            diagnostics: vec![Diagnostic::warning(
                "supabase.journal.unavailable",
                error.to_string(),
            )],
            retry: Retry::after(Duration::from_secs(2)),
        }),
        Err(Fault::Invariant(error)) => Err(PlanOutcome::Failed(vec![Diagnostic::error(
            "supabase.journal.invariant",
            error.to_string(),
        )])),
    }
}

async fn load_for_apply(journal: &OperationJournal) -> Result<JournalSnapshot, ApplyOutcome> {
    match journal.load().await {
        Ok(snapshot) => Ok(snapshot),
        Err(Fault::Transient(error)) => Err(ApplyOutcome::Waiting {
            diagnostics: vec![Diagnostic::warning(
                "supabase.journal.unavailable",
                error.to_string(),
            )],
            retry: Retry::after(Duration::from_secs(2)),
        }),
        Err(Fault::Invariant(error)) => Err(ApplyOutcome::Failed(vec![Diagnostic::error(
            "supabase.journal.invariant",
            error.to_string(),
        )])),
    }
}

async fn append_for_apply(
    journal: &OperationJournal,
    tail: u64,
    event: JournalEvent,
) -> Result<u64, ApplyOutcome> {
    match journal.append(tail, event).await {
        Ok(tail) => Ok(tail),
        Err(Fault::Domain(JournalError::CasConflict { .. })) => {
            Err(ApplyOutcome::Stale(vec![Diagnostic::info(
                "supabase.plan.stale",
                "operation journal advanced before the receipt append",
            )]))
        }
        Err(Fault::Transient(error)) => Err(ApplyOutcome::Waiting {
            diagnostics: vec![Diagnostic::warning(
                "supabase.journal.unavailable",
                error.to_string(),
            )],
            retry: Retry::after(Duration::from_secs(2)),
        }),
        Err(Fault::Invariant(error)) => Err(ApplyOutcome::Failed(vec![Diagnostic::error(
            "supabase.journal.invariant",
            error.to_string(),
        )])),
    }
}

async fn append_started(
    journal: &OperationJournal,
    tail: u64,
    plan: &ExecutablePlan,
    operation: &PlannedOperation,
) -> Result<u64, ApplyOutcome> {
    append_for_apply(
        journal,
        tail,
        JournalEvent::OperationStarted {
            plan_id: plan.plan_id.clone(),
            operation_id: operation.id.clone(),
            idempotency_key: operation.id.clone(),
        },
    )
    .await
}

async fn append_succeeded(
    journal: &OperationJournal,
    tail: u64,
    plan: &ExecutablePlan,
    operation: &PlannedOperation,
    observed_digest: &str,
    recovered: bool,
) -> Result<u64, ApplyOutcome> {
    append_for_apply(
        journal,
        tail,
        JournalEvent::OperationSucceeded {
            plan_id: plan.plan_id.clone(),
            operation_id: operation.id.clone(),
            observed_digest: observed_digest.into(),
            recovered,
        },
    )
    .await
}

fn plan_matches(plan: &ExecutablePlan, desired: &DesiredSlice) -> bool {
    plan.graph_id == hex::encode(desired.graph_id)
        && plan.generation == desired.generation.to_string()
        && plan.slice_sequence == desired.sequence.to_string()
        && plan.desired_digest == format!("blake3:{}", hex::encode(desired.desired_digest()))
}

fn plan_diagnostics(diagnostics: Vec<PlanDiagnostic>) -> Vec<Diagnostic> {
    diagnostics
        .into_iter()
        .map(|item| {
            Diagnostic::error(item.code, item.message)
                .component(item.component_spec_hash)
                .pointer(item.pointer)
                .help(item.help)
        })
        .collect()
}

fn target_waiting(error: TargetError) -> ApplyOutcome {
    ApplyOutcome::Waiting {
        diagnostics: vec![
            Diagnostic::warning("supabase.target.unavailable", error.to_string())
                .help("The connector will retry from current target truth."),
        ],
        retry: Retry::after(Duration::from_secs(2)),
    }
}

fn operation_complete(
    operation: &PlannedOperation,
    plan: &ExecutablePlan,
    recovered: bool,
) -> Diagnostic {
    Diagnostic::info(
        "supabase.apply.operation-complete",
        format!(
            "operation {} ({}) from plan {} reached its postcondition{}",
            operation.id,
            operation.address,
            plan.plan_id,
            if recovered { " after recovery" } else { "" }
        ),
    )
    .help("The next pass re-observes and replans the remaining level.")
}
