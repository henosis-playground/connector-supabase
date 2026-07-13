//! Durable level-triggered, one-operation-per-pass reconciliation.

use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use connectrpc::client::ClientConfig;
use connectrpc::client::HttpClient;
use faultline::Error as Fault;
use henosis_proto::connect::henosis::v1::ConnectorCallbackServiceClient;
use henosis_proto::proto::henosis::v1::ComponentDisposition;
use henosis_proto::proto::henosis::v1::ComponentDispositionKind;
use henosis_proto::proto::henosis::v1::Diagnostic;
use henosis_proto::proto::henosis::v1::DiagnosticSeverity;
use henosis_proto::proto::henosis::v1::FetchSliceRequest;
use henosis_proto::proto::henosis::v1::GraphSlice;
use henosis_proto::proto::henosis::v1::PublicationEvidence;
use henosis_proto::proto::henosis::v1::ReconcileSliceRequestView;
use henosis_proto::proto::henosis::v1::ReportSliceRequest;
use henosis_proto::proto::henosis::v1::SliceReport;
use http::Uri;
use serde::Deserialize;
use serde::Serialize;
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::Instrument as _;
use tracing::Level;
use tracing::Span;
use uuid::Uuid;

use crate::journal::JournalEvent;
use crate::journal::JournalSnapshot;
use crate::journal::OperationJournal;
use crate::journal::OperationState;
use crate::plan::ExecutablePlan;
use crate::plan::Operation;
use crate::plan::PlanContext;
use crate::plan::PlanDiagnostic;
use crate::plan::build_plan;
use crate::plan::validate_desired;
use crate::review;
use crate::slice::DesiredSlice;
use crate::slice::SliceError;
use crate::target::ApplyResult;
use crate::target::Target;
use crate::target::TargetError;

const PUBLICATION_NAMESPACE: Uuid = Uuid::from_bytes([
    0xbc, 0xd8, 0x7a, 0x63, 0xca, 0xd4, 0x56, 0x14, 0x88, 0x26, 0x38, 0xdb, 0x23, 0x78, 0xe9, 0x27,
]);

/// Atomic report and exact-slice recovery boundary.
pub trait Reporter: Send + Sync + 'static {
    /// Deliver one complete report level.
    fn report(
        &self,
        request: ReportSliceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), ReportError>> + Send + '_>>;

    /// Recover one exact accepted materialization after restart.
    fn fetch_slice(
        &self,
        graph_id: [u8; 16],
        sequence: u64,
    ) -> Pin<Box<dyn Future<Output = Result<GraphSlice, ReportError>> + Send + '_>>;
}

/// Core callback transport failure.
#[derive(Debug, Error)]
#[error("core callback failed: {0}")]
pub struct ReportError(String);

/// Real generated-contract callback client.
#[derive(Clone)]
pub struct CoreReporter {
    client: ConnectorCallbackServiceClient<HttpClient>,
}

impl CoreReporter {
    /// Build a plaintext compose-network client.
    pub fn new(uri: Uri, token: Option<String>) -> Self {
        let mut config = ClientConfig::new(uri);
        if let Some(token) = token {
            config = config.with_default_header("authorization", format!("Bearer {token}"));
        }
        Self {
            client: ConnectorCallbackServiceClient::new(HttpClient::plaintext(), config),
        }
    }
}

impl Reporter for CoreReporter {
    fn report(
        &self,
        request: ReportSliceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), ReportError>> + Send + '_>> {
        Box::pin(async move {
            self.client
                .report_slice(request)
                .await
                .map(|_| ())
                .map_err(|error| ReportError(error.to_string()))
        })
    }

    fn fetch_slice(
        &self,
        graph_id: [u8; 16],
        sequence: u64,
    ) -> Pin<Box<dyn Future<Output = Result<GraphSlice, ReportError>> + Send + '_>> {
        Box::pin(async move {
            self.client
                .fetch_slice(FetchSliceRequest {
                    graph_id: Some(graph_id.to_vec()),
                    connector: Some(crate::CONNECTOR_NAME.into()),
                    sequence: Some(sequence),
                    ..Default::default()
                })
                .await
                .map_err(|error| ReportError(error.to_string()))?
                .into_owned()
                .slice
                .into_option()
                .ok_or_else(|| ReportError("core omitted recovered slice".into()))
        })
    }
}

/// Persistent checkpoint and review-artifact layout.
#[derive(Clone, Debug)]
pub struct ReconcilerConfig {
    /// Connector state root.
    pub state_dir: PathBuf,
    /// Connector build identity bound into plans.
    pub connector_build: String,
}

/// Acceptance/reconciliation failure at the service boundary.
#[derive(Debug, Error)]
pub enum ReconcileError {
    /// Caller supplied an invalid shared slice/context.
    #[error("invalid slice: {0}")]
    Invalid(#[from] SliceError),
    /// Graph was terminally retired.
    #[error("graph is retired")]
    Retired,
    /// Equal sequence was reused for different desired contents.
    #[error("slice sequence {0} was already accepted with different contents")]
    SequenceConflict(u64),
    /// Durable local or S2 operation state failed.
    #[error("connector state failure: {0}")]
    State(String),
    /// Core callback failed.
    #[error(transparent)]
    Report(#[from] ReportError),
}

/// Durable connector controller.
pub struct Reconciler {
    config: ReconcilerConfig,
    target: Target,
    journal: OperationJournal,
    reporter: Arc<dyn Reporter>,
    desired: RwLock<HashMap<[u8; 16], DesiredSlice>>,
    graph_locks: RwLock<HashMap<[u8; 16], Arc<Mutex<()>>>>,
    target_lock: Mutex<()>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Checkpoint {
    graph_id: [u8; 16],
    generation: u64,
    sequence: u64,
    desired_digest: [u8; 32],
    #[serde(default)]
    current_plan: Option<PathBuf>,
    #[serde(default)]
    last_report: Option<ReportSnapshot>,
    #[serde(default)]
    retired: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReportSnapshot {
    request_id: Vec<u8>,
    publication_id: Option<Vec<u8>>,
    report: SliceReport,
}

impl Reconciler {
    /// Build a controller around explicit target, journal, and callback
    /// adapters.
    pub fn new(
        config: ReconcilerConfig,
        target: Target,
        journal: OperationJournal,
        reporter: Arc<dyn Reporter>,
    ) -> Result<Self, ReconcileError> {
        fs::create_dir_all(config.state_dir.join("graphs"))
            .map_err(|error| ReconcileError::State(error.to_string()))?;
        Ok(Self {
            config,
            target,
            journal,
            reporter,
            desired: RwLock::new(HashMap::new()),
            graph_locks: RwLock::new(HashMap::new()),
            target_lock: Mutex::new(()),
        })
    }

    /// Durably accept a complete desired level and schedule one pass.
    pub async fn accept(
        self: &Arc<Self>,
        request: &ReconcileSliceRequestView<'_>,
    ) -> Result<u64, ReconcileError> {
        let desired = DesiredSlice::from_request(request)?;
        let graph_id = desired.graph_id;
        let sequence = desired.sequence;
        let lock = self.graph_lock(graph_id).await;
        let _guard = lock.lock().await;
        if let Some(current) = self.load(graph_id)? {
            if current.retired {
                return Err(ReconcileError::Retired);
            }
            if sequence < current.sequence {
                return Ok(current.sequence);
            }
            if sequence == current.sequence && desired.desired_digest() != current.desired_digest {
                return Err(ReconcileError::SequenceConflict(sequence));
            }
            if sequence == current.sequence {
                self.desired.write().await.insert(graph_id, desired);
                drop(_guard);
                self.schedule(graph_id, sequence, Duration::ZERO);
                return Ok(sequence);
            }
            if desired.generation < current.generation {
                return Err(ReconcileError::State(format!(
                    "slice generation {} precedes retained generation {}",
                    desired.generation, current.generation
                )));
            }
        }
        self.save(&Checkpoint {
            graph_id,
            generation: desired.generation,
            sequence,
            desired_digest: desired.desired_digest(),
            current_plan: None,
            last_report: None,
            retired: false,
        })?;
        self.desired.write().await.insert(graph_id, desired);
        drop(_guard);
        self.schedule(graph_id, sequence, Duration::ZERO);
        Ok(sequence)
    }

    /// Recover exact accepted levels and resume after restart.
    pub async fn resume(self: &Arc<Self>) -> Result<usize, ReconcileError> {
        let mut count = 0;
        for entry in fs::read_dir(self.config.state_dir.join("graphs"))
            .map_err(|error| ReconcileError::State(error.to_string()))?
        {
            let path = entry
                .map_err(|error| ReconcileError::State(error.to_string()))?
                .path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let bytes =
                fs::read(&path).map_err(|error| ReconcileError::State(error.to_string()))?;
            let checkpoint = serde_json::from_slice::<Checkpoint>(&bytes)
                .map_err(|error| ReconcileError::State(error.to_string()))?;
            if checkpoint.retired {
                continue;
            }
            let recovered = self
                .reporter
                .fetch_slice(checkpoint.graph_id, checkpoint.sequence)
                .await?;
            let desired = DesiredSlice::from_recovered(&recovered)?;
            if desired.desired_digest() != checkpoint.desired_digest {
                return Err(ReconcileError::SequenceConflict(checkpoint.sequence));
            }
            self.desired
                .write()
                .await
                .insert(checkpoint.graph_id, desired);
            self.schedule(checkpoint.graph_id, checkpoint.sequence, Duration::ZERO);
            count += 1;
        }
        Ok(count)
    }

    /// Conservatively release S2 ownership without deleting database objects.
    pub async fn retire(
        &self,
        graph_id: [u8; 16],
        generation: u64,
        sequence: u64,
    ) -> Result<u64, ReconcileError> {
        let lock = self.graph_lock(graph_id).await;
        let _guard = lock.lock().await;
        let mut checkpoint = self
            .load(graph_id)?
            .ok_or_else(|| ReconcileError::State("cannot retire unknown graph".into()))?;
        if checkpoint.generation != generation || sequence < checkpoint.sequence {
            return Err(ReconcileError::State(
                "retire identity does not match retained level".into(),
            ));
        }
        if checkpoint.retired {
            return Ok(generation);
        }
        let _target = self.target_lock.lock().await;
        let journal = journal_snapshot(self.journal.load().await)?;
        let _ = journal_append(
            self.journal
                .append(
                    journal.tail,
                    JournalEvent::GraphReleased {
                        graph_id: hex::encode(graph_id),
                    },
                )
                .await,
        )?;
        checkpoint.retired = true;
        checkpoint.current_plan = None;
        checkpoint.last_report = None;
        self.save(&checkpoint)?;
        self.desired.write().await.remove(&graph_id);
        Ok(generation)
    }

    async fn reconcile_once(
        self: Arc<Self>,
        graph_id: [u8; 16],
        expected_sequence: u64,
    ) -> Result<(), ReconcileError> {
        let lock = self.graph_lock(graph_id).await;
        let _graph_guard = lock.lock().await;
        let mut checkpoint = self
            .load(graph_id)?
            .ok_or_else(|| ReconcileError::State("accepted graph checkpoint is missing".into()))?;
        if checkpoint.retired || checkpoint.sequence != expected_sequence {
            return Ok(());
        }
        let desired = self
            .desired
            .read()
            .await
            .get(&graph_id)
            .cloned()
            .ok_or_else(|| ReconcileError::State("accepted desired slice is not loaded".into()))?;
        let span = tracing::span!(
            Level::INFO,
            "supabase.reconcile_slice",
            { crate::telemetry::GRAPH_ID } = %hex::encode(graph_id),
            { crate::telemetry::GRAPH_GENERATION } = %desired.generation,
            { crate::telemetry::SLICE_SEQUENCE } = %desired.sequence,
            { crate::telemetry::COMPONENT_COUNT } = desired.components.len(),
            { crate::telemetry::RECONCILE_PHASE } = tracing::field::Empty,
            { crate::telemetry::RECONCILE_OUTCOME } = tracing::field::Empty,
            { crate::telemetry::PLAN_ID } = tracing::field::Empty,
            { crate::telemetry::OPERATION_ID } = tracing::field::Empty,
            { crate::telemetry::JOURNAL_TAIL } = tracing::field::Empty,
        );
        async {
            let _target_guard = self.target_lock.lock().await;
            let journal = journal_snapshot(self.journal.load().await)?;
            Span::current().record(crate::telemetry::JOURNAL_TAIL, journal.tail.to_string());
            let diagnostics = validate_desired(&desired, &journal);
            if !diagnostics.is_empty() {
                Span::current().record(crate::telemetry::RECONCILE_PHASE, "plan");
                Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "plan_failed");
                let report = failed_report(&desired, plan_diagnostics(diagnostics));
                checkpoint.current_plan = None;
                self.publish(&mut checkpoint, report, None).await?;
                return Ok(());
            }

            if let Some(path) = checkpoint.current_plan.clone() {
                Span::current().record(crate::telemetry::RECONCILE_PHASE, "apply");
                let plan = review::load(&path)
                    .map_err(|error| ReconcileError::State(error.to_string()))?;
                validate_plan_identity(&plan, &desired)?;
                Span::current().record(crate::telemetry::PLAN_ID, &plan.plan_id);
                let operation = plan.operations.first().ok_or_else(|| {
                    ReconcileError::State("persisted plan has no operation".into())
                })?;
                Span::current().record(crate::telemetry::OPERATION_ID, &operation.id);
                let expected_tail = plan
                    .journal_tail
                    .parse::<u64>()
                    .map_err(|error| ReconcileError::State(error.to_string()))?;
                let operation_state = journal.operations.get(&operation.id);
                if journal.tail != expected_tail
                    && !matches!(
                        operation_state,
                        Some(OperationState::Started { plan_id }) if plan_id == &plan.plan_id
                    )
                {
                    stale_plan(&self.journal, &journal, &plan, "journal-fence-changed").await?;
                    checkpoint.current_plan = None;
                    let report = reconciling_report(
                        &desired,
                        diagnostic(
                            "supabase.plan.stale",
                            "operation journal advanced after this plan was reviewed; a fresh \
                             plan is required",
                            None,
                            Some("No target mutation was attempted."),
                            None,
                        ),
                    );
                    self.publish(&mut checkpoint, report, None).await?;
                    self.schedule(graph_id, desired.sequence, Duration::ZERO);
                    Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "plan_stale");
                    return Ok(());
                }

                if let Operation::EstablishBinding { binding } = &operation.operation {
                    let mut tail = journal.tail;
                    if operation_state.is_none() {
                        tail = append_started(&self.journal, tail, &plan, operation).await?;
                    }
                    tail = journal_append(
                        self.journal
                            .append(
                                tail,
                                JournalEvent::BindingEstablished {
                                    binding: binding.clone(),
                                },
                            )
                            .await,
                    )?;
                    let _ = append_succeeded(
                        &self.journal,
                        tail,
                        &plan,
                        operation,
                        &plan.observed_digest,
                        operation_state.is_some(),
                    )
                    .await?;
                    checkpoint.current_plan = None;
                    let report = operation_report(&desired, operation, &plan);
                    self.publish(&mut checkpoint, report, None).await?;
                    self.schedule(graph_id, desired.sequence, Duration::ZERO);
                    Span::current()
                        .record(crate::telemetry::RECONCILE_OUTCOME, "operation_applied");
                    return Ok(());
                }

                let current = self.target.observe(&desired).await.map_err(target_state)?;
                let recovered = matches!(
                    operation_state,
                    Some(OperationState::Started { plan_id }) if plan_id == &plan.plan_id
                ) && self
                    .target
                    .operation_satisfied(&operation.operation, &current);
                if recovered {
                    let _ = append_succeeded(
                        &self.journal,
                        journal.tail,
                        &plan,
                        operation,
                        &format!("blake3:{}", hex::encode(current.digest())),
                        true,
                    )
                    .await?;
                    checkpoint.current_plan = None;
                    let report = operation_report(&desired, operation, &plan);
                    self.publish(&mut checkpoint, report, None).await?;
                    self.schedule(graph_id, desired.sequence, Duration::ZERO);
                    Span::current()
                        .record(crate::telemetry::RECONCILE_OUTCOME, "operation_recovered");
                    return Ok(());
                }

                let mut tail = journal.tail;
                if operation_state.is_none() {
                    tail = append_started(&self.journal, tail, &plan, operation).await?;
                }
                match self.target.apply(&desired, &plan, operation).await {
                    Ok(ApplyResult::Applied { observed_digest }) => {
                        let _ = append_succeeded(
                            &self.journal,
                            tail,
                            &plan,
                            operation,
                            &observed_digest,
                            operation_state.is_some(),
                        )
                        .await?;
                        checkpoint.current_plan = None;
                        let report = operation_report(&desired, operation, &plan);
                        self.publish(&mut checkpoint, report, None).await?;
                        self.schedule(graph_id, desired.sequence, Duration::ZERO);
                        Span::current()
                            .record(crate::telemetry::RECONCILE_OUTCOME, "operation_applied");
                    }
                    Ok(ApplyResult::Stale { observed_digest }) => {
                        let snapshot = journal_snapshot(self.journal.load().await)?;
                        stale_plan(
                            &self.journal,
                            &snapshot,
                            &plan,
                            &format!("target-changed:{observed_digest}"),
                        )
                        .await?;
                        checkpoint.current_plan = None;
                        let report = reconciling_report(
                            &desired,
                            diagnostic(
                                "supabase.plan.stale",
                                "target observation changed before apply; the exact reviewed plan \
                                 was rejected",
                                None,
                                Some("A new plan will be derived from current target truth."),
                                None,
                            ),
                        );
                        self.publish(&mut checkpoint, report, None).await?;
                        self.schedule(graph_id, desired.sequence, Duration::ZERO);
                        Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "plan_stale");
                    }
                    Err(TargetError::Provider(provider)) => {
                        let snapshot = journal_snapshot(self.journal.load().await)?;
                        let _ = journal_append(
                            self.journal
                                .append(
                                    snapshot.tail,
                                    JournalEvent::OperationFailed {
                                        plan_id: plan.plan_id.clone(),
                                        operation_id: operation.id.clone(),
                                        code: provider.code.clone(),
                                        detail: provider.detail.clone(),
                                    },
                                )
                                .await,
                        )?;
                        checkpoint.current_plan = None;
                        let component = operation
                            .component_scopes
                            .first()
                            .and_then(|hash| hex::decode(hash).ok());
                        let report = failed_report(
                            &desired,
                            vec![diagnostic(
                                &provider.code,
                                &provider.detail,
                                component,
                                provider.help.as_deref(),
                                None,
                            )],
                        );
                        self.publish(&mut checkpoint, report, None).await?;
                        Span::current()
                            .record(crate::telemetry::RECONCILE_OUTCOME, "operation_failed");
                    }
                    Err(TargetError::Unavailable(detail)) => {
                        let report = reconciling_report(
                            &desired,
                            diagnostic(
                                "supabase.target.unavailable",
                                &detail,
                                None,
                                Some("The connector will retry from current target truth."),
                                None,
                            ),
                        );
                        self.publish(&mut checkpoint, report, None).await?;
                        self.schedule(graph_id, desired.sequence, Duration::from_secs(2));
                        Span::current()
                            .record(crate::telemetry::RECONCILE_OUTCOME, "target_unavailable");
                    }
                }
                return Ok(());
            }

            Span::current().record(crate::telemetry::RECONCILE_PHASE, "plan");
            let observed = match self.target.observe(&desired).await {
                Ok(value) => value,
                Err(error) => {
                    let report = reconciling_report(
                        &desired,
                        diagnostic(
                            "supabase.target.observe",
                            &error.to_string(),
                            None,
                            Some("The connector will retry observation without applying anything."),
                            None,
                        ),
                    );
                    self.publish(&mut checkpoint, report, None).await?;
                    self.schedule(graph_id, desired.sequence, Duration::from_secs(2));
                    Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "observe_failed");
                    return Ok(());
                }
            };
            let plan = match build_plan(
                &desired,
                &journal,
                &observed,
                PlanContext {
                    journal_tail_after_plan: journal.tail.saturating_add(1),
                    connector_build: &self.config.connector_build,
                    api_url: self.target.api_url(),
                    database_url_ref: self.target.database_url_ref(),
                    anon_key_ref: self.target.anon_key_ref(),
                },
            ) {
                Ok(plan) => plan,
                Err(diagnostics) => {
                    let report = failed_report(&desired, plan_diagnostics(diagnostics));
                    self.publish(&mut checkpoint, report, None).await?;
                    Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "plan_failed");
                    return Ok(());
                }
            };
            Span::current().record(crate::telemetry::PLAN_ID, &plan.plan_id);
            if plan.operations.is_empty() {
                let outputs = plan.contract_outputs();
                let evidence = PublicationEvidence::default()
                    .with_revision(plan.observed_digest.clone())
                    .with_uri(self.journal.evidence_uri(journal.tail));
                let report = ready_report(&desired, outputs, evidence);
                let publication_id = stable_publication_id(&report);
                self.publish(&mut checkpoint, report, publication_id)
                    .await?;
                Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "ready");
                return Ok(());
            }
            let artifacts = review::persist(&self.config.state_dir, &plan)
                .map_err(|error| ReconcileError::State(error.to_string()))?;
            let new_tail = journal_append(
                self.journal
                    .append(
                        journal.tail,
                        JournalEvent::PlanCreated {
                            plan_id: plan.plan_id.clone(),
                            graph_id: plan.graph_id.clone(),
                            generation: plan.generation.clone(),
                            slice_sequence: plan.slice_sequence.clone(),
                            desired_digest: plan.desired_digest.clone(),
                            observed_digest: plan.observed_digest.clone(),
                        },
                    )
                    .await,
            )?;
            if new_tail.to_string() != plan.journal_tail {
                return Err(ReconcileError::State(
                    "PlanCreated tail differs from executable freshness fence".into(),
                ));
            }
            checkpoint.current_plan = Some(artifacts.plan_path);
            let report = reconciling_report(
                &desired,
                diagnostic(
                    "supabase.plan.ready",
                    &format!(
                        "reviewable plan {} is ready at {}",
                        plan.plan_id,
                        artifacts.json_path.display()
                    ),
                    None,
                    Some(
                        "The next level-triggered pass applies at most one exact planned \
                         operation.",
                    ),
                    None,
                ),
            );
            self.publish(&mut checkpoint, report, None).await?;
            self.schedule(graph_id, desired.sequence, Duration::ZERO);
            Span::current().record(crate::telemetry::RECONCILE_OUTCOME, "planned");
            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn publish(
        &self,
        checkpoint: &mut Checkpoint,
        report: SliceReport,
        publication_id: Option<Vec<u8>>,
    ) -> Result<(), ReconcileError> {
        let snapshot = ReportSnapshot {
            request_id: Uuid::now_v7().as_bytes().to_vec(),
            publication_id,
            report,
        };
        checkpoint.last_report = Some(snapshot.clone());
        self.save(checkpoint)?;
        self.reporter
            .report(ReportSliceRequest {
                request_id: Some(snapshot.request_id),
                report: buffa::MessageField::some(snapshot.report),
                publication_id: snapshot.publication_id,
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    fn schedule(self: &Arc<Self>, graph_id: [u8; 16], sequence: u64, delay: Duration) {
        let reconciler = Arc::clone(self);
        tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let _ = reconciler.reconcile_once(graph_id, sequence).await;
        });
    }

    async fn graph_lock(&self, graph_id: [u8; 16]) -> Arc<Mutex<()>> {
        if let Some(lock) = self.graph_locks.read().await.get(&graph_id) {
            return Arc::clone(lock);
        }
        Arc::clone(
            self.graph_locks
                .write()
                .await
                .entry(graph_id)
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    fn state_path(&self, graph_id: [u8; 16]) -> PathBuf {
        self.config
            .state_dir
            .join("graphs")
            .join(format!("{}.json", hex::encode(graph_id)))
    }

    fn load(&self, graph_id: [u8; 16]) -> Result<Option<Checkpoint>, ReconcileError> {
        let path = self.state_path(graph_id);
        match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| ReconcileError::State(error.to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(ReconcileError::State(error.to_string())),
        }
    }

    fn save(&self, checkpoint: &Checkpoint) -> Result<(), ReconcileError> {
        let path = self.state_path(checkpoint.graph_id);
        let parent = path
            .parent()
            .ok_or_else(|| ReconcileError::State("checkpoint path has no parent".into()))?;
        let mut temporary = NamedTempFile::new_in(parent)
            .map_err(|error| ReconcileError::State(error.to_string()))?;
        let bytes = serde_json::to_vec_pretty(checkpoint)
            .map_err(|error| ReconcileError::State(error.to_string()))?;
        temporary
            .write_all(&bytes)
            .map_err(|error| ReconcileError::State(error.to_string()))?;
        temporary
            .as_file_mut()
            .sync_all()
            .map_err(|error| ReconcileError::State(error.to_string()))?;
        temporary
            .persist(path)
            .map_err(|error| ReconcileError::State(error.error.to_string()))?;
        Ok(())
    }
}

fn validate_plan_identity(
    plan: &ExecutablePlan,
    desired: &DesiredSlice,
) -> Result<(), ReconcileError> {
    if plan.graph_id != hex::encode(desired.graph_id)
        || plan.generation != desired.generation.to_string()
        || plan.slice_sequence != desired.sequence.to_string()
        || plan.desired_digest != format!("blake3:{}", hex::encode(desired.desired_digest()))
    {
        Err(ReconcileError::State(
            "persisted executable plan does not describe current desired level".into(),
        ))
    } else {
        Ok(())
    }
}

async fn append_started(
    journal: &OperationJournal,
    tail: u64,
    plan: &ExecutablePlan,
    operation: &crate::plan::PlannedOperation,
) -> Result<u64, ReconcileError> {
    journal_append(
        journal
            .append(
                tail,
                JournalEvent::OperationStarted {
                    plan_id: plan.plan_id.clone(),
                    operation_id: operation.id.clone(),
                    idempotency_key: operation.id.clone(),
                },
            )
            .await,
    )
}

async fn append_succeeded(
    journal: &OperationJournal,
    tail: u64,
    plan: &ExecutablePlan,
    operation: &crate::plan::PlannedOperation,
    observed_digest: &str,
    recovered: bool,
) -> Result<u64, ReconcileError> {
    journal_append(
        journal
            .append(
                tail,
                JournalEvent::OperationSucceeded {
                    plan_id: plan.plan_id.clone(),
                    operation_id: operation.id.clone(),
                    observed_digest: observed_digest.into(),
                    recovered,
                },
            )
            .await,
    )
}

async fn stale_plan(
    journal: &OperationJournal,
    snapshot: &JournalSnapshot,
    plan: &ExecutablePlan,
    reason: &str,
) -> Result<(), ReconcileError> {
    let _ = journal_append(
        journal
            .append(
                snapshot.tail,
                JournalEvent::PlanStale {
                    plan_id: plan.plan_id.clone(),
                    reason: reason.into(),
                },
            )
            .await,
    )?;
    Ok(())
}

fn journal_snapshot(
    result: Result<JournalSnapshot, Fault<faultline::Never, anyhow::Error, anyhow::Error>>,
) -> Result<JournalSnapshot, ReconcileError> {
    result.map_err(|error| ReconcileError::State(error.to_string()))
}

fn journal_append(
    result: Result<u64, Fault<crate::journal::JournalError, anyhow::Error, anyhow::Error>>,
) -> Result<u64, ReconcileError> {
    result.map_err(|error| ReconcileError::State(error.to_string()))
}

fn target_state(error: TargetError) -> ReconcileError {
    ReconcileError::State(error.to_string())
}

fn plan_diagnostics(diagnostics: Vec<PlanDiagnostic>) -> Vec<Diagnostic> {
    diagnostics
        .into_iter()
        .map(|item| {
            diagnostic(
                &item.code,
                &item.message,
                Some(item.component_spec_hash.to_vec()),
                Some(&item.help),
                Some(&item.pointer),
            )
        })
        .collect()
}

fn diagnostic(
    code: &str,
    message: &str,
    component_spec_hash: Option<Vec<u8>>,
    help: Option<&str>,
    pointer: Option<&str>,
) -> Diagnostic {
    let mut value = Diagnostic::default()
        .with_code(code)
        .with_message(message)
        .with_severity(DiagnosticSeverity::Error);
    if let Some(hash) = component_spec_hash {
        value = value.with_component_spec_hash(hash);
    }
    if let Some(help) = help {
        value = value.with_help(help);
    }
    if let Some(pointer) = pointer {
        value = value.with_pointer(pointer);
    }
    value
}

fn dispositions(
    desired: &DesiredSlice,
    kind: ComponentDispositionKind,
) -> Vec<ComponentDisposition> {
    desired
        .components
        .iter()
        .map(|component| {
            ComponentDisposition::default()
                .with_component_spec_hash(component.spec_hash.to_vec())
                .with_kind(kind)
        })
        .collect()
}

fn report_for(
    desired: &DesiredSlice,
    kind: ComponentDispositionKind,
    outputs: Vec<henosis_proto::proto::henosis::v1::ComponentOutputs>,
    diagnostics: Vec<Diagnostic>,
) -> SliceReport {
    SliceReport {
        graph_id: Some(desired.graph_id.to_vec()),
        generation: Some(desired.generation),
        connector: Some(crate::CONNECTOR_NAME.into()),
        dispositions: dispositions(desired, kind),
        outputs,
        diagnostics,
        sequence: Some(desired.sequence),
        ..Default::default()
    }
}

fn failed_report(desired: &DesiredSlice, diagnostics: Vec<Diagnostic>) -> SliceReport {
    report_for(
        desired,
        ComponentDispositionKind::Failed,
        Vec::new(),
        diagnostics,
    )
}

fn reconciling_report(desired: &DesiredSlice, mut diagnostic: Diagnostic) -> SliceReport {
    diagnostic.severity = Some(DiagnosticSeverity::Info.into());
    report_for(
        desired,
        ComponentDispositionKind::Reconciling,
        Vec::new(),
        vec![diagnostic],
    )
}

fn operation_report(
    desired: &DesiredSlice,
    operation: &crate::plan::PlannedOperation,
    plan: &ExecutablePlan,
) -> SliceReport {
    reconciling_report(
        desired,
        diagnostic(
            "supabase.apply.operation-complete",
            &format!(
                "operation {} ({}) from plan {} reached its postcondition",
                operation.id, operation.address, plan.plan_id
            ),
            None,
            Some("The next pass re-observes and replans the remaining level."),
            None,
        ),
    )
}

fn ready_report(
    desired: &DesiredSlice,
    outputs: Vec<henosis_proto::proto::henosis::v1::ComponentOutputs>,
    publication: PublicationEvidence,
) -> SliceReport {
    let mut report = report_for(
        desired,
        ComponentDispositionKind::Ready,
        outputs,
        Vec::new(),
    );
    report.publication = buffa::MessageField::some(publication);
    report
}

fn stable_publication_id(report: &SliceReport) -> Option<Vec<u8>> {
    if report.outputs.is_empty() {
        return None;
    }
    let bytes = serde_json::to_vec(
        &report
            .outputs
            .iter()
            .map(|output| (&output.component_spec_hash, &output.values_json))
            .collect::<Vec<_>>(),
    )
    .expect("output publication identity is JSON");
    Some(
        Uuid::new_v5(&PUBLICATION_NAMESPACE, &bytes)
            .as_bytes()
            .to_vec(),
    )
}

#[allow(dead_code)]
fn _path_is_private_plan(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "plans")
}
