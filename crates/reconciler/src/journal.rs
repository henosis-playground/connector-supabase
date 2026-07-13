//! Minimal S2-backed identity binding and operation journal.

use std::collections::BTreeMap;

use anyhow::Context as _;
use faultline::Error as Fault;
use faultline::Never;
use s2_sdk::S2;
use s2_sdk::S2Stream;
use s2_sdk::types::AccountEndpoint;
use s2_sdk::types::AppendConditionFailed;
use s2_sdk::types::AppendInput;
use s2_sdk::types::AppendRecord;
use s2_sdk::types::AppendRecordBatch;
use s2_sdk::types::BasinEndpoint;
use s2_sdk::types::BasinName;
use s2_sdk::types::ReadFrom;
use s2_sdk::types::ReadInput;
use s2_sdk::types::ReadLimits;
use s2_sdk::types::ReadStart;
use s2_sdk::types::ReadStop;
use s2_sdk::types::S2Config;
use s2_sdk::types::S2Endpoints;
use s2_sdk::types::S2Error;
use s2_sdk::types::StreamName;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// Current durable envelope discriminator.
pub const API_VERSION: &str = "henosis.dev/supabase-operation-journal/v1";

/// Configured S2 journal coordinates.
#[derive(Clone, Debug)]
pub struct JournalConfig {
    /// S2 bearer token.
    pub access_token: String,
    /// Account endpoint.
    pub account_endpoint: String,
    /// Basin endpoint.
    pub basin_endpoint: String,
    /// Existing basin name.
    pub basin: String,
    /// Connector-owned target stream.
    pub stream: String,
}

/// S2 operation journal client.
#[derive(Clone, Debug)]
pub struct OperationJournal {
    stream: S2Stream,
    basin_name: String,
    stream_name: String,
}

/// Folded minimal operational state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct JournalSnapshot {
    /// Next S2 sequence number.
    pub tail: u64,
    /// Active resource identity bindings.
    pub bindings: BTreeMap<String, Binding>,
    /// Operation terminal state by operation ID.
    pub operations: BTreeMap<String, OperationState>,
}

/// Stable logical-to-native identity binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Binding {
    /// Owning graph UUID as lowercase hexadecimal.
    pub graph_id: String,
    /// Stable author-owned identity.
    pub resource_id: String,
    /// Self-hosted project identity.
    pub project: String,
    /// `PostgreSQL` database.
    pub database: String,
    /// Owned schema remote identity.
    pub schema: String,
}

/// Folded operation state used for crash recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationState {
    /// A durable started receipt exists without a terminal receipt.
    Started {
        /// Plan that authorized the operation.
        plan_id: String,
    },
    /// The postcondition was observed and receipted.
    Succeeded {
        /// Plan that authorized the operation.
        plan_id: String,
        /// Target digest after completion.
        observed_digest: String,
    },
    /// The target rejected the operation with canonical evidence.
    Failed {
        /// Plan that authorized the operation.
        plan_id: String,
        /// Stable diagnostic code.
        code: String,
        /// Verbatim safe provider detail.
        detail: String,
    },
}

/// One versioned append-only journal envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JournalEnvelope {
    /// Version discriminator.
    pub api_version: String,
    /// Stable event identity for retry deduplication.
    pub event_id: String,
    /// Event payload.
    pub event: JournalEvent,
}

/// Minimal durable operation evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum JournalEvent {
    /// Legacy pre-SDK plan marker retained only so existing streams remain
    /// readable.
    PlanCreated {
        plan_id: String,
        graph_id: String,
        generation: String,
        slice_sequence: String,
        desired_digest: String,
        observed_digest: String,
    },
    /// Legacy pre-SDK stale marker retained only so existing streams remain
    /// readable.
    PlanStale { plan_id: String, reason: String },
    /// A stable remote identity was claimed.
    BindingEstablished {
        /// Complete binding.
        binding: Binding,
    },
    /// One exact operation is about to cross the target boundary.
    OperationStarted {
        /// Authorizing plan.
        plan_id: String,
        /// Stable operation identity.
        operation_id: String,
        /// Stable retry identity.
        idempotency_key: String,
    },
    /// The operation postcondition was re-observed.
    OperationSucceeded {
        /// Authorizing plan.
        plan_id: String,
        /// Stable operation identity.
        operation_id: String,
        /// Target digest after completion.
        observed_digest: String,
        /// Whether completion was recovered after an interrupted attempt.
        recovered: bool,
    },
    /// The target returned a canonical failure.
    OperationFailed {
        /// Authorizing plan.
        plan_id: String,
        /// Stable operation identity.
        operation_id: String,
        /// Stable diagnostic code.
        code: String,
        /// Verbatim safe provider detail.
        detail: String,
    },
    /// A graph released its exclusive local target ownership.
    GraphReleased {
        /// Graph UUID as lowercase hexadecimal.
        graph_id: String,
    },
}

/// Journal domain failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum JournalError {
    /// Another writer advanced the stream.
    #[error("operation journal advanced to tail {current_tail}")]
    CasConflict {
        /// Current remote tail.
        current_tail: u64,
    },
}

impl OperationJournal {
    /// Construct a client for one configured target stream.
    pub fn connect(config: &JournalConfig) -> anyhow::Result<Self> {
        let endpoints = S2Endpoints::new(
            AccountEndpoint::new(&config.account_endpoint)
                .context("invalid S2 account endpoint")?,
            BasinEndpoint::new(&config.basin_endpoint).context("invalid S2 basin endpoint")?,
        )
        .context("invalid S2 endpoint configuration")?;
        let client = S2::new(S2Config::new(&config.access_token).with_endpoints(endpoints))
            .context("construct S2 client")?;
        let basin = config
            .basin
            .parse::<BasinName>()
            .context("invalid S2 basin name")?;
        let stream = config
            .stream
            .parse::<StreamName>()
            .context("invalid S2 stream name")?;
        Ok(Self {
            stream: client.basin(basin).stream(stream),
            basin_name: config.basin.clone(),
            stream_name: config.stream.clone(),
        })
    }

    /// Immutable evidence URI for an exact next-tail fence.
    pub fn evidence_uri(&self, tail: u64) -> String {
        format!("s2://{}/{}@{tail}", self.basin_name, self.stream_name)
    }

    /// Load and fold all operation evidence through the current tail.
    pub async fn load(
        &self,
    ) -> Result<JournalSnapshot, Fault<Never, anyhow::Error, anyhow::Error>> {
        let tail = match self.stream.check_tail().await {
            Ok(value) => value.seq_num,
            Err(S2Error::Server(response))
                if response.code.to_ascii_lowercase().contains("not_found")
                    || response.message.to_ascii_lowercase().contains("not found") =>
            {
                0
            }
            Err(error) => return Err(never_transient(error)),
        };
        let mut snapshot = JournalSnapshot {
            tail,
            ..Default::default()
        };
        let mut next = 0;
        while next < tail {
            let batch = self
                .stream
                .read(
                    ReadInput::new()
                        .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(next)))
                        .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(1_000)))
                        .with_ignore_command_records(true),
                )
                .await
                .map_err(never_transient)?;
            if batch.records.is_empty() {
                return Err(never_invariant(anyhow::anyhow!(
                    "empty S2 journal batch before tail {tail} at {next}"
                )));
            }
            for record in batch.records {
                next = record.seq_num.saturating_add(1);
                let envelope =
                    serde_json::from_slice::<JournalEnvelope>(&record.body).map_err(|error| {
                        never_invariant(anyhow::anyhow!(
                            "invalid operation journal record at {}: {error}",
                            record.seq_num
                        ))
                    })?;
                if envelope.api_version != API_VERSION {
                    return Err(never_invariant(anyhow::anyhow!(
                        "unsupported operation journal apiVersion {:?} at {}",
                        envelope.api_version,
                        record.seq_num
                    )));
                }
                snapshot.apply(envelope.event);
            }
        }
        Ok(snapshot)
    }

    /// Compare-and-append one already-validated event at an exact tail.
    pub async fn append(
        &self,
        expected_tail: u64,
        event: JournalEvent,
    ) -> Result<u64, Fault<JournalError, anyhow::Error, anyhow::Error>> {
        let envelope = JournalEnvelope {
            api_version: API_VERSION.into(),
            event_id: uuid::Uuid::now_v7().to_string(),
            event,
        };
        let body = serde_json::to_vec(&envelope).map_err(journal_invariant)?;
        let record = AppendRecord::new(body).map_err(journal_invariant)?;
        let batch = AppendRecordBatch::try_from_iter([record]).map_err(journal_invariant)?;
        match self
            .stream
            .append(AppendInput::new(batch).with_match_seq_num(expected_tail))
            .await
        {
            Ok(ack) => Ok(ack.start.seq_num.saturating_add(1)),
            Err(S2Error::AppendConditionFailed(AppendConditionFailed::SeqNumMismatch(
                current_tail,
            ))) => Err(Fault::Domain(JournalError::CasConflict { current_tail })),
            Err(error) => Err(Fault::Transient(anyhow::Error::new(error))),
        }
    }
}

fn never_transient(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<Never, anyhow::Error, anyhow::Error> {
    Fault::Transient(anyhow::Error::new(error))
}

fn never_invariant(error: impl Into<anyhow::Error>) -> Fault<Never, anyhow::Error, anyhow::Error> {
    Fault::Invariant(error.into())
}

fn journal_invariant(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<JournalError, anyhow::Error, anyhow::Error> {
    Fault::Invariant(anyhow::Error::new(error))
}

impl JournalSnapshot {
    fn apply(&mut self, event: JournalEvent) {
        match event {
            JournalEvent::PlanCreated { .. } | JournalEvent::PlanStale { .. } => {}
            JournalEvent::BindingEstablished { binding } => {
                self.bindings.insert(binding.resource_id.clone(), binding);
            }
            JournalEvent::OperationStarted {
                plan_id,
                operation_id,
                ..
            } => {
                self.operations
                    .insert(operation_id, OperationState::Started { plan_id });
            }
            JournalEvent::OperationSucceeded {
                plan_id,
                operation_id,
                observed_digest,
                ..
            } => {
                self.operations.insert(
                    operation_id,
                    OperationState::Succeeded {
                        plan_id,
                        observed_digest,
                    },
                );
            }
            JournalEvent::OperationFailed {
                plan_id,
                operation_id,
                code,
                detail,
            } => {
                self.operations.insert(
                    operation_id,
                    OperationState::Failed {
                        plan_id,
                        code,
                        detail,
                    },
                );
            }
            JournalEvent::GraphReleased { graph_id } => {
                self.bindings
                    .retain(|_, binding| binding.graph_id != graph_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folds_bindings_and_crash_recovery_receipts_without_target_state() {
        let binding = Binding {
            graph_id: "11".repeat(16),
            resource_id: "catalog".into(),
            project: "henosis-local".into(),
            database: "postgres".into(),
            schema: "catalog".into(),
        };
        let mut snapshot = JournalSnapshot::default();
        snapshot.apply(JournalEvent::BindingEstablished {
            binding: binding.clone(),
        });
        snapshot.apply(JournalEvent::OperationStarted {
            plan_id: "sha256:plan".into(),
            operation_id: "sha256:operation".into(),
            idempotency_key: "sha256:operation".into(),
        });
        assert_eq!(snapshot.bindings["catalog"], binding);
        assert!(matches!(
            snapshot.operations["sha256:operation"],
            OperationState::Started { .. }
        ));
        snapshot.apply(JournalEvent::OperationSucceeded {
            plan_id: "sha256:plan".into(),
            operation_id: "sha256:operation".into(),
            observed_digest: "blake3:target".into(),
            recovered: true,
        });
        assert!(matches!(
            snapshot.operations["sha256:operation"],
            OperationState::Succeeded { .. }
        ));
        snapshot.apply(JournalEvent::GraphReleased {
            graph_id: "11".repeat(16),
        });
        assert!(snapshot.bindings.is_empty());
    }
}
