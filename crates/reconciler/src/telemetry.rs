//! Registry for connector-specific tracing attributes.

/// Raw graph UUID rendered as lowercase hexadecimal.
pub const GRAPH_ID: &str = "soter.henosis.graph.id";
/// Desired graph generation rendered as a string ID.
pub const GRAPH_GENERATION: &str = "soter.henosis.graph.generation";
/// Durable graph-slice sequence rendered as a string ID.
pub const SLICE_SEQUENCE: &str = "soter.henosis.slice.sequence";
/// Number of components in the complete owned slice.
pub const COMPONENT_COUNT: &str = "soter.henosis.slice.component_count";
/// Bounded reconcile phase.
pub const RECONCILE_PHASE: &str = "soter.henosis.reconcile.phase";
/// Bounded reconcile outcome.
pub const RECONCILE_OUTCOME: &str = "soter.henosis.reconcile.outcome";
/// Immutable plan digest.
pub const PLAN_ID: &str = "soter.henosis.plan.id";
/// Stable operation digest.
pub const OPERATION_ID: &str = "soter.henosis.operation.id";
/// Exact S2 operation-journal tail rendered as a string ID.
pub const JOURNAL_TAIL: &str = "soter.henosis.journal.tail";
