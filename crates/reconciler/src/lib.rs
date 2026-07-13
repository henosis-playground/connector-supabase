//! Reviewable, freshness-fenced Supabase target lifecycle for Henosis.

pub mod context;
pub mod journal;
pub mod plan;
pub mod reconciler;
pub mod review;
pub mod slice;
pub mod target;

pub use reconciler::ConnectorConfig;
pub use reconciler::SupabaseConnector;

/// Registry key served by this connector.
pub const CONNECTOR_NAME: &str = "supabase";
