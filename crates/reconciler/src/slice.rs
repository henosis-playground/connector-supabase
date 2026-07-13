//! Target-specific decoding after the SDK's shared contract boundary.

use std::collections::BTreeSet;

use connector_sdk::ContractError;
use connector_sdk::TargetSlice;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;

use crate::context::ComponentContext;

/// Connector-owned desired target model.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DesiredSlice {
    /// Raw graph UUID bytes.
    pub graph_id: [u8; 16],
    /// Desired-intent provenance.
    pub generation: u64,
    /// Exact complete materialization identity.
    pub sequence: u64,
    /// Registered specs sorted by content hash.
    pub components: IdOrdMap<ComponentPin>,
    /// Current-generation upstream output levels.
    pub upstream_outputs: IdOrdMap<UpstreamOutput>,
}

/// One validated component contribution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentPin {
    /// Registered component-spec content hash.
    pub spec_hash: [u8; 32],
    /// Human-facing component name.
    pub name: String,
    /// Strict connector-owned context.
    pub context: ComponentContext,
}

impl IdOrdItem for ComponentPin {
    type Key<'a> = [u8; 32];

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.spec_hash
    }
}

/// One canonical upstream publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpstreamOutput {
    /// Producer spec hash.
    pub component_spec_hash: [u8; 32],
    /// Canonical JSON object bytes.
    pub values_json: Vec<u8>,
}

impl IdOrdItem for UpstreamOutput {
    type Key<'a> = [u8; 32];

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.component_spec_hash
    }
}

impl DesiredSlice {
    /// Decode only Supabase target semantics; shared hashes, ownership,
    /// dependencies, and upstream JSON are already validated by the SDK.
    pub fn decode(slice: &TargetSlice) -> Result<Self, ContractError> {
        let mut components = IdOrdMap::with_capacity(slice.components.len());
        let mut names = BTreeSet::new();
        let mut resource_ids = BTreeSet::new();
        let mut schemas = BTreeSet::new();
        let mut target = None::<(String, String, String)>;
        for component in &slice.components {
            let context =
                ComponentContext::from_bytes(&component.connector_context).map_err(|error| {
                    ContractError::target(format!("component {:?}: {error}", component.name))
                })?;
            if !names.insert(component.name.clone()) {
                return Err(ContractError::target(format!(
                    "component name {:?} is duplicated",
                    component.name
                )));
            }
            if !resource_ids.insert(context.resource_id.clone()) {
                return Err(ContractError::target(format!(
                    "resourceId {:?} is duplicated",
                    context.resource_id
                )));
            }
            if !schemas.insert(context.target.schema.clone()) {
                return Err(ContractError::target(format!(
                    "target schema {:?} is duplicated",
                    context.target.schema
                )));
            }
            let current = (
                context.target.stack.clone(),
                context.target.project.clone(),
                context.target.database.clone(),
            );
            if target.as_ref().is_some_and(|expected| expected != &current) {
                return Err(ContractError::target(
                    "every component in a Supabase slice must select the same \
                     stack/project/database",
                ));
            }
            target = Some(current);
            components
                .insert_unique(ComponentPin {
                    spec_hash: component.spec_hash,
                    name: component.name.clone(),
                    context,
                })
                .map_err(|_| ContractError::target("component hash is duplicated"))?;
        }

        let upstream_outputs =
            IdOrdMap::from_iter_unique(slice.upstream_outputs.iter().map(|output| {
                UpstreamOutput {
                    component_spec_hash: output.component_spec_hash,
                    values_json: output.values_json.clone(),
                }
            }))
            .map_err(|_| ContractError::target("upstream output hash is duplicated"))?;
        Ok(Self {
            graph_id: slice.graph_id,
            generation: slice.generation,
            sequence: slice.sequence,
            components,
            upstream_outputs,
        })
    }

    /// Stable digest of every desired input that may affect the target.
    pub fn desired_digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"henosis.dev/supabase-desired/v1\0");
        hasher.update(&self.graph_id);
        hasher.update(&self.generation.to_be_bytes());
        for component in self.components.iter() {
            hasher.update(&component.spec_hash);
        }
        for output in self.upstream_outputs.iter() {
            hasher.update(&output.component_spec_hash);
            hasher.update(&(output.values_json.len() as u64).to_be_bytes());
            hasher.update(&output.values_json);
        }
        *hasher.finalize().as_bytes()
    }
}
