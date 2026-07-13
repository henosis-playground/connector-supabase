//! Parse-don't-validate boundary from the shared protobuf contract.

use std::collections::BTreeSet;

use buffa::Message as _;
use buffa::MessageView as _;
use henosis_proto::proto::henosis::v1::GraphSlice;
use henosis_proto::proto::henosis::v1::ReconcileSliceRequestView;
use henosis_proto::proto::henosis::v1::RegisteredComponentSpecView;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::context::ComponentContext;
use crate::context::ContextError;

/// Validated complete desired level.
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
    /// Canonical JSON object.
    pub values_json: Vec<u8>,
}

impl IdOrdItem for UpstreamOutput {
    type Key<'a> = [u8; 32];

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.component_spec_hash
    }
}

/// Slice contract violation at durable acceptance.
#[derive(Debug, Error)]
pub enum SliceError {
    /// A required shared-contract field is absent or malformed.
    #[error("{0}")]
    Invalid(String),
    /// One component has invalid connector context.
    #[error("component {component}: {source}")]
    Context {
        /// Component name or hash fallback.
        component: String,
        /// Context-specific failure.
        source: ContextError,
    },
}

impl DesiredSlice {
    /// Validate a borrowed request view, copying only accepted domain state.
    pub fn from_request(request: &ReconcileSliceRequestView<'_>) -> Result<Self, SliceError> {
        let slice = request
            .slice
            .as_option()
            .ok_or_else(|| SliceError::Invalid("slice is required".into()))?;
        Self::from_view(slice)
    }

    /// Validate an exact level recovered from core after restart.
    pub fn from_recovered(slice: &GraphSlice) -> Result<Self, SliceError> {
        let bytes = slice.encode_to_vec();
        let view = henosis_proto::proto::henosis::v1::GraphSliceView::decode_view(&bytes)
            .map_err(|error| SliceError::Invalid(error.to_string()))?;
        Self::from_view(&view)
    }

    fn from_view(
        slice: &henosis_proto::proto::henosis::v1::GraphSliceView<'_>,
    ) -> Result<Self, SliceError> {
        let graph_id = exact_bytes(slice.graph_id, "slice.graph_id")?;
        let generation = slice.generation.filter(|value| *value > 0).ok_or_else(|| {
            SliceError::Invalid("slice.generation must be greater than zero".into())
        })?;
        let sequence = slice
            .sequence
            .ok_or_else(|| SliceError::Invalid("slice.sequence is required".into()))?;
        if slice.connector != Some(crate::CONNECTOR_NAME) {
            return Err(SliceError::Invalid(format!(
                "slice.connector must be {:?}",
                crate::CONNECTOR_NAME
            )));
        }

        let mut components = IdOrdMap::with_capacity(slice.components.len());
        let mut names = BTreeSet::new();
        let mut resource_ids = BTreeSet::new();
        let mut schemas = BTreeSet::new();
        let mut target = None::<(String, String, String)>;
        for registered in slice.components.iter() {
            let pin = parse_component(registered)?;
            if !names.insert(pin.name.clone()) {
                return Err(SliceError::Invalid(format!(
                    "component name {:?} is duplicated",
                    pin.name
                )));
            }
            if !resource_ids.insert(pin.context.resource_id.clone()) {
                return Err(SliceError::Invalid(format!(
                    "resourceId {:?} is duplicated",
                    pin.context.resource_id
                )));
            }
            if !schemas.insert(pin.context.target.schema.clone()) {
                return Err(SliceError::Invalid(format!(
                    "target schema {:?} is duplicated",
                    pin.context.target.schema
                )));
            }
            let current = (
                pin.context.target.stack.clone(),
                pin.context.target.project.clone(),
                pin.context.target.database.clone(),
            );
            if target.as_ref().is_some_and(|expected| expected != &current) {
                return Err(SliceError::Invalid(
                    "every component in a Supabase slice must select the same \
                     stack/project/database"
                        .into(),
                ));
            }
            target = Some(current);
            let hash = pin.spec_hash;
            components.insert_unique(pin).map_err(|_| {
                SliceError::Invalid(format!(
                    "component spec hash {} is duplicated",
                    hex::encode(hash)
                ))
            })?;
        }

        let mut upstream_outputs = IdOrdMap::with_capacity(slice.upstream_outputs.len());
        for output in slice.upstream_outputs.iter() {
            let component_spec_hash = exact_bytes(
                output.component_spec_hash,
                "slice.upstream_outputs.component_spec_hash",
            )?;
            let value =
                serde_json::from_slice::<serde_json::Value>(output.values_json.unwrap_or_default())
                    .map_err(|error| {
                        SliceError::Invalid(format!(
                            "upstream output {} is not JSON: {error}",
                            hex::encode(component_spec_hash)
                        ))
                    })?;
            let values_json = serde_json::to_vec(&value)
                .map_err(|error| SliceError::Invalid(error.to_string()))?;
            upstream_outputs
                .insert_unique(UpstreamOutput {
                    component_spec_hash,
                    values_json,
                })
                .map_err(|_| {
                    SliceError::Invalid(format!(
                        "upstream output {} is duplicated",
                        hex::encode(component_spec_hash)
                    ))
                })?;
        }

        Ok(Self {
            graph_id,
            generation,
            sequence,
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

fn parse_component(
    component: &RegisteredComponentSpecView<'_>,
) -> Result<ComponentPin, SliceError> {
    let spec_hash = exact_bytes(component.hash, "component.hash")?;
    let fallback = hex::encode(spec_hash);
    let spec = component
        .spec
        .as_option()
        .ok_or_else(|| SliceError::Invalid(format!("component {fallback} has no spec body")))?;
    let encoded = spec
        .to_owned_message()
        .map_err(|error| SliceError::Invalid(error.to_string()))?
        .encode_to_vec();
    if blake3::hash(&encoded).as_bytes() != &spec_hash {
        return Err(SliceError::Invalid(format!(
            "component {fallback} hash does not match canonical spec content"
        )));
    }
    let name = spec
        .name
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SliceError::Invalid(format!("component {fallback} has no name")))?;
    if spec.connector != Some(crate::CONNECTOR_NAME) {
        return Err(SliceError::Invalid(format!(
            "component {name:?} is not owned by connector {:?}",
            crate::CONNECTOR_NAME
        )));
    }
    let mut dependencies = BTreeSet::new();
    for dependency in spec.depends_on.iter() {
        let dependency: [u8; 32] = exact_bytes(Some(dependency), "component.spec.depends_on")?;
        if !dependencies.insert(dependency) {
            return Err(SliceError::Invalid(format!(
                "component {name:?} repeats dependency {}",
                hex::encode(dependency)
            )));
        }
    }
    let context = ComponentContext::from_bytes(spec.connector_context.unwrap_or_default())
        .map_err(|source| SliceError::Context {
            component: name.into(),
            source,
        })?;
    Ok(ComponentPin {
        spec_hash,
        name: name.into(),
        context,
    })
}

fn exact_bytes<const N: usize>(value: Option<&[u8]>, field: &str) -> Result<[u8; N], SliceError> {
    value
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| SliceError::Invalid(format!("{field} must contain exactly {N} bytes")))
}
