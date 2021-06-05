use std::any::Any;
use std::fmt::Debug;

use erased_serde::{Serialize as ErasedSerialize, Serializer as ErasedSerializer};
use serde::Serialize;

use crate::Result;

/// A protocol to represent any kind of aggregation product a event stream may produce
pub trait Artifact: Any + Send + Debug + ErasedSerialize {
    /// Upcast the artifact to `&dyn Any`
    ///
    /// Usually, an implementation involves nothing more than `{ self }` and may be provided by a
    /// procedural macro in the future.
    fn upcast_ref(&self) -> &dyn Any;

    /// Upcast the artifact to `&mut dyn Any`
    ///
    /// Usually, an implementation involves nothing more than `{ self }` and may be provided by a
    /// procedural macro in the future.
    fn upcast_mut(&mut self) -> &mut dyn Any;
}

erased_serde::serialize_trait_object!(Artifact);

/// Container for arbitrary artifacts a stream processing pipeline may create
#[derive(Debug, Serialize)]
pub struct AnyArtifact {
    artifact: Box<dyn Artifact>,
}

impl AnyArtifact {
    /// Try to cast down the artifact to the given type
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        <dyn Any>::downcast_ref::<T>(self.artifact.upcast_ref())
    }

    /// Try to cast down the artifact mutably to the given type
    pub fn downcast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        <dyn Any>::downcast_mut::<T>(self.artifact.upcast_mut())
    }

    /// Find the first artifact in an iterator that can be casted down to the given type
    pub fn find<'a, T: 'static>(
        artifacts: &mut dyn Iterator<Item = &'a AnyArtifact>,
    ) -> Option<&'a T> {
        for artifact in artifacts {
            if let Some(value) = artifact.downcast_ref::<T>() {
                return Some(value);
            }
        }
        None
    }

    /// Find all artifacts in an iterator that can be casted down to the given type
    pub fn find_all<'a, T: 'static>(
        artifacts: &'a mut (dyn std::iter::Iterator<Item = &'a AnyArtifact> + 'a),
    ) -> impl Iterator<Item = &'a T> {
        artifacts.filter_map(|a| a.downcast_ref::<T>())
    }

    /// Serialize inner artifact without the `AnyArtifact` container
    pub fn serialize_inner(&self, serializer: &mut dyn ErasedSerializer) -> Result<()> {
        Ok(self.artifact.erased_serialize(serializer).map(|_| ())?)
    }
}

impl<T: Artifact> From<T> for AnyArtifact {
    fn from(artifact: T) -> Self {
        AnyArtifact {
            artifact: Box::new(artifact),
        }
    }
}
