//! Extensions
//!
//! This module defines a general interface for extensions and provides implementations of the
//! standard extensions defined by the XES standard.
//!
//! From [IEEE Std 1849-2016](https://standards.ieee.org/standard/1849-2016.html):
//! > An extension defines a (possibly empty) set of attributes for every type of component.
//! > The extension provides points of reference for interpreting these attributes, and, thus, their
//! > components. Extensions, therefore, are primarily a vehicle for attaching semantics to a set of
//! > defined attributes per component.
//!

// modules
pub mod concept;
pub mod organizational;
pub mod time;

// standard library
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

// third party

// local
use crate::stream::validator::ValidatorFn;
use crate::stream::{Attributes, ExtensionDecl, Meta};
use crate::{Error, Result};

// expose extensions
pub use concept::Concept;
pub use organizational::Org;
pub use time::Time;

/// Helper struct that holds references to object safe parts of an extension
pub struct RegistryEntry {
    key: &'static str,
    _declare: Box<dyn Fn() -> ExtensionDecl + Send>,
    _validator: Box<dyn Fn(&Meta) -> ValidatorFn + Send>,
}

impl RegistryEntry {
    /// Wrapper for an extension's declare method
    pub fn declare(&self) -> ExtensionDecl {
        (self._declare)()
    }

    /// Wrapper for an extension's validator method
    pub fn validator(&self, meta: &Meta) -> ValidatorFn {
        (self._validator)(meta)
    }
}

impl fmt::Debug for RegistryEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryEntry")
            .field("key", &self.key)
            .finish()
    }
}

/// Lookup table for available extensions
#[derive(Debug)]
pub struct Registry {
    extensions: HashMap<String, RegistryEntry>,
}

impl Registry {
    /// Register an extension in registry
    pub fn register(&mut self, entry: RegistryEntry) {
        self.extensions.insert(entry.key.to_string(), entry);
    }

    /// Get an extension by its prefix
    pub fn get(&self, key: &str) -> Option<&RegistryEntry> {
        self.extensions.get(key)
    }

    /// Remove extension from registry and return it
    pub fn remove(&mut self, key: &str) -> Option<RegistryEntry> {
        self.extensions.remove(key)
    }
}

impl Registry {
    /// Create a new, empty registry
    pub fn new() -> Self {
        Self {
            extensions: HashMap::new(),
        }
    }
}

impl Default for Registry {
    fn default() -> Self {
        let mut registry = Self::new();

        registry.register(Concept::registry_entry());
        registry.register(Org::registry_entry());
        registry.register(Time::registry_entry());

        registry
    }
}

lazy_static! {
    /// The default extension registry
    pub static ref REGISTRY: Mutex<Registry> = Mutex::new(Registry::default());
}

/// Enrich an event stream with semantics
///
/// An event stream may contain all kinds of unstructured data in its attributes. An Extension
/// provides these with semantics by 'viewing' the attributes from the extension's perspective.
/// Apart from that, extensions provide functionality for generating attributes, attribute
/// validation, filtering and statistics.
///
pub trait Extension<'a> {
    const NAME: &'static str;
    const PREFIX: &'static str;
    const URI: &'static str;

    /// Get a extension specific view on a viewable (Meta, Trace, Event etc.)
    fn view<T: Attributes + ?Sized>(view: &'a T) -> Result<Self>
    where
        Self: Sized;

    /// Generate an extension declaration for events stream's meta element
    fn declare() -> ExtensionDecl {
        ExtensionDecl {
            name: Self::NAME.to_string(),
            prefix: Self::PREFIX.to_string(),
            uri: Self::URI.to_string(),
        }
    }

    /// Generate a validation function from stream meta data
    fn validator(_meta: &Meta) -> ValidatorFn;

    /// Generate an entry as used for extension registries
    fn registry_entry() -> RegistryEntry
    where
        Self: 'static,
    {
        RegistryEntry {
            key: Self::PREFIX,
            _declare: Box::new(Self::declare),
            _validator: Box::new(Self::validator),
        }
    }

    /// Register extension in global registry
    fn register() -> Result<()>
    where
        Self: 'static,
    {
        let mut registry = REGISTRY.lock().map_err(|_| {
            Error::ExtensionError("unable to acquire extension registry".to_string())
        })?;
        registry.register(Self::registry_entry());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry() {
        let keys = &["concept", "time"];
        let registry = REGISTRY.lock().unwrap();
        let mut extensions: Vec<ExtensionDecl> = Vec::new();

        // use extensions via registry
        for key in keys {
            let entry = registry.extensions.get(*key).unwrap();

            extensions.push(entry.declare());
        }

        // release registry
        drop(registry);

        // test
        for (key, extension) in keys.iter().zip(extensions) {
            assert_eq!(*key, extension.prefix.as_str());
        }
    }

    #[test]
    fn test_registry_register() {
        let mut registry = REGISTRY.lock().unwrap();

        // check presence of extension
        assert!(registry.get("concept").is_some());

        // let's drop an extension
        let entry = registry.remove("concept").unwrap();
        assert_eq!(entry.declare().prefix.as_str(), "concept");
        assert!(registry.get("concept").is_none());

        // and insert an equivalent extension after releasing registry
        drop(registry);
        Concept::register().unwrap();

        // check for the change
        let registry = REGISTRY.lock().unwrap();
        assert!(registry.get("concept").is_some());
    }
}
