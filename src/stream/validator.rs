//! Validate an event stream's semantic correctness
//!
//! Not all error cases are caught by other parts of an event stream pipeline. The `Validator` aims
//! for doing the remaining checks. For now, that includes
//! - checking for attributes enforced by globals
//! - semantic validation via extensions
//!

// standard library

// third party

// local
use crate::stream::extension::REGISTRY;
use crate::stream::observer::Handler;
use crate::stream::{Attributes, Event, Meta, Scope, Trace};
use crate::{Error, Result};

pub type ValidatorFn = Box<dyn Fn(Box<&dyn Attributes>) -> Result<()> + Send>;

/// Container for validator functions
pub struct Validator {
    validators: Vec<ValidatorFn>,
    trace_only: Vec<ValidatorFn>,
    event_only: Vec<ValidatorFn>,
}

impl Default for Validator {
    fn default() -> Self {
        Validator {
            validators: Vec::new(),
            trace_only: Vec::new(),
            event_only: Vec::new(),
        }
    }
}

impl Handler for Validator {
    fn on_meta(&mut self, meta: Meta) -> Result<Meta> {
        let registry = REGISTRY.lock().map_err(|_| {
            Error::ExtensionError("unable to acquire extension registry".to_string())
        })?;

        // generate extension validators
        for extension_decl in meta.extensions.iter() {
            if let Some(entry) = registry.get(extension_decl.prefix.as_str()) {
                self.validators.push(entry.validator(&meta));
            } else {
                eprintln!(
                    "{:?} extension is not supported and therefore not validated",
                    extension_decl.name
                )
            }
        }

        // generate globals validators
        for global in meta.globals.iter() {
            let global = global.clone();
            match global.scope {
                Scope::Trace => self.trace_only.push(Box::new(move |x| global.validate(*x))),
                Scope::Event => self.event_only.push(Box::new(move |x| global.validate(*x))),
            }
        }

        // validate meta against extensions
        for validator in self.validators.iter() {
            validator(Box::new(&meta))?;
        }

        Ok(meta)
    }

    fn on_trace(&mut self, trace: Trace) -> Result<Option<Trace>> {
        for validator in self.trace_only.iter().chain(self.validators.iter()) {
            validator(Box::new(&trace))?
        }

        Ok(Some(trace))
    }

    fn on_event(&mut self, event: Event, _in_trace: bool) -> Result<Option<Event>> {
        for validator in self.event_only.iter().chain(self.validators.iter()) {
            validator(Box::new(&event))?;
        }

        Ok(Some(event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_util::load_example;
    use crate::stream::consume;
    use crate::stream::observer::Observer;

    #[test]
    fn test_globals_validation() {
        let buffer = load_example(&["test", "extension_full.xes"]);
        let mut validator = Observer::from((buffer, Validator::default()));
        consume(&mut validator).expect("validation is expected to succeed");

        let buffer = load_example(&["non_validating", "globals_violation_type.xes"]);
        let mut validator = Observer::from((buffer, Validator::default()));

        if let Err(Error::ValidationError(msg)) = consume(&mut validator) {
            assert!(msg.contains(r#"Couldn't find an attribute with key ""lifecycle:transition"""#))
        } else {
            panic!("expected validation error")
        }

        let buffer = load_example(&["non_validating", "event_incorrect_type.xes"]);
        let mut validator = Observer::from((buffer, Validator::default()));

        if let Err(Error::ValidationError(msg)) = consume(&mut validator) {
            assert!(msg
                .contains(r#"Expected ""org:resource"" to be of type String but got Int instead"#));
        } else {
            panic!("expected validation error")
        }
    }
}
