//! Try to minor but common errors that appear in the wild
//!
//! By now, the following error classes are covered:
//! - fix invalid classifier names
//!

use crate::stream::observer::{Handler, Observer};
use crate::stream::plugin::{Declaration, Entry, Factory, FactoryType, PluginProvider};
use crate::stream::xml_util::CRE_NCNAME;
use crate::stream::{Meta, Stream};
use crate::Result;

/// Collection of stream repair strategies
pub struct Repair;

impl Default for Repair {
    fn default() -> Self {
        Repair {}
    }
}

impl Handler for Repair {
    fn on_meta(&mut self, mut meta: Meta) -> Result<Meta> {
        // try to fix classifier names
        for classifier_decl in meta.classifiers.iter_mut() {
            if !CRE_NCNAME.is_match(&classifier_decl.name) {
                let fixed = classifier_decl.name.replace(" ", "");
                debug!(
                    "try fix ClassifierDecl.name: {:?} --> {:?}",
                    &classifier_decl.name, &fixed
                );
                classifier_decl.name = fixed;
            }
        }

        Ok(meta)
    }
}

impl PluginProvider for Repair {
    fn entries() -> Vec<Entry>
    where
        Self: Sized,
    {
        vec![Entry::new(
            "Repair",
            "Applies a number of methods in order to fix broken items such as invalid names",
            Factory::new(
                Declaration::default().stream("inner", "The stream to be repaired"),
                FactoryType::Stream(Box::new(|parameters| -> Result<Box<dyn Stream>> {
                    Ok(
                        Observer::from((parameters.acquire_stream("inner")?, Repair::default()))
                            .into_boxed(),
                    )
                })),
            ),
        )]
    }
}

#[cfg(test)]
mod test {
    use crate::dev_util::load_example;
    use crate::stream::validator::Validator;
    use crate::stream::void::consume;

    use super::*;

    #[test]
    fn test_repair() {
        let broken = ["classifier_incorrect_names.xes"];

        for file in broken.iter() {
            let buffer = load_example(&["non_validating", file]);

            let mut raw = Validator::default().into_observer(buffer.clone());
            let mut repaired =
                Validator::default().into_observer(Repair::default().into_observer(buffer));

            assert!(consume(&mut raw).is_err());
            assert!(consume(&mut repaired).is_ok());
        }
    }
}
