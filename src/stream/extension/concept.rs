/// The standard time extension
use regex::Regex;

use crate::error::{Error, Result};
use crate::stream::extension::{Attributes, Extension};
use crate::stream::filter::Condition;
use crate::stream::validator::ValidatorFn;
use crate::stream::{ComponentType, Meta};

#[derive(Debug)]
pub enum ConceptKey {
    Name,
    Instance,
}

pub struct Concept<'a> {
    pub name: Option<&'a str>,
    pub instance: Option<&'a str>,
    origin: ComponentType,
}

impl<'a> Extension<'a> for Concept<'a> {
    const NAME: &'static str = "Concept";
    const PREFIX: &'static str = "concept";
    const URI: &'static str = "http://www.xes-standard.org/concept.xesext";

    fn view<T: Attributes + ?Sized>(component: &'a T) -> Result<Self> {
        let mut concept = Concept {
            name: None,
            instance: None,
            origin: component.hint(),
        };

        // extract name
        if let Some(name) = component.get("concept:name") {
            concept.name = Some(name.try_string()?)
        }

        // extract instance
        if ComponentType::Event == concept.origin {
            if let Some(instance) = component.get("concept:instance") {
                concept.instance = Some(instance.try_string()?)
            }
        }

        Ok(concept)
    }

    fn validator(_meta: &Meta) -> ValidatorFn {
        Box::new(|x| {
            let _ = Concept::view(*x)?;
            // since all error classes are caught during creation of a concept instance there's
            // nothing else to do here :)
            Ok(())
        })
    }
}

impl Concept<'_> {
    pub const NAME: &'static ConceptKey = &ConceptKey::Name;
    pub const INSTANCE: &'static ConceptKey = &ConceptKey::Instance;

    fn by_key(&self, attr: &ConceptKey) -> Option<&str> {
        match attr {
            ConceptKey::Name => self.name,
            ConceptKey::Instance => self.instance,
        }
    }

    /// Condition factory that returns a function which checks if a concept equals the given value
    pub fn filter_eq<'a, T: 'a + Attributes>(
        key: &'a ConceptKey,
        value: &'a str,
    ) -> Condition<'a, T> {
        Box::new(move |x: &T| match Concept::view(x)?.by_key(key) {
            Some(value_) => Ok(value_ == value),
            None => Err(Error::AttributeError(format!("{:?} is not defined", key))),
        })
    }

    /// Condition factory that returns a function which checks if a concept value is in the given list
    pub fn filter_in<'a, T: 'a + Attributes>(
        key: &'a ConceptKey,
        values: &'a [&str],
    ) -> Condition<'a, T> {
        Box::new(move |x: &T| match Concept::view(x)?.by_key(key) {
            Some(value) => Ok(values.iter().any(|n| *n == value)),
            None => Err(Error::AttributeError(format!("{:?} is not defined", key))),
        })
    }

    /// Condition factory that returns a function which checks if a concept matches given regex
    pub fn filter_match<'a, T: 'a + Attributes>(
        key: &'a ConceptKey,
        pattern: &'a Regex,
    ) -> Condition<'a, T> {
        Box::new(move |x: &T| match Concept::view(x)?.by_key(key) {
            Some(value) => Ok(pattern.is_match(value)),
            None => Err(Error::AttributeError(format!("{:?} is not defined", key))),
        })
    }
}

#[cfg(test)]
pub mod tests {
    use regex::Regex;

    use crate::dev_util::load_example;
    use crate::stream::filter::tests::test_filter;
    use crate::stream::{Component, Stream};

    use super::*;

    #[test]
    fn test_view() {
        let mut buffer = load_example(&["correct", "event_correct_attributes.xes"]);

        while let Some(component) = buffer.next().unwrap() {
            match component {
                Component::Trace(trace) => assert!(Concept::view(&trace).unwrap().name.is_none()),
                Component::Event(event) => assert!(Concept::view(&event).unwrap().name.is_some()),
                _ => (),
            }
        }
    }

    #[test]
    fn test_filter_eq_in() {
        let param = [
            ("L1.xes", "[d][cbd][bcd][bcd][bcd][cbd]"),
            (
                "L2.xes",
                "[cbd][cbbcd][cbbcd][cbd][cbd][cbd][bcbcd][bcbcd][bcd][bccbd][bcd][bcd][cbbccbd]",
            ),
            ("L3.xes", "[bcdbcdbdc][bdc][bcdbdc][bdc]"),
            (
                "L5.xes",
                "[bcdb][bcdb][bcdb][bcdb][bcdb][bcdb][bcdb][bcdb][bcdb][bcdb][bcdb][bcdb][b][b]",
            ),
        ];

        for (f, s) in param.iter() {
            test_filter(
                load_example(&["book", f]),
                vec![],
                vec![
                    vec![
                        Concept::filter_in(Concept::NAME, &["a", "b"]),
                        Concept::filter_eq(Concept::NAME, "c"),
                        Concept::filter_eq(Concept::NAME, "d"),
                    ],
                    vec![
                        Concept::filter_eq(Concept::NAME, "b"),
                        Concept::filter_eq(Concept::NAME, "c"),
                        Concept::filter_in(Concept::NAME, &["d", "e"]),
                    ],
                ],
                s,
                None,
            );
        }
    }

    #[test]
    fn test_filter_match() {
        let param = [
            ("L1.xes", "[acbd][acbd]"),
            ("L2.xes", "[acbd][acbd][acbd][acbd]"),
            ("L3.xes", "[abdceg][abdceg]"),
            ("L5.xes", "[abecdbf][abecdbf][abecdbf]"),
        ];

        let p_case_1 = Regex::new(r#"Case1\.\d"#).unwrap();
        let p_case_2 = Regex::new(r#"Case2\.\d"#).unwrap();
        let p_case_3 = Regex::new(r#"Case3\.\d"#).unwrap();

        for (f, s) in param.iter() {
            test_filter(
                load_example(&["book", f]),
                vec![
                    vec![
                        Concept::filter_match(Concept::NAME, &p_case_1),
                        Concept::filter_match(Concept::NAME, &p_case_2),
                    ],
                    vec![
                        Concept::filter_match(Concept::NAME, &p_case_2),
                        Concept::filter_match(Concept::NAME, &p_case_3),
                    ],
                ],
                vec![],
                s,
                None,
            );
        }
    }
}
