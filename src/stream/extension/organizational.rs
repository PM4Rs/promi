/// The standard time extension
use regex::Regex;

use crate::error::{Error, Result};
use crate::stream::extension::Extension;
use crate::stream::filter::Condition;
use crate::stream::validator::ValidatorFn;
use crate::stream::{AttributeContainer, ComponentType, Meta};

#[derive(Debug)]
pub enum OrgKey {
    Resource,
    Role,
    Group,
}

pub struct Org<'a> {
    pub resource: Option<&'a str>,
    pub role: Option<&'a str>,
    pub group: Option<&'a str>,
    origin: ComponentType,
}

impl<'a> Extension<'a> for Org<'a> {
    const NAME: &'static str = "Organizational";
    const PREFIX: &'static str = "org";
    const URI: &'static str = "http://www.xes-standard.org/org.xesext";

    fn view<T: AttributeContainer + ?Sized>(component: &'a T) -> Result<Self> {
        let mut org = Org {
            resource: None,
            role: None,
            group: None,
            origin: component.hint(),
        };

        // only events are supported
        if ComponentType::Event == org.origin {
            // extract resource
            if let Some(name) = component.get_value("org:resource") {
                org.resource = Some(name.try_string()?)
            }

            // extract role
            if let Some(name) = component.get_value("org:role") {
                org.role = Some(name.try_string()?)
            }

            // extract group
            if let Some(name) = component.get_value("org:group") {
                org.group = Some(name.try_string()?)
            }
        }

        Ok(org)
    }

    fn validator(_meta: &Meta) -> ValidatorFn {
        Box::new(|x| {
            let _ = Org::view(*x)?;
            // since all error classes are caught until creation of an org instance there's nothing
            // else to do here :)
            Ok(())
        })
    }
}

impl Org<'_> {
    pub const RESOURCE: &'static OrgKey = &OrgKey::Resource;
    pub const ROLE: &'static OrgKey = &OrgKey::Role;
    pub const GROUP: &'static OrgKey = &OrgKey::Group;

    fn by_key(&self, attr: &OrgKey) -> Option<&str> {
        match attr {
            OrgKey::Resource => self.resource,
            OrgKey::Role => self.role,
            OrgKey::Group => self.group,
        }
    }

    /// Condition factory that returns a function which checks if an org equals the given value
    pub fn filter_eq<'a, T: 'a + AttributeContainer>(
        key: &'a OrgKey,
        value: &'a str,
    ) -> Condition<'a, T> {
        Box::new(move |x: &T| match Org::view(x)?.by_key(key) {
            Some(value_) => Ok(value_ == value),
            None => Err(Error::AttributeError(format!("{:?} is not defined", key))),
        })
    }

    /// Condition factory that returns a function which checks if an org value is in the given list
    pub fn filter_in<'a, T: 'a + AttributeContainer>(
        key: &'a OrgKey,
        values: &'a [&str],
    ) -> Condition<'a, T> {
        Box::new(move |x: &T| match Org::view(x)?.by_key(key) {
            Some(value) => Ok(values.iter().any(|n| *n == value)),
            None => Err(Error::AttributeError(format!("{:?} is not defined", key))),
        })
    }

    /// Condition factory that returns a function which checks if an org matches given regex
    pub fn filter_match<'a, T: 'a + AttributeContainer>(
        key: &'a OrgKey,
        pattern: &'a Regex,
    ) -> Condition<'a, T> {
        Box::new(move |x: &T| match Org::view(x)?.by_key(key) {
            Some(value) => Ok(pattern.is_match(value)),
            None => Err(Error::AttributeError(format!("{:?} is not defined", key))),
        })
    }
}

#[cfg(test)]
pub mod tests {
    use crate::dev_util::load_example;
    use crate::stream::filter::drop_err;
    use crate::stream::filter::tests::test_filter;
    use crate::stream::{Component, Stream};

    use super::*;

    #[test]
    fn test_view() {
        let mut buffer = load_example(&["correct", "event_correct_attributes.xes"]);

        while let Some(component) = buffer.next().unwrap() {
            match component {
                Component::Trace(trace) => {
                    let view = Org::view(&trace).unwrap();
                    assert!(view.resource.is_none());
                    assert!(view.role.is_none());
                    assert!(view.group.is_none());
                }
                Component::Event(_event) => (),
                _ => (),
            }
        }
    }

    #[test]
    fn test_filter_eq_in() {
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![],
            vec![
                vec![
                    Org::filter_in(Org::RESOURCE, &["A", "B"]),
                    Org::filter_eq(Org::RESOURCE, "C"),
                    Org::filter_eq(Org::RESOURCE, "D"),
                ],
                vec![
                    Org::filter_eq(Org::RESOURCE, "B"),
                    Org::filter_eq(Org::RESOURCE, "C"),
                    Org::filter_in(Org::RESOURCE, &["D", "E"]),
                ],
            ],
            "[BC][D][][][][]",
            Some(Box::new(|component: &dyn AttributeContainer| {
                Ok(Org::view(component)
                    .unwrap()
                    .resource
                    .unwrap_or("?")
                    .to_string())
            })),
        );
    }

    #[test]
    fn test_filter_match() {
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![],
            vec![
                vec![drop_err(Org::filter_match(
                    Org::ROLE,
                    &Regex::new(r#"[123]"#).unwrap(),
                ))],
                vec![drop_err(Org::filter_match(
                    Org::GROUP,
                    &Regex::new(r#"[678]"#).unwrap(),
                ))],
            ],
            "[][23][][][][]",
            Some(Box::new(|component: &dyn AttributeContainer| {
                Ok(Org::view(component)
                    .unwrap()
                    .role
                    .unwrap_or("?")
                    .to_string())
            })),
        );
    }
}
