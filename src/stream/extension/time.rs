/// The standard time extension
use std::fmt::Debug;
use std::ops::Neg;

use chrono::Duration;

use crate::error::Result;
use crate::stream::extension::{Attributes, Extension};
use crate::stream::filter::Condition;
use crate::stream::validator::ValidatorFn;
use crate::stream::{ComponentType, Meta};
use crate::{DateTime, Error};

#[derive(Debug)]
pub enum TimeType<'a> {
    Timestamp(&'a DateTime),
    Interval((&'a DateTime, &'a DateTime)),
}

impl TimeType<'_> {
    fn interval(&self) -> (&DateTime, &DateTime) {
        match self {
            TimeType::Timestamp(time) => (time, time),
            TimeType::Interval((t1, t2)) => (t1, t2),
        }
    }

    fn duration(t1: &DateTime, t2: &DateTime) -> Duration {
        let mut duration = t1.signed_duration_since(*t2);

        if duration < chrono::Duration::seconds(0) {
            duration = duration.neg()
        }

        duration
    }

    fn is_eq(&self, other: &TimeType) -> bool {
        let (t1, t2) = self.interval();
        let (t3, t4) = other.interval();
        t1 == t3 && t2 == t4
    }

    fn is_eq_tol(&self, other: &TimeType, tolerance: &Duration) -> bool {
        let (t1, t2) = self.interval();
        let (t3, t4) = other.interval();
        Self::duration(t1, t3) <= *tolerance && Self::duration(t2, t4) <= *tolerance
    }

    fn is_before(&self, other: &TimeType) -> bool {
        let (_, t2) = self.interval();
        let (t3, _) = other.interval();
        t2 < t3
    }

    fn is_after(&self, other: &TimeType) -> bool {
        let (t1, _) = self.interval();
        let (_, t4) = other.interval();
        t4 < t1
    }

    fn is_in(&self, other: &TimeType) -> bool {
        let (t1, t2) = self.interval();
        let (t3, t4) = other.interval();
        t3 <= t1 && t2 <= t4
    }

    fn starts_in(&self, other: &TimeType) -> bool {
        let (t1, _) = self.interval();
        let (t3, t4) = other.interval();
        t3 <= t1 && t1 <= t4
    }

    fn ends_in(&self, other: &TimeType) -> bool {
        let (_, t2) = self.interval();
        let (t3, t4) = other.interval();
        t3 <= t2 && t2 <= t4
    }
}

#[derive(Debug)]
pub struct Time<'a> {
    pub time: TimeType<'a>,
    origin: ComponentType,
}

impl<'a> Extension<'a> for Time<'a> {
    const NAME: &'static str = "Time";
    const PREFIX: &'static str = "time";
    const URI: &'static str = "http://www.xes-standard.org/time.xesext";

    fn view<T: Attributes + ?Sized>(component: &'a T) -> Result<Self> {
        let origin = component.hint();
        let time = match origin {
            ComponentType::Event => {
                TimeType::Timestamp(component.get_or("time:timestamp")?.try_date()?)
            }
            ComponentType::Trace => match &component.children()[..] {
                [] => return Err(Error::ExtensionError("no interval found".to_string())),
                [x] => {
                    let x = x.get_or("time:timestamp")?.try_date()?;
                    TimeType::Interval((x, x))
                }
                [x, .., y] => {
                    let x = x.get_or("time:timestamp")?.try_date()?;
                    let y = y.get_or("time:timestamp")?.try_date()?;

                    if x > y {
                        return Err(Error::ExtensionError(format!(
                            "invalid interval {:?}",
                            (x, y)
                        )));
                    }

                    TimeType::Interval((x, y))
                }
            },
            other => {
                return Err(Error::ExtensionError(format!(
                    "time extension does not support {:?}",
                    other
                )))
            }
        };

        Ok(Time { time, origin })
    }

    fn validator(_meta: &Meta) -> ValidatorFn {
        Box::new(|x| {
            let children = x.children();

            for slice in children[..].windows(2) {
                match slice {
                    [a, b] => {
                        let ts1 = Time::view(*a)?.time;
                        let ts2 = Time::view(*b)?.time;

                        if ts2.is_before(&ts1) {
                            return Err(Error::ValidationError(format!(
                                "at least two child components of \"{:?}\" appear not to be in chronological order ({:?}, {:?})",
                                x.hint(), ts1, ts2
                            )));
                        }
                    }
                    _ => unreachable!(),
                }
            }

            Ok(())
        })
    }
}

impl Time<'_> {
    pub fn filter_eq<'a, T: 'a + Attributes>(other: &'a TimeType) -> Condition<'a, T> {
        Box::new(move |x: &T| Ok(Time::view(x)?.time.is_eq(other)))
    }

    pub fn filter_eq_tol<'a, T: 'a + Attributes>(
        other: &'a TimeType,
        tolerance: &'a Duration,
    ) -> Condition<'a, T> {
        Box::new(move |x: &T| Ok(Time::view(x)?.time.is_eq_tol(other, tolerance)))
    }

    pub fn filter_before<'a, T: 'a + Attributes>(other: &'a TimeType) -> Condition<'a, T> {
        Box::new(move |x: &T| Ok(Time::view(x)?.time.is_before(other)))
    }

    pub fn filter_after<'a, T: 'a + Attributes>(other: &'a TimeType) -> Condition<'a, T> {
        Box::new(move |x: &T| Ok(Time::view(x)?.time.is_after(&other)))
    }

    pub fn filter_in<'a, T: 'a + Attributes>(other: &'a TimeType) -> Condition<'a, T> {
        Box::new(move |x: &T| Ok(Time::view(x)?.time.is_in(other)))
    }

    pub fn filter_starts_in<'a, T: 'a + Attributes>(other: &'a TimeType) -> Condition<'a, T> {
        Box::new(move |x: &T| Ok(Time::view(x)?.time.starts_in(other)))
    }

    pub fn filter_ends_in<'a, T: 'a + Attributes>(other: &'a TimeType) -> Condition<'a, T> {
        Box::new(move |x: &T| Ok(Time::view(x)?.time.ends_in(other)))
    }
}

#[cfg(test)]
mod tests {
    use crate::dev_util::load_example;
    use crate::stream::filter::tests::test_filter;
    use crate::stream::observer::Handler;
    use crate::stream::validator::Validator;
    use crate::stream::{void::consume, Component, Stream};

    use super::*;

    #[test]
    fn test_view() {
        let mut buffer = load_example(&["non_validating", "event_incorrect_order.xes"]);

        while let Some(component) = buffer.next().unwrap() {
            match component {
                Component::Meta(meta) => assert!(Time::view(&meta).is_err()),
                Component::Trace(trace) => assert!(Time::view(&trace).is_err()),
                Component::Event(event) => assert!(Time::view(&event).is_ok()),
            }
        }
    }

    #[test]
    fn test_filter_eq() {
        let a = DateTime::parse_from_rfc3339("1987-07-28T13:37:42.000+00:00").unwrap();
        let b = DateTime::parse_from_rfc3339("1987-07-28T13:40:42.000+00:00").unwrap();

        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![],
            vec![vec![
                Time::filter_eq(&TimeType::Timestamp(&a)),
                Time::filter_eq(&TimeType::Interval((&b, &b))),
            ]],
            "[][dg][][][][]",
            None,
        );
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![vec![Time::filter_eq(&TimeType::Interval((&a, &b)))]],
            vec![],
            "[defg]",
            None,
        );
    }

    #[test]
    fn test_filter_eq_tol() {
        let a = DateTime::parse_from_rfc3339("1987-07-29T13:37:42.000+00:00").unwrap();
        let b = DateTime::parse_from_rfc3339("1987-07-30T13:40:42.000+00:00").unwrap();

        let tolerance = Duration::seconds(90);
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![],
            vec![vec![
                Time::filter_eq_tol(&TimeType::Timestamp(&a), &tolerance),
                Time::filter_eq_tol(&TimeType::Interval((&b, &b)), &tolerance),
            ]],
            "[][][hi][no][][]",
            None,
        );

        let tolerance = Duration::minutes(5);
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![vec![Time::filter_eq_tol(
                &TimeType::Timestamp(&a),
                &tolerance,
            )]],
            vec![],
            "[hijk]",
            None,
        );
    }

    #[test]
    fn test_filter_before_after() {
        let a = DateTime::parse_from_rfc3339("1987-07-29T13:37:42.000+00:00").unwrap();
        let b = DateTime::parse_from_rfc3339("1987-07-30T13:38:42.000+00:00").unwrap();

        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![],
            vec![vec![
                Time::filter_before(&TimeType::Timestamp(&a)),
                Time::filter_after(&TimeType::Interval((&a, &b))),
            ]],
            "[abc][defg][][no][pqrs][tuvw]",
            None,
        );
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![vec![
                Time::filter_before(&TimeType::Interval((&a, &b))),
                Time::filter_after(&TimeType::Timestamp(&b)),
            ]],
            vec![],
            "[abc][defg][pqrs][tuvw]",
            None,
        );
    }

    #[test]
    fn test_filter_starts_ends_in() {
        let a = DateTime::parse_from_rfc3339("1987-07-28T13:37:42.000+00:00").unwrap();
        let b = DateTime::parse_from_rfc3339("1987-07-28T13:40:42.000+00:00").unwrap();
        let c = DateTime::parse_from_rfc3339("1987-07-29T13:39:12.000+00:00").unwrap();
        let d = DateTime::parse_from_rfc3339("1987-07-30T13:39:12.000+00:00").unwrap();

        // test in
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![],
            vec![vec![
                Time::filter_in(&TimeType::Timestamp(&a)),
                Time::filter_in(&TimeType::Interval((&b, &c))),
            ]],
            "[][dg][hi][][][]",
            None,
        );
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![vec![Time::filter_in(&TimeType::Interval((&a, &d)))]],
            vec![],
            "[defg][hijk]",
            None,
        );

        // test starts in
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![],
            vec![vec![Time::filter_starts_in(&TimeType::Interval((&c, &d)))]],
            "[][][jk][lm][][]",
            None,
        );
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![vec![Time::filter_starts_in(&TimeType::Interval((&c, &d)))]],
            vec![],
            "[lmno]",
            None,
        );

        // test ends in
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![],
            vec![vec![Time::filter_ends_in(&TimeType::Interval((&c, &d)))]],
            "[][][jk][lm][][]",
            None,
        );
        test_filter(
            load_example(&["test", "extension_full.xes"]),
            vec![vec![Time::filter_ends_in(&TimeType::Interval((&c, &d)))]],
            vec![],
            "[hijk]",
            None,
        );
    }

    #[test]
    fn test_validation() {
        let buffer = load_example(&["non_validating", "event_incorrect_order.xes"]);
        let mut validator = Validator::default().into_observer(buffer);

        if let Err(Error::ValidationError(msg)) = consume(&mut validator) {
            assert!(msg.contains(r#"at least two child components of "Trace" appear not to be in chronological order (Timestamp(2000-01-01T00:00:00+00:00), Timestamp(1999-01-01T00:00:00+00:00))"#))
        } else {
            panic!("expected validation error")
        }
    }
}
