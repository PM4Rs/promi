// standard library
use std::fmt::Debug;

// third party
use chrono::Duration;

// local
use crate::error::Result;
use crate::stream::extension::{Attributes, Extension};
use crate::stream::filter::Condition;
use crate::stream::ElementType;
use crate::{DateTime, Error};
use std::ops::Neg;

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
    origin: ElementType,
}

impl<'a> Extension<'a> for Time<'a> {
    const NAME: &'static str = "Time";
    const PREFIX: &'static str = "time";
    const URI: &'static str = "http://www.xes-standard.org/time.xesext";

    fn view<T: Attributes + ?Sized>(component: &'a T) -> Result<Self> {
        let origin = component.hint();
        let time = match origin {
            ElementType::Event => {
                TimeType::Timestamp(component.get_or("time:timestamp")?.try_date()?)
            }
            ElementType::Trace => match &component.children()[..] {
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
    use super::*;
    use crate::dev_util::load_example;
    use crate::stream::filter::tests::test_filter;
    use crate::stream::{Element, Stream};

    #[test]
    fn test_view() {
        let mut buffer = load_example(&["non_validating", "event_incorrect_order.xes"]);

        while let Some(element) = buffer.next().unwrap() {
            match element {
                Element::Meta(meta) => assert!(Time::view(&meta).is_err()),
                Element::Trace(trace) => assert!(Time::view(&trace).is_err()),
                Element::Event(event) => assert!(Time::view(&event).is_ok()),
            }
        }
    }

    #[test]
    fn test_filter_eq() {
        let buffer = load_example(&["test", "extension_full.xes"]);
        let a = DateTime::parse_from_rfc3339("1987-07-28T13:37:42.000+00:00").unwrap();
        let b = DateTime::parse_from_rfc3339("1987-07-28T13:40:42.000+00:00").unwrap();

        test_filter(
            buffer.clone(),
            vec![],
            vec![vec![
                Time::filter_eq(&TimeType::Timestamp(&a)),
                Time::filter_eq(&TimeType::Interval((&b, &b))),
            ]],
            "[][dg][][][][]",
        );
        test_filter(
            buffer,
            vec![vec![Time::filter_eq(&TimeType::Interval((&a, &b)))]],
            vec![],
            "[defg]",
        );
    }

    #[test]
    fn test_filter_eq_tol() {
        let buffer = load_example(&["test", "extension_full.xes"]);
        let a = DateTime::parse_from_rfc3339("1987-07-29T13:37:42.000+00:00").unwrap();
        let b = DateTime::parse_from_rfc3339("1987-07-30T13:40:42.000+00:00").unwrap();

        let tolerance = Duration::seconds(90);
        test_filter(
            buffer.clone(),
            vec![],
            vec![vec![
                Time::filter_eq_tol(&TimeType::Timestamp(&a), &tolerance),
                Time::filter_eq_tol(&TimeType::Interval((&b, &b)), &tolerance),
            ]],
            "[][][hi][no][][]",
        );

        let tolerance = Duration::minutes(5);
        test_filter(
            buffer,
            vec![vec![Time::filter_eq_tol(
                &TimeType::Timestamp(&a),
                &tolerance,
            )]],
            vec![],
            "[hijk]",
        );
    }

    #[test]
    fn test_filter_before_after() {
        let buffer = load_example(&["test", "extension_full.xes"]);
        let a = DateTime::parse_from_rfc3339("1987-07-29T13:37:42.000+00:00").unwrap();
        let b = DateTime::parse_from_rfc3339("1987-07-30T13:38:42.000+00:00").unwrap();

        test_filter(
            buffer.clone(),
            vec![],
            vec![vec![
                Time::filter_before(&TimeType::Timestamp(&a)),
                Time::filter_after(&TimeType::Interval((&a, &b))),
            ]],
            "[abc][defg][][no][pqrs][tuvw]",
        );
        test_filter(
            buffer.clone(),
            vec![vec![
                Time::filter_before(&TimeType::Interval((&a, &b))),
                Time::filter_after(&TimeType::Timestamp(&b)),
            ]],
            vec![],
            "[abc][defg][pqrs][tuvw]",
        );
    }

    #[test]
    fn test_filter_starts_ends_in() {
        let buffer = load_example(&["test", "extension_full.xes"]);
        let a = DateTime::parse_from_rfc3339("1987-07-28T13:37:42.000+00:00").unwrap();
        let b = DateTime::parse_from_rfc3339("1987-07-28T13:40:42.000+00:00").unwrap();
        let c = DateTime::parse_from_rfc3339("1987-07-29T13:39:12.000+00:00").unwrap();
        let d = DateTime::parse_from_rfc3339("1987-07-30T13:39:12.000+00:00").unwrap();

        // test in
        test_filter(
            buffer.clone(),
            vec![],
            vec![vec![
                Time::filter_in(&TimeType::Timestamp(&a)),
                Time::filter_in(&TimeType::Interval((&b, &c))),
            ]],
            "[][dg][hi][][][]",
        );
        test_filter(
            buffer.clone(),
            vec![vec![Time::filter_in(&TimeType::Interval((&a, &d)))]],
            vec![],
            "[defg][hijk]",
        );

        // test starts in
        test_filter(
            buffer.clone(),
            vec![],
            vec![vec![Time::filter_starts_in(&TimeType::Interval((&c, &d)))]],
            "[][][jk][lm][][]",
        );
        test_filter(
            buffer.clone(),
            vec![vec![Time::filter_starts_in(&TimeType::Interval((&c, &d)))]],
            vec![],
            "[lmno]",
        );

        // test ends in
        test_filter(
            buffer.clone(),
            vec![],
            vec![vec![Time::filter_ends_in(&TimeType::Interval((&c, &d)))]],
            "[][][jk][lm][][]",
        );
        test_filter(
            buffer,
            vec![vec![Time::filter_ends_in(&TimeType::Interval((&c, &d)))]],
            vec![],
            "[hijk]",
        );
    }
}
