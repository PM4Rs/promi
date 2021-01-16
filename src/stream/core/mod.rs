//! Core data structures and traits
pub mod artifact;
pub mod attribute;
pub mod component;
pub mod sink;
pub mod stream;

#[cfg(test)]
pub mod tests {
    use component::Component;
    use sink::Sink;

    use crate::{Error, Result};

    use super::*;

    #[derive(Debug)]
    pub struct TestSink {
        ct_open: usize,
        ct_component: usize,
        ct_close: usize,
        ct_error: usize,
    }

    impl Default for TestSink {
        fn default() -> Self {
            TestSink {
                ct_open: 0,
                ct_component: 0,
                ct_close: 0,
                ct_error: 0,
            }
        }
    }

    impl Sink for TestSink {
        fn on_open(&mut self) -> Result<()> {
            self.ct_open += 1;
            Ok(())
        }

        fn on_component(&mut self, _: Component) -> Result<()> {
            self.ct_component += 1;
            Ok(())
        }

        fn on_close(&mut self) -> Result<()> {
            self.ct_close += 1;
            Ok(())
        }

        fn on_error(&mut self, _: Error) -> Result<()> {
            self.ct_error += 1;
            Ok(())
        }
    }

    impl TestSink {
        pub fn counts(&self) -> [usize; 4] {
            [
                self.ct_open,
                self.ct_component,
                self.ct_close,
                self.ct_error,
            ]
        }
    }
}
