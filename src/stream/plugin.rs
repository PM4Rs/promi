use std::collections::HashMap;
use std::sync::Mutex;

use serde::export::fmt::Debug;

use crate::stream::channel::{StreamReceiver, StreamSender};
use crate::stream::duplicator::Duplicator;
use crate::stream::split::Split;
use crate::stream::stats::StatsCollector;
use crate::stream::validator::Validator;
use crate::stream::void::Void;
use crate::stream::xes::XesPlugins;
use crate::stream::{AnyArtifact, AttributeValue, Sink, Stream};
use crate::{Error, Result};

pub type AttrMap = HashMap<String, AttributeValue>;

pub struct Parameters<'a> {
    attributes: AttrMap,
    artifacts: HashMap<String, &'a mut AnyArtifact>,
    artifacts_extra: Vec<&'a mut AnyArtifact>,
    streams: HashMap<String, Box<dyn Stream + 'a>>,
    streams_extra: Vec<Box<dyn Stream + 'a>>,
    sinks: HashMap<String, Box<dyn Sink + 'a>>,
    sinks_extra: Vec<Box<dyn Sink + 'a>>,
}

impl<'a> Parameters<'a> {
    pub fn acquire_attribute(&mut self, key: &str) -> Result<AttributeValue> {
        self.attributes
            .remove(key)
            .ok_or_else(|| Error::StreamError(format!("no attribute {:?}", key)))
    }

    pub fn acquire_artifact(&mut self, key: &str) -> Result<&'a mut AnyArtifact> {
        self.artifacts
            .remove(key)
            .ok_or_else(|| Error::StreamError(format!("no artifact {:?}", key)))
    }

    pub fn acquire_artifact_extra(&mut self) -> Vec<&'a mut AnyArtifact> {
        self.artifacts_extra.drain(..).collect()
    }

    pub fn acquire_stream(&mut self, key: &str) -> Result<Box<dyn Stream + 'a>> {
        self.streams
            .remove(key)
            .ok_or_else(|| Error::StreamError(format!("no stream {:?}", key)))
    }

    pub fn acquire_stream_extra(&mut self) -> Vec<Box<dyn Stream + 'a>> {
        self.streams_extra.drain(..).collect()
    }

    pub fn acquire_sink(&mut self, key: &str) -> Result<Box<dyn Sink + 'a>> {
        self.sinks
            .remove(key)
            .ok_or_else(|| Error::StreamError(format!("no sink {:?}", key)))
    }

    pub fn acquire_sink_extra(&mut self) -> Vec<Box<dyn Sink + 'a>> {
        self.sinks_extra.drain(..).collect()
    }

    fn warn_non_empty(&self) {
        let remaining_attributes = self.attributes.len();
        if remaining_attributes > 0 {
            warn!("{} attributes remain unused", remaining_attributes)
        }

        let remaining_artifacts = self.artifacts.len() + self.artifacts_extra.len();
        if remaining_artifacts > 0 {
            warn!("{} artifacts remain unused", remaining_artifacts)
        }

        let remaining_streams = self.streams.len() + self.streams_extra.len();
        if remaining_streams > 0 {
            warn!("{} streams remain unused", remaining_streams)
        }

        let remaining_sinks = self.sinks.len() + self.sinks_extra.len();
        if remaining_sinks > 0 {
            warn!("{} sinks remain unused", remaining_sinks)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Declaration {
    attributes: Vec<(String, String, Option<AttributeValue>)>,
    artifacts: Vec<(String, String)>,
    streams: Vec<(String, String)>,
    sinks: Vec<(String, String)>,
}

impl Default for Declaration {
    fn default() -> Self {
        Declaration {
            attributes: vec![],
            artifacts: vec![],
            streams: vec![],
            sinks: vec![],
        }
    }
}

impl Declaration {
    pub fn attribute<S: Into<String>, D: Into<String>>(mut self, name: S, description: D) -> Self {
        self.attributes
            .push((name.into(), description.into(), None));
        self
    }

    pub fn default_attr<S: Into<String>, D: Into<String>>(
        mut self,
        name: S,
        description: D,
        default: AttributeValue,
    ) -> Self {
        self.attributes
            .push((name.into(), description.into(), Some(default)));
        self
    }

    pub fn artifact<S: Into<String>, D: Into<String>>(mut self, name: S, description: D) -> Self {
        self.artifacts.push((name.into(), description.into()));
        self
    }

    pub fn stream<S: Into<String>, D: Into<String>>(mut self, name: S, description: D) -> Self {
        self.streams.push((name.into(), description.into()));
        self
    }

    pub fn sink<S: Into<String>, D: Into<String>>(mut self, name: S, description: D) -> Self {
        self.sinks.push((name.into(), description.into()));
        self
    }

    fn make<'a>(
        &self,
        mut attributes: AttrMap,
        artifacts: &'a mut [AnyArtifact],
        streams: Vec<Box<dyn Stream + 'a>>,
        sinks: Vec<Box<dyn Sink + 'a>>,
    ) -> Result<Parameters<'a>> {
        let mut artifacts = artifacts.iter_mut();
        let mut streams = streams.into_iter();
        let mut sinks = sinks.into_iter();

        let mut attribute_map = HashMap::new();
        let mut artifact_map = HashMap::new();
        let mut stream_map = HashMap::new();
        let mut sink_map = HashMap::new();

        for (name, _, default) in self.attributes.iter() {
            attribute_map.insert(
                name.clone(),
                attributes
                    .remove(name)
                    .or_else(|| default.clone())
                    .ok_or_else(|| {
                        Error::StreamError(format!("attribute {:?} is missing", &name))
                    })?,
            );
        }

        attribute_map.extend(attributes.into_iter());

        for (i, (name, _)) in self.artifacts.iter().enumerate() {
            artifact_map.insert(
                name.clone(),
                artifacts.next().ok_or_else(|| {
                    Error::StreamError(format!("{}. artifact {:?} is missing", i, name))
                })?,
            );
        }

        for (i, (name, _)) in self.streams.iter().enumerate() {
            stream_map.insert(
                name.clone(),
                streams.next().ok_or_else(|| {
                    Error::StreamError(format!("{}. stream {:?} is missing", i, name))
                })?,
            );
        }

        for (i, (name, _)) in self.sinks.iter().enumerate() {
            sink_map.insert(
                name.clone(),
                sinks.next().ok_or_else(|| {
                    Error::StreamError(format!("{}. sink {:?} is missing", i, name))
                })?,
            );
        }

        Ok(Parameters {
            attributes: attribute_map,
            artifacts: artifact_map,
            artifacts_extra: artifacts.collect(),
            streams: stream_map,
            streams_extra: streams.collect(),
            sinks: sink_map,
            sinks_extra: sinks.collect(),
        })
    }
}

pub type StreamFactory =
    Box<dyn for<'a> Fn(&mut Parameters<'a>) -> Result<Box<dyn Stream + 'a>> + Send>;
pub type SinkFactory =
    Box<dyn for<'a> Fn(&mut Parameters<'a>) -> Result<Box<dyn Sink + 'a>> + Send>;

pub enum FactoryType {
    Stream(StreamFactory),
    Sink(SinkFactory),
}

pub struct Factory {
    declaration: Declaration,
    factory: FactoryType,
}

impl Factory {
    pub fn new(declaration: Declaration, factory: FactoryType) -> Self {
        Self {
            declaration,
            factory,
        }
    }

    pub fn build_stream<'a>(
        &self,
        attributes: AttrMap,
        artifacts: &'a mut [AnyArtifact],
        streams: Vec<Box<dyn Stream + 'a>>,
        sinks: Vec<Box<dyn Sink + 'a>>,
    ) -> Result<Box<dyn Stream + 'a>> {
        match &self.factory {
            FactoryType::Stream(factory) => {
                let mut parameters = self
                    .declaration
                    .make(attributes, artifacts, streams, sinks)?;
                let stream = factory(&mut parameters);
                parameters.warn_non_empty();
                stream
            }
            _ => Err(Error::StreamError(format!(""))),
        }
    }

    pub fn build_sink<'a>(
        &self,
        attributes: AttrMap,
        artifacts: &'a mut [AnyArtifact],
        streams: Vec<Box<dyn Stream + 'a>>,
        sinks: Vec<Box<dyn Sink + 'a>>,
    ) -> Result<Box<dyn Sink + 'a>> {
        match &self.factory {
            FactoryType::Sink(factory) => {
                let mut parameters = self
                    .declaration
                    .make(attributes, artifacts, streams, sinks)?;
                let sink = factory(&mut parameters);
                parameters.warn_non_empty();
                sink
            }
            _ => Err(Error::StreamError(format!(""))),
        }
    }
}

pub trait Plugin {
    fn entries() -> Vec<RegistryEntry>
    where
        Self: Sized;

    fn register_at(registry: &mut Registry)
    where
        Self: Sized,
    {
        for entry in Self::entries() {
            if let Some(e) = registry.insert(entry.name.clone(), entry) {
                debug!("overwrite registry entry: {:?}", e.name);
            }
        }
    }

    fn register() -> Result<()>
    where
        Self: Sized,
    {
        let mut registry = REGISTRY.lock().map_err(|_| {
            Error::StreamError("unable to acquire stream plugin registry".to_string())
        })?;

        Self::register_at(&mut registry);
        Ok(())
    }
}

pub struct RegistryEntry {
    name: String,
    description: String,
    pub factory: Factory,
}

impl RegistryEntry {
    pub fn new<N: Into<String>, D: Into<String>>(
        name: N,
        description: D,
        factory: Factory,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            factory,
        }
    }
}

pub type Registry = HashMap<String, RegistryEntry>;

lazy_static! {
    /// The default stream registry
    pub static ref REGISTRY: Mutex<Registry> = {
        let mut registry = HashMap::new();

        Void::register_at(&mut registry);
        Duplicator::register_at(&mut registry);
        StatsCollector::register_at(&mut registry);
        Validator::register_at(&mut registry);
        Split::register_at(&mut registry);
        StreamSender::register_at(&mut registry);
        StreamReceiver::register_at(&mut registry);
        XesPlugins::register_at(&mut registry);

        Mutex::new(registry)
    };
}

pub fn log_plugins() -> Result<()> {
    let registry = REGISTRY
        .lock()
        .map_err(|_| Error::StreamError("unable to acquire stream plugin registry".to_string()))?;

    let mut entries: Vec<_> = registry.iter().collect();
    entries.sort_by_key(|e| e.0);

    info!("Installed Plugins:");
    for (i, (_, entry)) in entries.into_iter().enumerate() {
        let declaration = &entry.factory.declaration;

        info!("{:>2}. {}", i + 1, entry.name);
        info!("    {:?}", entry.description);

        for (name, description, default) in declaration.attributes.iter() {
            let default_str = default
                .as_ref()
                .map(|v| format!("[{:?}]", v))
                .unwrap_or_else(|| "".into());
            info!("    ATR: {:>8}: {:?} {}", name, description, default_str)
        }

        for (name, description) in declaration.artifacts.iter() {
            info!("    ART: {:>8}: {:?}", name, description)
        }

        for (name, description) in declaration.streams.iter() {
            info!("    STR: {:>8}: {:?}", name, description)
        }

        for (name, description) in declaration.sinks.iter() {
            info!("    SNK: {:>8}: {:?}", name, description)
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use crate::dev_util::logging;
    use crate::stream::Artifact;

    use super::*;

    #[derive(Debug, Clone, serde::Serialize)]
    struct TestArtifact;

    impl Default for TestArtifact {
        fn default() -> Self {
            TestArtifact {}
        }
    }

    impl Artifact for TestArtifact {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn test_parameters() {
        logging();

        let declaration = Declaration::default()
            .default_attr("foo", "some description", AttributeValue::Int(42))
            .attribute("bar", "some description")
            .artifact("foo", "some description")
            .artifact("bar", "some description")
            .stream("foo", "some description")
            .stream("bar", "some description")
            .sink("foo", "some description")
            .sink("bar", "some description");

        let atr_extra: HashMap<String, AttributeValue> = vec![
            ("bar".into(), AttributeValue::Int(13)),
            ("baz".into(), AttributeValue::Int(37)),
        ]
        .into_iter()
        .collect();
        let art_extra: &mut [AnyArtifact] = &mut [
            TestArtifact::default().into(),
            TestArtifact::default().into(),
            TestArtifact::default().into(),
        ];
        let str_extra: Vec<Box<dyn Stream>> = vec![
            Box::new(Void::default()),
            Box::new(Void::default()),
            Box::new(Void::default()),
        ];
        let snk_extra: Vec<Box<dyn Sink>> = vec![
            Box::new(Void::default()),
            Box::new(Void::default()),
            Box::new(Void::default()),
        ];
        let mut parameters = declaration
            .make(atr_extra, art_extra, str_extra, snk_extra)
            .unwrap();

        assert_eq!(
            *parameters
                .acquire_attribute("foo")
                .unwrap()
                .try_int()
                .unwrap(),
            42
        );
        assert_eq!(
            *parameters
                .acquire_attribute("bar")
                .unwrap()
                .try_int()
                .unwrap(),
            13
        );
        assert_eq!(
            *parameters
                .acquire_attribute("baz")
                .unwrap()
                .try_int()
                .unwrap(),
            37
        );

        parameters.warn_non_empty();

        assert!(parameters.acquire_artifact("foo").is_ok());
        assert!(parameters.acquire_artifact("bar").is_ok());
        assert!(parameters.acquire_stream("foo").is_ok());
        assert!(parameters.acquire_stream("bar").is_ok());
        assert!(parameters.acquire_sink("foo").is_ok());
        assert!(parameters.acquire_sink("bar").is_ok());
        assert_eq!(parameters.acquire_artifact_extra().len(), 1);
        assert_eq!(parameters.acquire_stream_extra().len(), 1);
        assert_eq!(parameters.acquire_sink_extra().len(), 1);

        parameters.warn_non_empty();

        assert!(parameters.acquire_attribute("foo").is_err());
        assert!(parameters.acquire_attribute("bar").is_err());
        assert!(parameters.acquire_attribute("baz").is_err());
        assert!(parameters.acquire_artifact("foo").is_err());
        assert!(parameters.acquire_artifact("bar").is_err());
        assert!(parameters.acquire_stream("foo").is_err());
        assert!(parameters.acquire_stream("bar").is_err());
        assert!(parameters.acquire_sink("foo").is_err());
        assert!(parameters.acquire_sink("bar").is_err());
        assert_eq!(parameters.acquire_artifact_extra().len(), 0);
        assert_eq!(parameters.acquire_stream_extra().len(), 0);
        assert_eq!(parameters.acquire_sink_extra().len(), 0);
    }

    #[test]
    fn test_parameters_warning() {
        logging();

        let decl_atr = Declaration::default()
            .default_attr("foo", "some description", AttributeValue::Int(0))
            .attribute("bar", "some description");

        let decl_art = Declaration::default()
            .artifact("foo", "some description")
            .artifact("bar", "some description");

        let decl_str = Declaration::default()
            .stream("foo", "some description")
            .stream("bar", "some description");

        let decl_snk = Declaration::default()
            .sink("foo", "some description")
            .sink("bar", "some description");

        let atr_err: HashMap<String, AttributeValue> = vec![].into_iter().collect();
        let mut art_err: &mut [AnyArtifact] = &mut [TestArtifact::default().into()];
        let str_err: Vec<Box<dyn Stream>> = vec![Box::new(Void::default())];
        let snk_err: Vec<Box<dyn Sink>> = vec![Box::new(Void::default())];

        assert!(decl_atr
            .make(atr_err.clone(), &mut [], vec![], vec![],)
            .is_err());
        assert!(decl_art
            .make(atr_err.clone(), &mut art_err, vec![], vec![],)
            .is_err());
        assert!(decl_str
            .make(atr_err.clone(), &mut [], str_err, vec![],)
            .is_err());
        assert!(decl_snk
            .make(atr_err.clone(), &mut [], vec![], snk_err,)
            .is_err());
    }
}