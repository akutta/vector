use std::{future::ready, pin::Pin};

use futures::{Stream, StreamExt};
use launchdarkly_server_sdk::{Client, ConfigBuilder, ServiceEndpointsBuilder};
use serde_with::serde_as;
use vrl::value::Value;

use vector_lib::config::{clone_input_definitions, LogNamespace};
use vector_lib::configurable::configurable_component;
use vector_lib::enrichment::TableRegistry;

use crate::{config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput}, event::Event, transforms::{TaskTransform, Transform}};
use crate::config::GenerateConfig;
use crate::schema::Definition;

/// Configuration for Bool Feature Flag
#[configurable_component]
#[derive(Clone, Debug)]
pub struct BoolConfig {
    /// Default Value to return for Bool Feature Flag
    #[serde(default)]
    pub default: bool,
}

/// Configuration for Int Feature Flag
#[configurable_component]
#[derive(Clone, Debug)]
pub struct IntConfig {
    /// Default Value to return for Int Feature Flag
    #[serde(default)]
    pub default: i64,
}

/// Configuration for Float Feature Flag
#[configurable_component]
#[derive(Clone, Debug)]
pub struct FloatConfig {
    /// Default Value to return for Float Feature Flag
    #[serde(default)]
    pub default: f64,
}

/// Configuration for String Feature Flag
#[configurable_component]
#[derive(Clone, Debug)]
pub struct StringConfig {
    /// Default Value to return for String Feature Flag
    #[serde(default)]
    pub default: String,
}

/// Configuration for JSON Feature Flag
#[configurable_component]
#[derive(Clone, Debug)]
pub struct JsonConfig {
    /// Default Value to return for JSON Feature Flag
    #[serde(default)]
    pub default: String,
}

/// Specification of the type of feature flag
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The type of feature flag to evaluate."))]
pub enum LaunchDarklyFlagTypeConfig {
    /// A bool.
    Bool(BoolConfig),

    /// An int.
    Int(IntConfig),

    /// A float.
    Float(FloatConfig),

    /// A string.
    String(StringConfig),

    /// JSON.
    Json(JsonConfig),
}

/// The type of event to evaluate flags for
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The type of event to evaluate the feature flag for"))]
pub enum LaunchDarklyEnrichType {
    /// A metric.
    Metric,
    /// A log.
    Log,
}

/// Specification of a feature flag to evaluate
#[configurable_component]
#[derive(Clone, Debug)]
pub struct FeatureFlagConfig {
    /// The feature flag to evaluate as defined in Launch Darkly
    #[configurable(metadata(docs::examples = "my-feature-flag"))]
    pub name: String,

    /// The type of the feature flag
    #[configurable(metadata(docs::examples = "bool", docs::examples = "int", docs::examples = "float", docs::examples = "string", docs::examples = "json"))]
    #[serde(flatten)]
    pub kind: LaunchDarklyFlagTypeConfig,

    /// The field used to identify the user for the Launch Darkly feature flag evaluation
    #[configurable(metadata(docs::examples = "service"))]
    pub key: String,

    /// The set of event types to evaluate the feature flag for
    #[configurable(metadata(docs::examples = "metric", docs::examples="log"))]
    #[serde(default = "default_event_kinds")]
    pub event_kind: Vec<LaunchDarklyEnrichType>,

    /// The fields used as additional metadata for the Launch Darkly feature flag evaluation
    #[configurable(metadata(docs::examples = "region", docks::examples = "environment"))]
    pub context_fields: Option<Vec<String>>,

    /// The key used to store the result of the Launch Darkly feature flag evaluation
    #[configurable(metadata(docs::examples = "evaluation"))]
    pub result_key: String,
}

fn default_event_kinds() -> Vec<LaunchDarklyEnrichType> {
    vec![LaunchDarklyEnrichType::Log, LaunchDarklyEnrichType::Metric]
}

impl GenerateConfig for FeatureFlagConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(FeatureFlagConfig {
            name: "".to_string(),
            kind: LaunchDarklyFlagTypeConfig::Bool(BoolConfig { default: false }),
            key: "".to_string(),
            event_kind: vec![LaunchDarklyEnrichType::Log, LaunchDarklyEnrichType::Metric],
            context_fields: None,
            result_key: "".to_string(),
        }).unwrap()
    }
}

/// Configuration for the `launch_darkly` transform.
#[serde_as]
#[configurable_component(transform(
    "launch_darkly",
    "Enrich event with result from evalution of Launch Darkly feature flag",
))]
#[derive(Clone, Debug, Derivative)]
#[serde(deny_unknown_fields)]
#[derivative(Default)]
pub struct LaunchDarklyTransformConfig {
    /// The relay proxy configuration for Launch Darkly
    ///  https://docs.launchdarkly.com/sdk/features/relay-proxy-configuration/proxy-mode#rust
    #[configurable(metadata(docs::examples = "https://your-relay-proxy.com:8030"))]
    pub relay_proxy: Option<String>,

    /// The SDK key for Launch Darkly
    #[configurable(metadata(docs::examples = "sdk-key-123abc"))]
    pub sdk_key: String,

    /// Whether to run the client in offline mode
    #[configurable(metadata(docs::examples = "true", docs::examples = "false"))]
    #[serde(default)]
    pub offline: bool,

    /// A list of feature flags to evaluate as defined in Launch Darkly
    #[configurable(derived)]
    #[serde(default)]
    pub flags: Vec<FeatureFlagConfig>,
}

impl GenerateConfig for LaunchDarklyTransformConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(LaunchDarklyTransformConfig {
            relay_proxy: None,
            sdk_key: "".to_string(),
            offline: false,
            flags: vec![]
        }).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "launch_darkly")]
impl TransformConfig for LaunchDarklyTransformConfig {
    async fn build(&self, _cx: &TransformContext) -> crate::Result<Transform> {
        let ld_client = match self.relay_proxy {
            Some(ref relay_proxy) => {
                if relay_proxy.is_empty() {
                    return Err("relay_proxy must not be empty".into());
                }
                let client = Client::build(ConfigBuilder::new(&self.sdk_key)
                                  .service_endpoints(ServiceEndpointsBuilder::new()
                                      .relay_proxy(&relay_proxy)
                                  ).offline(self.offline).build()?)?;
                client
            }
            None => {
                let client = Client::build(ConfigBuilder::new(&self.sdk_key)
                    .offline(self.offline)
                    .build()?
                )?;
                client
            }
        };

        ld_client.start_with_default_executor();
        if !ld_client.initialized_async().await {
            panic!("Client failed to successfully initialize");
        }

        let mut log_flags : Vec<FeatureFlagConfig> = vec![];
        let mut metric_flags : Vec<FeatureFlagConfig> = vec![];
        for flag in &self.flags {
            for event_kind in &flag.event_kind {
                match event_kind {
                    LaunchDarklyEnrichType::Metric => {
                        metric_flags.push(flag.clone());
                    }
                    LaunchDarklyEnrichType::Log => {
                        log_flags.push(flag.clone());
                    }
                }
            }
        }


        Ok(Transform::event_task(LaunchDarklyTransform { ld_client, log_flags: log_flags.to_vec(), metric_flags: metric_flags.to_vec() }))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Metric | DataType::Log)
    }

    fn outputs(&self, _: TableRegistry, input_definitions: &[(OutputId, Definition)], _: LogNamespace) -> Vec<TransformOutput> {
        vec![TransformOutput::new(DataType::all(), clone_input_definitions(input_definitions))]
    }
}

pub struct LaunchDarklyTransform {
    ld_client: Client,
    log_flags: Vec<FeatureFlagConfig>,
    metric_flags: Vec<FeatureFlagConfig>
}

impl LaunchDarklyTransform {
    fn transform_one(&self, mut event: Event) -> Event {
        if self.log_flags.is_empty() && self.metric_flags.is_empty() {
            return event;
        }

        let _ = self.ld_client;
        match event {
            Event::Log(ref mut log) => {
                self.log_flags.iter().for_each(|flag| {
                    let value = match &flag.kind {
                        LaunchDarklyFlagTypeConfig::Bool(config) => Value::Boolean(config.clone().default.into()),
                        LaunchDarklyFlagTypeConfig::Int(config) => config.clone().default.into(),
                        LaunchDarklyFlagTypeConfig::Float(config) => config.clone().default.into(),
                        LaunchDarklyFlagTypeConfig::String(config) => Value::Bytes(config.clone().default.into()),
                        LaunchDarklyFlagTypeConfig::Json(config) => Value::Bytes(config.clone().default.into()),
                    };
                    log.insert(format!("\"{}\"", &flag.result_key).as_str(), value);
                });
            }
            Event::Metric(ref mut metric) => {
                metric.replace_tag(String::from("launch_darkly"), String::from("ld_value"));
            }
            Event::Trace(_) => panic!("Traces are not supported."),
        }
        event
    }
}

impl TaskTransform<Event> for LaunchDarklyTransform {
    fn transform(
        self: Box<Self>,
        task: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
        where
            Self: 'static,
    {
        let inner = self;
        Box::pin(task.filter_map(move |event| ready(Some(inner.transform_one(event)))))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio::time::sleep;
    use tokio_stream::wrappers::ReceiverStream;
    use vector_lib::event::Event;

    use crate::event::{EventMetadata, LogEvent, Metric, MetricKind, MetricValue};
    use crate::test_util::components::assert_transform_compliance;
    use crate::transforms::launch_darkly::LaunchDarklyTransformConfig;
    use crate::transforms::test::create_topology;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<LaunchDarklyTransformConfig>();
    }

    fn parse_yaml_config(s: &str) -> LaunchDarklyTransformConfig {
        serde_yaml::from_str(s).unwrap()
    }

    #[tokio::test]
    async fn enrich_log() {
        let transform_config = parse_yaml_config(
            r#"
            sdk_key: "sdk-fake-key"
            offline: true
            flags:
            - name: my-feature-flag-1
              type: bool
              key: service
              result_key: evaluation-1
            - name: my-feature-flag-2
              type: int
              default: 123
              key: service
              result_key: evaluation-2
            - name: my-feature-flag-3
              type: float
              default: 123.1234
              key: service
              result_key: evaluation-3
            - name: my-feature-flag-4
              type: string
              default: special value
              key: service
              result_key: evaluation-4
            - name: my-feature-flag-5
              type: json
              key: service
              result_key: evaluation-5
            - name: no-op
              type: json
              key: service
              result_key: evaluation-6
              event_kind: [metric]
            "#,
        );

        assert_transform_compliance(async {
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) =
                create_topology(ReceiverStream::new(rx), transform_config.clone()).await;

            // We need to sleep to let the background task fetch the data.
            sleep(Duration::from_secs(1)).await;

            let log = LogEvent::default();
            let mut expected_log = log.clone();
            expected_log.insert(
                format!("\"{}\"", &transform_config.flags[0].result_key).as_str(),
                false);
            expected_log.insert(
                format!("\"{}\"", &transform_config.flags[1].result_key).as_str(),
                123);
            expected_log.insert(
                format!("\"{}\"", &transform_config.flags[2].result_key).as_str(),
                123.1234);
            expected_log.insert(
                format!("\"{}\"", &transform_config.flags[3].result_key).as_str(),
                "special value");
            expected_log.insert(
                format!("\"{}\"", &transform_config.flags[4].result_key).as_str(),
                "");

            tx.send(log.into()).await.unwrap();

            let event = out.recv().await.unwrap();
            assert_event_data_eq!(event.into_log(), expected_log);

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
        })
            .await;
    }

    #[tokio::test]
    async fn enrich_metric() {
        let transform_config = parse_yaml_config(
            r#"
            sdk_key: "sdk-fake-key"
            offline: true
            flags:
            - name: my-feature-flag-1
              type: bool
              key: service
              result_key: evaluation-1
            - name: my-feature-flag-2
              type: int
              default: 123
              key: service
              result_key: evaluation-2
            - name: my-feature-flag-3
              type: float
              default: 123.1234
              key: service
              result_key: evaluation-3
            - name: my-feature-flag-4
              type: string
              default: special value
              key: service
              result_key: evaluation-4
            - name: my-feature-flag-5
              type: json
              key: service
              result_key: evaluation-5
            - name: no-op
              type: json
              key: service
              result_key: evaluation-6
              event_kind: [log]
            "#,
        );

        assert_transform_compliance(async {
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) =
                create_topology(ReceiverStream::new(rx), transform_config.clone()).await;

            // We need to sleep to let the background task fetch the data.
            sleep(Duration::from_secs(1)).await;

            let event_metadata = EventMetadata::default().with_source_type("unit_test_stream");
            let metric = Event::Metric(
                Metric::new_with_metadata(
                    "counter",
                    MetricKind::Absolute,
                    MetricValue::Counter { value: 1.0 },
                    event_metadata,
                )
            );

            let mut expected_metric = metric.clone().into_metric();
            expected_metric.replace_tag(String::from("launch_darkly"), String::from("ld_value"));

            tx.send(metric.into()).await.unwrap();

            let event = out.recv().await.unwrap();
            assert_event_data_eq!(event.into_metric(), expected_metric);

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
        })
            .await;
    }
}
