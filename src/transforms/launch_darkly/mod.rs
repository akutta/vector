use std::{future::ready, pin::Pin};
use futures::{Stream, StreamExt};
use launchdarkly_server_sdk::{Client, ConfigBuilder, ServiceEndpointsBuilder};
use serde_with::serde_as;

use vector_lib::config::{clone_input_definitions, LogNamespace};
use vector_lib::configurable::configurable_component;
use vector_lib::enrichment::TableRegistry;

use crate::{config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput}, event::{Event}, transforms::{TaskTransform, Transform}};
use crate::schema::Definition;

impl_generate_config_from_default!(LaunchDarklyTransformConfig);

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

    /// The fields used as additional metadata for the Launch Darkly feature flag evaluation
    #[configurable(metadata(docs::examples = "region", docks::examples = "environment"))]
    pub context_fields: Option<Vec<String>>,

    /// The key used to store the result of the Launch Darkly feature flag evaluation
    #[configurable(metadata(docs::examples = "evaluation"))]
    pub result_key: String,
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
    pub relay_proxy: String,

    /// The SDK key for Launch Darkly
    #[configurable(metadata(docs::examples = "sdk-key-123abc"))]
    pub sdk_key: String,

    /// Whether to run the client in offline mode
    #[configurable(metadata(docs::examples = "true", docs::examples = "false"))]
    #[serde(default)]
    pub offline: bool,

    /// A list of feature flags to evaluate as defined in Launch Darkly
    #[serde(default = "default_flags")]
    pub flags: Vec<FeatureFlagConfig>,
}

fn default_flags() -> Vec<FeatureFlagConfig> {
    vec![]
}

#[async_trait::async_trait]
#[typetag::serde(name = "launch_darkly")]
impl TransformConfig for LaunchDarklyTransformConfig {
    async fn build(&self, _cx: &TransformContext) -> crate::Result<Transform> {
        let ld_client = Client::build(ConfigBuilder::new(&self.sdk_key)
            .service_endpoints(ServiceEndpointsBuilder::new()
                .relay_proxy(&self.relay_proxy)
            ).offline(self.offline).build()?
        )?;

        ld_client.start_with_default_executor();
        if !ld_client.initialized_async().await {
            panic!("Client failed to successfully initialize");
        }

        Ok(Transform::event_task(LaunchDarklyTransform { config: self.clone(), ld_client }))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Metric | DataType::Log)
    }

    fn outputs(&self, _: TableRegistry, input_definitions: &[(OutputId, Definition)], _: LogNamespace) -> Vec<TransformOutput> {
        vec![TransformOutput::new(DataType::all(), clone_input_definitions(input_definitions))]
    }
}

pub struct LaunchDarklyTransform {
    config: LaunchDarklyTransformConfig,
    ld_client: Client,
}

impl LaunchDarklyTransform {
    fn transform_one(&self, mut event: Event) -> Event {
        let _ = self.ld_client;
        match event {
            Event::Log(ref mut log) => {
                self.config.flags.iter().for_each(|flag| {
                    // let value = match &flag.kind {
                    //     LaunchDarklyFlagTypeConfig::Bool(config) => config.default.to_string(),
                    //     LaunchDarklyFlagTypeConfig::Int(config) => config.default.to_string(),
                    //     LaunchDarklyFlagTypeConfig::Float(config) => config.default.to_string(),
                    //     LaunchDarklyFlagTypeConfig::String(config) => config.default.clone(),
                    //     LaunchDarklyFlagTypeConfig::Json(config) => config.default.clone(),
                    // };
                    log.insert(format!("\"{}\"", &flag.result_key).as_str(), String::from("ld_value"));
                });
            }
            Event::Metric(ref mut metric) => {
                metric.replace_tag(String::from("launch_darkly"), String::from("ld_value"));
                metric.replace_tag(String::from("launch_darkly_key"), self.config.sdk_key.clone());
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

    use crate::event::LogEvent;
    use crate::test_util::components::assert_transform_compliance;
    use crate::transforms::launch_darkly::{BoolConfig, FeatureFlagConfig, LaunchDarklyFlagTypeConfig, LaunchDarklyTransformConfig};
    use crate::transforms::test::create_topology;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<LaunchDarklyTransformConfig>();
    }

    #[tokio::test]
    async fn enrich_log() {
        assert_transform_compliance(async {
            let transform_config = LaunchDarklyTransformConfig {
                relay_proxy: "".to_string(),
                sdk_key: "sdk-fake-key".to_string(),
                offline: true,
                flags: vec![FeatureFlagConfig {
                    name: "my-feature-flag".to_string(),
                    kind: LaunchDarklyFlagTypeConfig::Bool(BoolConfig {
                        default: false
                    }),
                    key: "service".to_string(),
                    result_key: "evaluation".to_string(),
                    context_fields: None,
                }],
            };

            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) =
                create_topology(ReceiverStream::new(rx), transform_config.clone()).await;

            // We need to sleep to let the background task fetch the data.
            sleep(Duration::from_secs(1)).await;

            let log = LogEvent::default();
            let mut expected_log = log.clone();
            expected_log.insert(
                format!("\"{}\"", &transform_config.flags[0].result_key).as_str(),
                "ld_value");

            tx.send(log.into()).await.unwrap();

            let event = out.recv().await.unwrap();
            assert_event_data_eq!(event.into_log(), expected_log);

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
        })
            .await;
    }
}
