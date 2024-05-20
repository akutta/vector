use launchdarkly_server_sdk::{Client, ConfigBuilder, ServiceEndpointsBuilder};
use serde_with::serde_as;
use vrl::core::Value;
use vector_lib::config::clone_input_definitions;
use vector_lib::enrichment::TableRegistry;
use crate::config::{DataType, GenerateConfig, Input, LogNamespace, OutputId, TransformConfig, TransformContext, TransformOutput};
use crate::schema::Definition;
use crate::sinks::prelude::configurable_component;
use crate::transforms::launch_darkly::transform::LaunchDarklyTransform;
use crate::transforms::Transform;

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

impl FeatureFlagConfig {
    pub(crate) fn default_value(&self) -> Value {
        match &self.kind {
            LaunchDarklyFlagTypeConfig::Bool(config) => Value::Boolean(config.clone().default.into()),
            LaunchDarklyFlagTypeConfig::Int(config) => config.clone().default.into(),
            LaunchDarklyFlagTypeConfig::Float(config) => config.clone().default.into(),
            LaunchDarklyFlagTypeConfig::String(config) => Value::from(config.clone().default),
            LaunchDarklyFlagTypeConfig::Json(config) => Value::from(config.clone().default),
        }
    }
}
