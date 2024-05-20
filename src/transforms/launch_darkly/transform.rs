use futures::{ Stream, StreamExt};
use std::future::ready;
use std::pin::Pin;
use launchdarkly_server_sdk::Client;
use vrl::core::Value;
use crate::event::Event;
use crate::transforms::launch_darkly::config::FeatureFlagConfig;
use crate::transforms::TaskTransform;

pub struct LaunchDarklyTransform {
    pub(crate) ld_client: Client,
    pub(crate) log_flags: Vec<FeatureFlagConfig>,
    pub(crate) metric_flags: Vec<FeatureFlagConfig>
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
                    log.insert(format!("\"{}\"", &flag.result_key).as_str(), flag.default_value());
                });
            }
            Event::Metric(ref mut metric) => {
                self.metric_flags.iter().for_each(|flag| {
                    let value = match flag.default_value() {
                        Value::Boolean(_) | Value::Integer(_) | Value::Float(_) => flag.default_value().to_string(),
                        Value::Bytes(b) => String::from_utf8_lossy(b.as_ref()).into(),
                        _ => panic!("Unexpected value type")
                    };
                    metric.replace_tag(flag.result_key.to_string(), value);
                });


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
