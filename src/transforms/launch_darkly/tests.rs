
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio::time::sleep;
    use tokio_stream::wrappers::ReceiverStream;
    use vector_lib::event::Event;

    use crate::event::{EventMetadata, LogEvent, Metric, MetricKind, MetricValue};
    use crate::test_util::components::assert_transform_compliance;
    use crate::transforms::launch_darkly::config::LaunchDarklyTransformConfig;
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

            expected_metric.replace_tag(
                transform_config.flags[0].result_key.to_string(),
                "false".to_string());
            expected_metric.replace_tag(
                transform_config.flags[1].result_key.to_string(),
                "123".to_string());
            expected_metric.replace_tag(
                transform_config.flags[2].result_key.to_string(),
                "123.1234".to_string());
            expected_metric.replace_tag(
                transform_config.flags[3].result_key.to_string(),
                "special value".to_string());
            expected_metric.replace_tag(
                transform_config.flags[4].result_key.to_string(),
                "".to_string());

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
