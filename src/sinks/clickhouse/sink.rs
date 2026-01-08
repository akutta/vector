//! Implementation of the `clickhouse` sink.

use std::fmt::Debug;

use super::config::Format;
use crate::sinks::{prelude::*, util::http::HttpRequest};

/// Generic ClickHouse sink that can use different request builders.
pub struct ClickhouseSink<S, R> {
    batch_settings: BatcherSettings,
    service: S,
    database: Template,
    table: Template,
    format: Format,
    request_builder: R,
}

impl<S, R> ClickhouseSink<S, R>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
    R: RequestBuilder<(PartitionKey, Vec<Event>), Request = HttpRequest<PartitionKey>>
        + Send
        + Sync
        + 'static,
    R::Error: Debug + std::fmt::Display + Into<crate::Error> + Send,
{
    #[allow(clippy::missing_const_for_fn)] // Cannot be const: takes non-const parameters
    pub fn new(
        batch_settings: BatcherSettings,
        service: S,
        database: Template,
        table: Template,
        format: Format,
        request_builder: R,
    ) -> Self {
        Self {
            batch_settings,
            service,
            database,
            table,
            format,
            request_builder,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let batch_settings = self.batch_settings;

        input
            .batched_partitioned(
                KeyPartitioner::new(self.database, self.table, self.format),
                || batch_settings.as_byte_size_config(),
            )
            .filter_map(|(key, batch)| async move { key.map(move |k| (k, batch)) })
            .request_builder(
                default_request_builder_concurrency_limit(),
                self.request_builder,
            )
            .filter_map(|request| async {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S, R> StreamSink<Event> for ClickhouseSink<S, R>
where
    S: Service<HttpRequest<PartitionKey>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
    R: RequestBuilder<(PartitionKey, Vec<Event>), Request = HttpRequest<PartitionKey>>
        + Send
        + Sync
        + 'static,
    R::Error: Debug + std::fmt::Display + Into<crate::Error> + Send,
{
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

/// PartitionKey used to partition events by (database, table) pair.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct PartitionKey {
    pub database: String,
    pub table: String,
    pub format: Format,
}

/// KeyPartitioner that partitions events by (database, table) pair.
pub struct KeyPartitioner {
    database: Template,
    table: Template,
    format: Format,
}

impl KeyPartitioner {
    pub const fn new(database: Template, table: Template, format: Format) -> Self {
        Self {
            database,
            table,
            format,
        }
    }

    fn render(template: &Template, item: &Event, field: &'static str) -> Option<String> {
        template
            .render_string(item)
            .map_err(|error| {
                emit!(TemplateRenderingError {
                    error,
                    field: Some(field),
                    drop_event: true,
                });
            })
            .ok()
    }
}

impl Partitioner for KeyPartitioner {
    type Item = Event;
    type Key = Option<PartitionKey>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        let database = Self::render(&self.database, item, "database_key")?;
        let table = Self::render(&self.table, item, "table_key")?;
        Some(PartitionKey {
            database,
            table,
            format: self.format,
        })
    }
}
