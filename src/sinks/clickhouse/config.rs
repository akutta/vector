//! Configuration for the `Clickhouse` sink.

use std::{collections::HashMap, fmt, sync::Arc};

use crate::codecs::Encoder;
use http::{Request, StatusCode, Uri};
use hyper::Body;
use vector_lib::codecs::encoding::format::SchemaProvider;
use vector_lib::codecs::encoding::{ArrowStreamSerializerConfig, BatchSerializerConfig};

use super::{
    request_builder::ClickhouseRequestBuilder,
    rowbinary::{RowBinaryEncoder, RowBinarySerializer, SchemaFetcher},
    service::{ClickhouseRetryLogic, ClickhouseServiceRequestBuilder},
    sink::{ClickhouseSink, PartitionKey},
};
use crate::{
    http::{Auth, HttpClient, MaybeAuth},
    sinks::{
        prelude::*,
        util::{RealtimeSizeBasedDefaultBatchSettings, UriSerde, http::HttpService},
    },
};

/// Data format.
///
/// The format used to parse input/output data.
///
/// [formats]: https://clickhouse.com/docs/en/interfaces/formats
#[configurable_component]
#[derive(Clone, Copy, Debug, Derivative, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
#[derivative(Default)]
#[allow(clippy::enum_variant_names)]
pub enum Format {
    #[derivative(Default)]
    /// JSONEachRow.
    JsonEachRow,

    /// JSONAsObject.
    JsonAsObject,

    /// JSONAsString.
    JsonAsString,

    /// ArrowStream (beta).
    #[configurable(metadata(status = "beta"))]
    ArrowStream,

    /// RowBinaryWithNamesAndTypes (beta).
    #[configurable(metadata(status = "beta"))]
    RowBinaryWithNamesAndTypes,
}

impl Format {
    pub const fn is_binary(&self) -> bool {
        matches!(self, Format::RowBinaryWithNamesAndTypes)
    }

    pub const fn is_json(&self) -> bool {
        matches!(
            self,
            Format::JsonEachRow | Format::JsonAsObject | Format::JsonAsString
        )
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Format::JsonEachRow => write!(f, "JSONEachRow"),
            Format::JsonAsObject => write!(f, "JSONAsObject"),
            Format::JsonAsString => write!(f, "JSONAsString"),
            Format::ArrowStream => write!(f, "ArrowStream"),
            Format::RowBinaryWithNamesAndTypes => write!(f, "RowBinaryWithNamesAndTypes"),
        }
    }
}

/// Schema configuration for binary formats.
///
/// When using `RowBinaryWithNamesAndTypes` format, the schema is fetched from the
/// ClickHouse table at startup. Event fields are automatically mapped to table columns
/// by matching the column name to the event field path (e.g., column "host" maps to ".host").
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct SchemaConfig {
    /// Allow null values for fields that are missing from events.
    ///
    /// When enabled, missing event fields will use NULL for Nullable columns, or the type's
    /// default value (0, empty string, etc.) for non-nullable columns. This is useful when
    /// working with schemas where not all fields are always present.
    ///
    /// When disabled (default), missing fields will cause an error, ensuring all expected
    /// data is present before sending to ClickHouse.
    #[serde(default)]
    pub allow_nullable_fields: bool,

    /// Default values to use when event fields are missing.
    ///
    /// The key is the column name, and the value is the default value as a string.
    /// The value will be converted to the column's type. These defaults take precedence
    /// over the `allow_nullable_fields` behavior.
    #[configurable(metadata(
        docs::examples = "severity = \"info\"",
        docs::examples = "status_code = \"0\""
    ))]
    #[serde(default)]
    pub defaults: HashMap<String, String>,
}

/// Configuration for the `clickhouse` sink.
#[configurable_component(sink("clickhouse", "Deliver log data to a ClickHouse database."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseConfig {
    /// The endpoint of the ClickHouse server.
    #[serde(alias = "host")]
    #[configurable(metadata(docs::examples = "http://localhost:8123"))]
    pub endpoint: UriSerde,

    /// The table that data is inserted into.
    #[configurable(metadata(docs::examples = "mytable"))]
    pub table: Template,

    /// The database that contains the table that data is inserted into.
    #[configurable(metadata(docs::examples = "mydatabase"))]
    pub database: Option<Template>,

    /// The format to parse input data.
    #[serde(default)]
    pub format: Format,

    /// Schema configuration for binary formats.
    #[configurable(derived)]
    #[serde(default)]
    pub schema: Option<SchemaConfig>,

    /// Sets `input_format_skip_unknown_fields`, allowing ClickHouse to discard fields not present in the table schema.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub skip_unknown_fields: Option<bool>,

    /// Sets `date_time_input_format` to `best_effort`, allowing ClickHouse to properly parse RFC3339/ISO 8601.
    #[serde(default)]
    pub date_time_best_effort: bool,

    /// Sets `insert_distributed_one_random_shard`, allowing ClickHouse to insert data into a random shard when using Distributed Table Engine.
    #[serde(default)]
    pub insert_random_shard: bool,

    #[configurable(derived)]
    #[serde(default = "Compression::gzip_default")]
    pub compression: Compression,

    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    pub encoding: Transformer,

    /// The batch encoding configuration for encoding events in batches.
    ///
    /// When specified, events are encoded together as a single batch.
    /// This is mutually exclusive with per-event encoding based on the `format` field.
    #[configurable(derived)]
    #[serde(default)]
    pub batch_encoding: Option<BatchSerializerConfig>,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,

    #[configurable(derived)]
    pub auth: Option<Auth>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    pub tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub query_settings: QuerySettingsConfig,
}

/// Query settings for the `clickhouse` sink.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct QuerySettingsConfig {
    /// Async insert-related settings.
    #[serde(default)]
    pub async_insert_settings: AsyncInsertSettingsConfig,
}

/// Async insert related settings for the `clickhouse` sink.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct AsyncInsertSettingsConfig {
    /// Sets `async_insert`, allowing ClickHouse to queue the inserted data and later flush to table in the background.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub enabled: Option<bool>,

    /// Sets `wait_for`, allowing ClickHouse to wait for processing of asynchronous insertion.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub wait_for_processing: Option<bool>,

    /// Sets 'wait_for_processing_timeout`, to control the timeout for waiting for processing asynchronous insertion.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub wait_for_processing_timeout: Option<u64>,

    /// Sets `async_insert_deduplicate`, allowing ClickHouse to perform deduplication when inserting blocks in the replicated table.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub deduplicate: Option<bool>,

    /// Sets `async_insert_max_data_size`, the maximum size in bytes of unparsed data collected per query before being inserted.
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub max_data_size: Option<u64>,

    /// Sets `async_insert_max_query_number`, the maximum number of insert queries before being inserted
    ///
    /// If left unspecified, use the default provided by the `ClickHouse` server.
    #[serde(default)]
    pub max_query_number: Option<u64>,
}

impl_generate_config_from_default!(ClickhouseConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "clickhouse")]
impl SinkConfig for ClickhouseConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        // Validate configuration based on format
        self.validate_config()?;

        let endpoint = self.endpoint.with_default_parts().uri;
        let auth = self.auth.choose_one(&self.endpoint.auth)?;
        let tls_settings = TlsSettings::from_options(self.tls.as_ref())?;
        let client = HttpClient::new(tls_settings, &cx.proxy)?;

        let database = self.database.clone().unwrap_or_else(|| {
            "default"
                .try_into()
                .expect("'default' should be a valid template")
        });

        // Build sink based on format
        match self.format {
            Format::JsonEachRow | Format::JsonAsObject | Format::JsonAsString | Format::ArrowStream => {
                self.build_json_sink(cx, client, endpoint, auth, database)
                    .await
            }
            Format::RowBinaryWithNamesAndTypes => {
                self.build_binary_sink(cx, client, endpoint, auth, database)
                    .await
            }
        }
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

impl ClickhouseConfig {
    /// Validates the configuration based on the selected format.
    fn validate_config(&self) -> crate::Result<()> {
        if self.format.is_binary() {
            // Binary formats require schema configuration
            let _schema = self.schema.as_ref().ok_or(
                "schema configuration is required for binary format 'row_binary_with_names_and_types'. \
                 Please provide a 'schema' section."
            )?;
        }
        Ok(())
    }

    /// Builds the sink for JSON formats (existing behavior).
    async fn build_json_sink(
        &self,
        _cx: SinkContext,
        client: HttpClient,
        endpoint: Uri,
        auth: Option<Auth>,
        database: Template,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        let clickhouse_service_request_builder = ClickhouseServiceRequestBuilder {
            auth: auth.clone(),
            endpoint: endpoint.clone(),
            skip_unknown_fields: self.skip_unknown_fields,
            date_time_best_effort: self.date_time_best_effort,
            insert_random_shard: self.insert_random_shard,
            compression: self.compression,
            query_settings: self.query_settings,
            // JSON formats handle JSON natively, no special setting needed
            has_json_columns: false,
        };

        let service: HttpService<ClickhouseServiceRequestBuilder, PartitionKey> =
            HttpService::new(client.clone(), clickhouse_service_request_builder);

        let request_limits = self.request.into_settings();

        let service = ServiceBuilder::new()
            .settings(request_limits, ClickhouseRetryLogic::default())
            .service(service);

        let batch_settings = self.batch.into_batcher_settings()?;

        // Resolve the encoding strategy (format + encoder) based on configuration
        let (format, encoder_kind) = self
            .resolve_strategy(&client, &endpoint, &database, auth.as_ref())
            .await?;

        let request_builder = ClickhouseRequestBuilder {
            compression: self.compression,
            encoder: (self.encoding.clone(), encoder_kind),
        };

        let sink = ClickhouseSink::new(
            batch_settings,
            service,
            database,
            self.table.clone(),
            format,
            request_builder,
        );

        let healthcheck = Box::pin(healthcheck(client, endpoint, auth));

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    /// Builds the sink for binary formats (RowBinaryWithNamesAndTypes).
    async fn build_binary_sink(
        &self,
        _cx: SinkContext,
        client: HttpClient,
        endpoint: Uri,
        auth: Option<Auth>,
        database: Template,
    ) -> crate::Result<(VectorSink, Healthcheck)> {
        // Get schema configuration
        let schema_config = self.schema.as_ref().ok_or(
            "schema configuration is required for binary format 'row_binary_with_names_and_types'"
        )?;

        // For binary format, we need to know the table name at build time to fetch the schema.
        // If the table is templated, we can't fetch the schema ahead of time.
        let table_str = self.table.get_ref();
        if table_str.contains("{{") || table_str.contains("{%") {
            return Err(
                "Templated table names are not supported with binary format. \
                 Use a static table name or switch to a JSON format."
                    .into(),
            );
        }

        // Similarly for database
        let database_str = database.get_ref();
        if database_str.contains("{{") || database_str.contains("{%") {
            return Err(
                "Templated database names are not supported with binary format. \
                 Use a static database name or switch to a JSON format."
                    .into(),
            );
        }

        // Fetch table schema from ClickHouse
        let schema_fetcher = SchemaFetcher::new(client.clone(), endpoint.clone(), auth.clone());
        let table_schema = schema_fetcher
            .fetch_schema(database_str, table_str)
            .await
            .map_err(|e| format!("Failed to fetch table schema: {}", e))?;

        // Create the RowBinary encoder
        let encoder = RowBinaryEncoder::new(&table_schema, schema_config)
            .map_err(|e| format!("Failed to create RowBinary encoder: {}", e))?;

        let encoder = Arc::new(encoder);

        // Create RowBinary serializer
        let table_schema_arc = Arc::new(table_schema);
        let serializer = RowBinarySerializer::new(Arc::clone(&table_schema_arc), schema_config)
            .map_err(|e| format!("Failed to create RowBinary serializer: {}", e))?;

        // Create service
        let clickhouse_service_request_builder = ClickhouseServiceRequestBuilder {
            auth: auth.clone(),
            endpoint: endpoint.clone(),
            skip_unknown_fields: self.skip_unknown_fields,
            date_time_best_effort: self.date_time_best_effort,
            insert_random_shard: self.insert_random_shard,
            compression: self.compression,
            query_settings: self.query_settings,
            has_json_columns: encoder.has_json_columns(),
        };

        let service: HttpService<ClickhouseServiceRequestBuilder, PartitionKey> =
            HttpService::new(client.clone(), clickhouse_service_request_builder);

        let request_limits = self.request.into_settings();

        let service = ServiceBuilder::new()
            .settings(request_limits, ClickhouseRetryLogic::default())
            .service(service);

        let batch_settings = self.batch.into_batcher_settings()?;

        // Create the request builder using BatchEncoder with RowBinarySerializer
        let batch_serializer = crate::codecs::BatchSerializerWrapper::new(serializer);
        let batch_encoder = crate::codecs::BatchEncoder::new(
            crate::codecs::BatchSerializer::Batch(batch_serializer),
        );
        let request_builder = ClickhouseRequestBuilder {
            compression: self.compression,
            encoder: (
                self.encoding.clone(),
                crate::codecs::EncoderKind::Batch(batch_encoder),
            ),
        };

        let sink = ClickhouseSink::new(
            batch_settings,
            service,
            database,
            self.table.clone(),
            self.format,
            request_builder,
        );

        let healthcheck = Box::pin(healthcheck(client, endpoint, auth));

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }
}

impl ClickhouseConfig {
    /// Resolves the encoding strategy (format + encoder) based on configuration.
    ///
    /// This method determines the appropriate ClickHouse format and Vector encoder
    /// based on the user's configuration, ensuring they are consistent.
    async fn resolve_strategy(
        &self,
        client: &HttpClient,
        endpoint: &Uri,
        database: &Template,
        auth: Option<&Auth>,
    ) -> crate::Result<(Format, crate::codecs::EncoderKind)> {
        use crate::codecs::EncoderKind;
        use vector_lib::codecs::{
            JsonSerializerConfig, NewlineDelimitedEncoderConfig, encoding::Framer,
        };

        if let Some(batch_encoding) = &self.batch_encoding {
            use crate::codecs::{BatchEncoder, BatchSerializer};

            // Validate that batch_encoding is only compatible with ArrowStream format
            if self.format != Format::ArrowStream {
                return Err(format!(
                    "'batch_encoding' is only compatible with 'format: arrow_stream'. Found 'format: {}'.",
                    self.format
                )
                .into());
            }

            let mut arrow_config = match batch_encoding {
                BatchSerializerConfig::ArrowStream(config) => config.clone(),
            };

            self.resolve_arrow_schema(
                client,
                endpoint.to_string(),
                database,
                auth,
                &mut arrow_config,
            )
            .await?;

            let resolved_batch_config = BatchSerializerConfig::ArrowStream(arrow_config);
            let arrow_serializer = resolved_batch_config.build()?;
            let batch_serializer = BatchSerializer::Batch(crate::codecs::BatchSerializerWrapper::new(arrow_serializer));
            let encoder = EncoderKind::Batch(BatchEncoder::new(batch_serializer));

            return Ok((Format::ArrowStream, encoder));
        }

        let encoder = EncoderKind::Framed(Box::new(Encoder::<Framer>::new(
            NewlineDelimitedEncoderConfig.build().into(),
            JsonSerializerConfig::default().build().into(),
        )));

        Ok((self.format, encoder))
    }

    async fn resolve_arrow_schema(
        &self,
        client: &HttpClient,
        endpoint: String,
        database: &Template,
        auth: Option<&Auth>,
        config: &mut ArrowStreamSerializerConfig,
    ) -> crate::Result<()> {
        use super::arrow;

        if self.table.is_dynamic() || database.is_dynamic() {
            return Err(
                "Arrow codec requires a static table and database. Dynamic schema inference is not supported."
                    .into(),
            );
        }

        let table_str = self.table.get_ref();
        let database_str = database.get_ref();

        debug!(
            "Fetching schema for table {}.{} at startup.",
            database_str, table_str
        );

        let provider = arrow::ClickHouseSchemaProvider::new(
            client.clone(),
            endpoint,
            database_str.to_string(),
            table_str.to_string(),
            auth.cloned(),
        );

        let schema = provider.get_schema().await.map_err(|e| {
            format!(
                "Failed to fetch schema for {}.{}: {}.",
                database_str, table_str, e
            )
        })?;

        config.schema = Some(schema);

        debug!(
            "Successfully fetched Arrow schema with {} fields.",
            config
                .schema
                .as_ref()
                .map(|s| s.fields().len())
                .unwrap_or(0)
        );

        Ok(())
    }
}

fn get_healthcheck_uri(endpoint: &Uri) -> String {
    let mut uri = endpoint.to_string();
    if !uri.ends_with('/') {
        uri.push('/');
    }
    uri.push_str("?query=SELECT%201");
    uri
}

async fn healthcheck(client: HttpClient, endpoint: Uri, auth: Option<Auth>) -> crate::Result<()> {
    let uri = get_healthcheck_uri(&endpoint);
    let mut request = Request::get(uri).body(Body::empty()).unwrap();

    if let Some(auth) = auth {
        auth.apply(&mut request);
    }

    let response = client.send(request).await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        status => Err(HealthcheckError::UnexpectedStatus { status }.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vector_lib::codecs::encoding::ArrowStreamSerializerConfig;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<ClickhouseConfig>();
    }

    #[test]
    fn test_get_healthcheck_uri() {
        assert_eq!(
            get_healthcheck_uri(&"http://localhost:8123".parse().unwrap()),
            "http://localhost:8123/?query=SELECT%201"
        );
        assert_eq!(
            get_healthcheck_uri(&"http://localhost:8123/".parse().unwrap()),
            "http://localhost:8123/?query=SELECT%201"
        );
        assert_eq!(
            get_healthcheck_uri(&"http://localhost:8123/path/".parse().unwrap()),
            "http://localhost:8123/path/?query=SELECT%201"
        );
    }

    /// Helper to create a minimal ClickhouseConfig for testing
    fn create_test_config(
        format: Format,
        batch_encoding: Option<BatchSerializerConfig>,
    ) -> ClickhouseConfig {
        ClickhouseConfig {
            endpoint: "http://localhost:8123".parse::<http::Uri>().unwrap().into(),
            table: "test_table".try_into().unwrap(),
            database: Some("test_db".try_into().unwrap()),
            format,
            batch_encoding,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_format_selection_with_batch_encoding() {
        use crate::http::HttpClient;
        use crate::tls::TlsSettings;

        // Create minimal dependencies for resolve_strategy
        let tls = TlsSettings::default();
        let client = HttpClient::new(tls, &Default::default()).unwrap();
        let endpoint: http::Uri = "http://localhost:8123".parse().unwrap();
        let database: Template = "test_db".try_into().unwrap();

        // Test incompatible formats - should all return errors
        let incompatible_formats = vec![
            (Format::JsonEachRow, "json_each_row"),
            (Format::JsonAsObject, "json_as_object"),
            (Format::JsonAsString, "json_as_string"),
        ];

        for (format, format_name) in incompatible_formats {
            let config = create_test_config(
                format,
                Some(BatchSerializerConfig::ArrowStream(
                    ArrowStreamSerializerConfig::default(),
                )),
            );

            let result = config
                .resolve_strategy(&client, &endpoint, &database, None)
                .await;

            assert!(
                result.is_err(),
                "Expected error for format {} with batch_encoding, but got success",
                format_name
            );
        }
    }

    #[test]
    fn test_format_selection_without_batch_encoding() {
        // When batch_encoding is None, the configured format should be used
        let configs = vec![
            Format::JsonEachRow,
            Format::JsonAsObject,
            Format::JsonAsString,
            Format::ArrowStream,
        ];

        for format in configs {
            let config = create_test_config(format, None);

            assert!(
                config.batch_encoding.is_none(),
                "batch_encoding should be None for format {:?}",
                format
            );
            assert_eq!(
                config.format, format,
                "format should match configured value"
            );
        }
    }
}
