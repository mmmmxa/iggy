// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::DeltaSink;
use crate::SinkState;
use crate::coercions::{coerce, create_coercion_tree};
use crate::schema::schema_differences;
use crate::storage::build_storage_options;
use async_trait::async_trait;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::arrow::json::ReaderBuilder;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata};
use simd_json::OwnedValue;
use simd_json::prelude::ValueAsObject;
use tracing::{debug, error, info};

// TODO: Expose metrics for observability purposes

#[async_trait]
impl Sink for DeltaSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Delta Lake sink connector with ID: {} for table: {}",
            self.id, self.config.table_uri
        );

        let table_url = url::Url::parse(&self.config.table_uri).map_err(|e| {
            error!(
                "Connector configuration: failed to parse table_uri = '{}': {e}.",
                self.config.table_uri
            );
            Error::InvalidConfigValue(format!("table_uri: {e}"))
        })?;
        let table_uri = &self.config.table_uri;

        info!("Parsed table URI: {}", table_url);

        let storage_options = build_storage_options(&self.config).inspect_err(|e| {
            error!("Connector configuration: invalid storage configuration. Error message: {e}");
        })?;

        let builder = deltalake::DeltaTableBuilder::from_url(table_url)
            .map_err(|e| {
                error!("deltalake-rs interface: failed to configure with table_uri = '{table_uri}'. Check deltalake::DeltaTableBuilder::from_url docs and code to correct your table_uri. Error message: {e}");
                Error::InvalidConfigValue(format!("table_uri = '{table_uri}' caused an error in deltalake-rs interface. Check deltalake::DeltaTableBuilder::from_url docs and code to correct your table_uri. Error message: {e}"))
            })?
            .with_storage_options(storage_options);
        let mut table = builder.build().map_err(|e| {
            error!("deltalake-rs interface: failed to configure with provided storage configuration. Error message: {e}");
            Error::InitError(format!("deltalake-rs interface: failed to configure with provided storage configuration. Error message: {e}"))
        })?;
        let table_exists = table
            .verify_deltatable_existence()
            .await
            .map_err(
                |e| {
                    error!("deltalake-rs interface: failed to list table_uri '{table_uri}' directory to verify delta table existence. Make sure the destination exists and the access to the destination is set up correctly - read the Iggy delta connector docs for more information. Error message: {e}");
                    Error::InitError(format!("deltalake-rs interface: failed to list table_uri '{table_uri}' directory to verify delta table existence. Make sure the destination exists and the access to the destination is set up correctly - read the Iggy delta connector docs for more information. Error message: {e}"))
                }
            )?;
        if !table_exists {
            error!(
                "No delta table found in '{table_uri}'. Make sure to create the delta table in the destination manually or verify the validity of such table."
            );
            return Err(Error::InitError(format!(
                "No delta table found in '{table_uri}'. Make sure to create the delta table in the destination manually or verify the validity of such table."
            )));
        }

        table
            .load()
            .await
            .map_err(|e| {
                error!("deltalake-rs interface: failed to load the table's latest snapshot. See deltalake::DeltaTable::load for more information. Error message: {e}");
                Error::InitError(format!("deltalake-rs interface: failed to load the table's latest snapshot. See deltalake::DeltaTable::load for more information. Error message: {e}"))
            })?;

        let kernel_schema = table
            .snapshot()
            .map_err(|e| {
                error!("deltalake-rs interface: failed to get the table's latest snapshot. See deltalake::DeltaTable::snapshot for more information. Error message: {e}");
                Error::InitError(format!("deltalake-rs interface: failed to get the table's latest snapshot. See deltalake::DeltaTable::snapshot for more information. Error message: {e}"))
            })?
            .schema();

        if let Some(expected_schema) = &self.config.expected_schema {
            let differences = schema_differences(expected_schema, &kernel_schema);
            if !differences.is_empty() {
                let message = format!(
                    "the schema of the table '{table_uri}' does not match expected_schema: {}",
                    differences.join("; ")
                );
                error!("Connector configuration: {message}");
                return Err(Error::InitError(message));
            }
        }

        // TODO: coercion tree is never refreshed if the schema changes concurrently,
        // leading to opaque errors downstream.
        let coercion_tree = create_coercion_tree(&kernel_schema);

        let writer = RecordBatchWriter::for_table(&table).map_err(|e| {
            error!("Failed to create RecordBatchWriter: {e}");
            Error::InitError(format!("Failed to create RecordBatchWriter: {e}"))
        })?;

        *self.state.lock().await = Some(SinkState {
            table,
            writer,
            coercion_tree,
        });

        info!(
            "Delta Lake sink connector with ID: {} opened successfully.",
            self.id
        );
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        debug!(
            "Delta sink with ID: {} received: {} messages, partition: {}, offset: {}",
            self.id,
            messages.len(),
            messages_metadata.partition_id,
            messages_metadata.current_offset,
        );

        let mut json_values = json_values(messages)?;

        if json_values.is_empty() {
            debug!("No JSON values to write");
            return Ok(());
        }

        // This lock is held across the write and flush_and_commit I/O below,
        // serializing consume() for topics sharing this sink. Kept intentionally:
        // see #3839 for why per-partition writers were researched and dropped.
        let mut state_guard = self.state.lock().await;
        let state = state_guard.as_mut().ok_or_else(|| {
            error!("Delta sink state not initialized — was open() called?");
            Error::InvalidState
        })?;

        // Apply coercions to match Delta table schema
        for value in &mut json_values {
            coerce(value, &state.coercion_tree).map_err(Error::InvalidRecordValue)?;
        }

        let batch = record_batch(&json_values, state.writer.arrow_schema())?;

        // Write the batch to internal Parquet buffers
        // TODO: Add retry mechanism if write fails.
        if let Err(e) = state.writer.write(batch).await {
            state.writer.reset();
            error!("Failed to write to Delta writer: {e}");
            return Err(Error::Storage(format!(
                "Failed to write to Delta writer: {e}"
            )));
        }

        // Flush buffers to object store and commit to Delta log
        let version = match state.writer.flush_and_commit(&mut state.table).await {
            Ok(v) => v,
            Err(e) => {
                state.writer.reset();
                error!("Failed to flush and commit to Delta table: {e}");
                return Err(Error::Storage(format!("Failed to flush and commit: {e}")));
            }
        };

        debug!(
            "Delta sink with ID: {} committed version {}",
            self.id, version
        );

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(mut state) = self.state.lock().await.take()
            && let Err(e) = state.writer.flush_and_commit(&mut state.table).await
        {
            error!(
                "Delta sink with ID: {} failed to flush on close: {e}",
                self.id
            );
            return Err(Error::Storage(format!("Failed to flush on close: {e}")));
        }
        info!("Delta Lake sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

fn record_batch(values: &[OwnedValue], schema: SchemaRef) -> Result<RecordBatch, Error> {
    // An empty object has no key for strict mode to reject, yet it becomes a row of nulls.
    if values
        .iter()
        .any(|value| value.as_object().is_some_and(|object| object.is_empty()))
    {
        return Err(Error::InvalidRecordValue(
            "record is an empty object, it would be written as a row of nulls".to_string(),
        ));
    }

    // Without strict mode the decoder drops keys that are not table columns and fills the
    // columns it did not see with null, so a record that shares no field with the table
    // is committed as a row of nulls.
    let mut decoder = ReaderBuilder::new(schema)
        .with_strict_mode(true)
        .with_batch_size(values.len())
        .build_decoder()
        .map_err(|e| Error::InvalidRecordValue(e.to_string()))?;
    // OwnedValue emits primitive numbers, independent of serde_json's features.
    decoder
        .serialize(values)
        .map_err(|e| Error::InvalidRecordValue(e.to_string()))?;
    decoder
        .flush()
        .map_err(|e| Error::InvalidRecordValue(e.to_string()))?
        .ok_or(Error::InvalidRecord)
}

/// Every message's JSON document as a simd-json value, or the batch error for a
/// payload that has none. Proto text holding JSON is the descriptor-less
/// `proto_convert` fallback and is written as the document it holds; proto
/// text that is not JSON fails the batch like any other non-JSON payload.
fn json_values(messages: Vec<ConsumedMessage>) -> Result<Vec<OwnedValue>, Error> {
    let mut json_values = Vec::with_capacity(messages.len());
    for message in messages {
        match message.payload.into_json_document() {
            Payload::Json(document) => json_values.push(document),
            payload => {
                error!(
                    "Unsupported payload type: {payload}. Delta sink only supports JSON payloads."
                );
                return Err(Error::InvalidPayloadType);
            }
        }
    }
    Ok(json_values)
}

#[cfg(test)]
mod tests {
    use super::{json_values, record_batch};
    use deltalake::arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
    use deltalake::arrow::datatypes::{DataType, Field, Fields, Schema};
    use deltalake::arrow::record_batch::RecordBatch;
    use iggy_connector_sdk::{ConsumedMessage, Error, Payload};
    use std::sync::Arc;

    #[test]
    fn given_json_numbers_when_decoding_should_preserve_numeric_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let identifier = 9_007_199_254_740_993i64;
        let values = vec![
            simd_json::json!({"id": identifier, "value": 1.5}),
            simd_json::json!({"id": 2, "value": null}),
        ];
        let batch = record_batch(&values, schema.clone()).expect("decode JSON numbers");
        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![identifier, 2])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
            ],
        )
        .expect("numeric columns");
        assert_eq!(batch, expected);
    }

    fn nested_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![Field::new("city", DataType::Utf8, true)])),
                true,
            ),
        ]))
    }

    #[test]
    fn given_record_with_unknown_field_when_decoding_should_fail() {
        let values = vec![simd_json::json!({"id": 1, "email": "a@b.c"})];

        let error = record_batch(&values, nested_schema()).expect_err("unknown field");

        assert!(
            matches!(&error, Error::InvalidRecordValue(message) if message.contains("email")),
            "{error:?}"
        );
    }

    #[test]
    fn given_record_with_no_field_in_common_when_decoding_should_fail() {
        let values = vec![simd_json::json!({"unrelated": 1})];

        assert!(record_batch(&values, nested_schema()).is_err());
    }

    #[test]
    fn given_record_with_unknown_nested_field_when_decoding_should_fail() {
        let values = vec![simd_json::json!({"id": 1, "address": {"city": "x", "zip": "1"}})];

        let error = record_batch(&values, nested_schema()).expect_err("unknown nested field");

        assert!(
            matches!(&error, Error::InvalidRecordValue(message) if message.contains("zip")),
            "{error:?}"
        );
    }

    #[test]
    fn given_empty_record_when_decoding_should_fail() {
        let values = vec![simd_json::json!({"id": 1}), simd_json::json!({})];

        assert!(matches!(
            record_batch(&values, nested_schema()),
            Err(Error::InvalidRecordValue(_))
        ));
    }

    #[test]
    fn given_record_missing_some_columns_when_decoding_should_fill_nulls() {
        let values = vec![simd_json::json!({"id": 1})];

        let batch = record_batch(&values, nested_schema()).expect("subset of columns");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(1).null_count(), 1);
    }

    fn message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload,
        }
    }

    #[test]
    fn json_values_parses_proto_text_that_is_json() {
        let messages = vec![
            message(Payload::Json(simd_json::json!({"id": 1}))),
            message(Payload::Proto(r#"{"id": 2}"#.to_owned())),
        ];

        let values = json_values(messages).expect("proto text holding JSON is a document");

        assert_eq!(
            values,
            vec![simd_json::json!({"id": 1}), simd_json::json!({"id": 2})]
        );
    }

    #[test]
    fn json_values_rejects_proto_text_that_is_not_json() {
        let messages = vec![message(Payload::Proto("id: 2".to_owned()))];

        assert_eq!(
            json_values(messages).expect_err("proto text that is not JSON has no document"),
            Error::InvalidPayloadType
        );
    }

    #[test]
    fn json_values_rejects_a_batch_with_one_unsupported_payload_among_valid_ones() {
        let messages = vec![
            message(Payload::Json(simd_json::json!({"id": 1}))),
            message(Payload::Raw(b"not json".to_vec())),
            message(Payload::Json(simd_json::json!({"id": 3}))),
        ];

        assert_eq!(
            json_values(messages).expect_err("one unsupported payload must fail the whole batch"),
            Error::InvalidPayloadType
        );
    }
}
