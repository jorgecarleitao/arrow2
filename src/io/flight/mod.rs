use std::sync::Arc;

use arrow_format::flight::data::{FlightData, SchemaResult};
use arrow_format::ipc;

use crate::{
    datatypes::*,
    error::{ArrowError, Result},
    io::ipc::read,
    io::ipc::write,
    io::ipc::write::common::{encode_columns, DictionaryTracker, EncodedData, WriteOptions},
    record_batch::RecordBatch,
};

use super::ipc::{IpcField, IpcSchema};

/// Serializes a [`RecordBatch`] to a vector of [`FlightData`] representing the serialized dictionaries
/// and a [`FlightData`] representing the batch.
pub fn serialize_batch(
    batch: &RecordBatch,
    fields: &[IpcField],
    options: &WriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let mut dictionary_tracker = DictionaryTracker::new(false);

    let columns = batch.clone().into();
    let (encoded_dictionaries, encoded_batch) =
        encode_columns(&columns, fields, &mut dictionary_tracker, options)
            .expect("DictionaryTracker configured above to not error on replacement");

    let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
    let flight_batch = encoded_batch.into();

    (flight_dictionaries, flight_batch)
}

impl From<EncodedData> for FlightData {
    fn from(data: EncodedData) -> Self {
        FlightData {
            data_header: data.ipc_message,
            data_body: data.arrow_data,
            ..Default::default()
        }
    }
}

/// Serializes a [`Schema`] to [`SchemaResult`].
pub fn serialize_schema_to_result(schema: &Schema, ipc_fields: &[IpcField]) -> SchemaResult {
    SchemaResult {
        schema: schema_as_flatbuffer(schema, ipc_fields),
    }
}

/// Serializes a [`Schema`] to [`FlightData`].
pub fn serialize_schema(schema: &Schema, ipc_fields: &[IpcField]) -> FlightData {
    let data_header = schema_as_flatbuffer(schema, ipc_fields);
    FlightData {
        data_header,
        ..Default::default()
    }
}

/// Convert a [`Schema`] to bytes in the format expected in [`arrow_format::flight::data::FlightInfo`].
pub fn serialize_schema_to_info(schema: &Schema, ipc_fields: &[IpcField]) -> Result<Vec<u8>> {
    let encoded_data = schema_as_encoded_data(schema, ipc_fields);

    let mut schema = vec![];
    write::common_sync::write_message(&mut schema, encoded_data)?;
    Ok(schema)
}

fn schema_as_flatbuffer(schema: &Schema, ipc_fields: &[IpcField]) -> Vec<u8> {
    let encoded_data = schema_as_encoded_data(schema, ipc_fields);
    encoded_data.ipc_message
}

fn schema_as_encoded_data(schema: &Schema, ipc_fields: &[IpcField]) -> EncodedData {
    EncodedData {
        ipc_message: write::schema_to_bytes(schema, ipc_fields),
        arrow_data: vec![],
    }
}

/// Deserialize an IPC message into [`Schema`], [`IpcSchema`].
/// Use to deserialize [`FlightData::data_header`] and [`SchemaResult::schema`].
pub fn deserialize_schemas(bytes: &[u8]) -> Result<(Schema, IpcSchema)> {
    if let Ok(ipc) = ipc::Message::root_as_message(bytes) {
        if let Some(schemas) = ipc.header_as_schema().map(read::fb_to_schema) {
            Ok(schemas)
        } else {
            Err(ArrowError::OutOfSpec(
                "Unable to get head as schema".to_string(),
            ))
        }
    } else {
        Err(ArrowError::OutOfSpec(
            "Unable to get root as message".to_string(),
        ))
    }
}

/// Deserializes [`FlightData`] to a [`RecordBatch`].
pub fn deserialize_batch(
    data: &FlightData,
    schema: Arc<Schema>,
    ipc_schema: &IpcSchema,
    dictionaries: &read::Dictionaries,
) -> Result<RecordBatch> {
    // check that the data_header is a record batch message
    let message = ipc::Message::root_as_message(&data.data_header[..]).map_err(|err| {
        ArrowError::OutOfSpec(format!("Unable to get root as message: {:?}", err))
    })?;

    let mut reader = std::io::Cursor::new(&data.data_body);

    message
        .header_as_record_batch()
        .ok_or_else(|| {
            ArrowError::OutOfSpec(
                "Unable to convert flight data header to a record batch".to_string(),
            )
        })
        .map(|batch| {
            read::read_record_batch(
                batch,
                schema.clone(),
                ipc_schema,
                None,
                dictionaries,
                ipc::Schema::MetadataVersion::V5,
                &mut reader,
                0,
            )
        })?
}
