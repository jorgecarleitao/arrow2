use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use arrow_format::flight::data::{FlightData, SchemaResult};
use arrow_format::ipc;

use crate::{
    array::*,
    datatypes::*,
    error::{ArrowError, Result},
    io::ipc::fb_to_schema,
    io::ipc::read::read_record_batch,
    io::ipc::write,
    io::ipc::write::common::{encoded_batch, DictionaryTracker, EncodedData, WriteOptions},
    record_batch::RecordBatch,
};

/// Serializes a [`RecordBatch`] to a vector of [`FlightData`] representing the serialized dictionaries
/// and a [`FlightData`] representing the batch.
pub fn serialize_batch(
    batch: &RecordBatch,
    options: &WriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let mut dictionary_tracker = DictionaryTracker::new(false);

    let (encoded_dictionaries, encoded_batch) =
        encoded_batch(batch, &mut dictionary_tracker, options)
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
pub fn serialize_schema_to_result(schema: &Schema) -> SchemaResult {
    SchemaResult {
        schema: schema_as_flatbuffer(schema),
    }
}

/// Serializes a [`Schema`] to [`FlightData`].
pub fn serialize_schema(schema: &Schema) -> FlightData {
    let data_header = schema_as_flatbuffer(schema);
    FlightData {
        data_header,
        ..Default::default()
    }
}

/// Convert a [`Schema`] to bytes in the format expected in [`arrow_format::flight::FlightInfo`].
pub fn serialize_schema_to_info(schema: &Schema) -> Result<Vec<u8>> {
    let encoded_data = schema_as_encoded_data(schema);

    let mut schema = vec![];
    write::common_sync::write_message(&mut schema, encoded_data)?;
    Ok(schema)
}

fn schema_as_flatbuffer(schema: &Schema) -> Vec<u8> {
    let encoded_data = schema_as_encoded_data(schema);
    encoded_data.ipc_message
}

fn schema_as_encoded_data(schema: &Schema) -> EncodedData {
    EncodedData {
        ipc_message: write::schema_to_bytes(schema),
        arrow_data: vec![],
    }
}

/// Deserialize an IPC message into a schema
fn schema_from_bytes(bytes: &[u8]) -> Result<Schema> {
    if let Ok(ipc) = ipc::Message::root_as_message(bytes) {
        if let Some((schema, _)) = ipc.header_as_schema().map(fb_to_schema) {
            Ok(schema)
        } else {
            Err(ArrowError::Ipc("Unable to get head as schema".to_string()))
        }
    } else {
        Err(ArrowError::Ipc("Unable to get root as message".to_string()))
    }
}

impl TryFrom<&FlightData> for Schema {
    type Error = ArrowError;
    fn try_from(data: &FlightData) -> Result<Self> {
        schema_from_bytes(&data.data_header[..]).map_err(|err| {
            ArrowError::Ipc(format!(
                "Unable to convert flight data to Arrow schema: {}",
                err
            ))
        })
    }
}

impl TryFrom<&SchemaResult> for Schema {
    type Error = ArrowError;
    fn try_from(data: &SchemaResult) -> Result<Self> {
        schema_from_bytes(&data.schema[..]).map_err(|err| {
            ArrowError::Ipc(format!(
                "Unable to convert schema result to Arrow schema: {}",
                err
            ))
        })
    }
}

/// Deserializes [`FlightData`] to a [`RecordBatch`].
pub fn deserialize_batch(
    data: &FlightData,
    schema: Arc<Schema>,
    is_little_endian: bool,
    dictionaries: &HashMap<usize, Arc<dyn Array>>,
) -> Result<RecordBatch> {
    // check that the data_header is a record batch message
    let message = ipc::Message::root_as_message(&data.data_header[..])
        .map_err(|err| ArrowError::Ipc(format!("Unable to get root as message: {:?}", err)))?;

    let mut reader = std::io::Cursor::new(&data.data_body);

    message
        .header_as_record_batch()
        .ok_or_else(|| {
            ArrowError::Ipc("Unable to convert flight data header to a record batch".to_string())
        })
        .map(|batch| {
            read_record_batch(
                batch,
                schema.clone(),
                None,
                is_little_endian,
                dictionaries,
                ipc::Schema::MetadataVersion::V5,
                &mut reader,
                0,
            )
        })?
}
