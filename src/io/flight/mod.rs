//! Serialization and deserialization to Arrow's flight protocol

use arrow_format::flight::data::{FlightData, SchemaResult};
use arrow_format::ipc;
use arrow_format::ipc::planus::ReadAsRoot;

use crate::{
    array::Array,
    chunk::Chunk,
    datatypes::*,
    error::{Error, Result},
    io::ipc::read,
    io::ipc::write,
    io::ipc::write::common::{encode_chunk, DictionaryTracker, EncodedData, WriteOptions},
};

use super::ipc::write::default_ipc_fields;
use super::ipc::{IpcField, IpcSchema};

/// Serializes [`Chunk`] to a vector of [`FlightData`] representing the serialized dictionaries
/// and a [`FlightData`] representing the batch.
pub fn serialize_batch(
    columns: &Chunk<Box<dyn Array>>,
    fields: &[IpcField],
    options: &WriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let mut dictionary_tracker = DictionaryTracker {
        dictionaries: Default::default(),
        cannot_replace: false,
    };

    let (encoded_dictionaries, encoded_batch) =
        encode_chunk(columns, fields, &mut dictionary_tracker, options)
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
pub fn serialize_schema_to_result(
    schema: &Schema,
    ipc_fields: Option<&[IpcField]>,
) -> SchemaResult {
    SchemaResult {
        schema: schema_as_flatbuffer(schema, ipc_fields),
    }
}

/// Serializes a [`Schema`] to [`FlightData`].
pub fn serialize_schema(schema: &Schema, ipc_fields: Option<&[IpcField]>) -> FlightData {
    let data_header = schema_as_flatbuffer(schema, ipc_fields);
    FlightData {
        data_header,
        ..Default::default()
    }
}

/// Convert a [`Schema`] to bytes in the format expected in [`arrow_format::flight::data::FlightInfo`].
pub fn serialize_schema_to_info(
    schema: &Schema,
    ipc_fields: Option<&[IpcField]>,
) -> Result<Vec<u8>> {
    let encoded_data = if let Some(ipc_fields) = ipc_fields {
        schema_as_encoded_data(schema, ipc_fields)
    } else {
        let ipc_fields = default_ipc_fields(&schema.fields);
        schema_as_encoded_data(schema, &ipc_fields)
    };

    let mut schema = vec![];
    write::common_sync::write_message(&mut schema, encoded_data)?;
    Ok(schema)
}

fn schema_as_flatbuffer(schema: &Schema, ipc_fields: Option<&[IpcField]>) -> Vec<u8> {
    if let Some(ipc_fields) = ipc_fields {
        write::schema_to_bytes(schema, ipc_fields)
    } else {
        let ipc_fields = default_ipc_fields(&schema.fields);
        write::schema_to_bytes(schema, &ipc_fields)
    }
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
    read::deserialize_schema(bytes)
}

/// Deserializes [`FlightData`] to [`Chunk`].
pub fn deserialize_batch(
    data: &FlightData,
    fields: &[Field],
    ipc_schema: &IpcSchema,
    dictionaries: &read::Dictionaries,
) -> Result<Chunk<Box<dyn Array>>> {
    // check that the data_header is a record batch message
    let message = arrow_format::ipc::MessageRef::read_as_root(&data.data_header)
        .map_err(|err| Error::OutOfSpec(format!("Unable to get root as message: {:?}", err)))?;

    let mut reader = std::io::Cursor::new(&data.data_body);

    match message.header()?.ok_or_else(|| {
        Error::oos("Unable to convert flight data header to a record batch".to_string())
    })? {
        ipc::MessageHeaderRef::RecordBatch(batch) => read::read_record_batch(
            batch,
            fields,
            ipc_schema,
            None,
            dictionaries,
            message.version()?,
            &mut reader,
            0,
        ),
        _ => Err(Error::nyi(
            "flight currently only supports reading RecordBatch messages",
        )),
    }
}
