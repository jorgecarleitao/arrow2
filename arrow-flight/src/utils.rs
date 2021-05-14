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

//! Utilities to assist with reading and writing Arrow data as Flight messages

use std::{convert::TryFrom, sync::Arc};

use crate::{FlightData, SchemaResult};

use arrow2::{
    array::*,
    datatypes::*,
    error::{ArrowError, Result},
    io::ipc,
    io::ipc::read::read_record_batch,
    io::ipc::write,
    io::ipc::write::common::{encoded_batch, DictionaryTracker, EncodedData, IpcWriteOptions},
    record_batch::RecordBatch,
};

/// Convert a `RecordBatch` to a vector of `FlightData` representing the bytes of the dictionaries
/// and a `FlightData` representing the bytes of the batch's values
pub fn flight_data_from_arrow_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let mut dictionary_tracker = DictionaryTracker::new(false);

    let (encoded_dictionaries, encoded_batch) =
        encoded_batch(batch, &mut dictionary_tracker, &options)
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

/// Convert a `Schema` to `SchemaResult` by converting to an IPC message
pub fn flight_schema_from_arrow_schema(schema: &Schema, options: &IpcWriteOptions) -> SchemaResult {
    SchemaResult {
        schema: flight_schema_as_flatbuffer(schema, options),
    }
}

/// Convert a `Schema` to `FlightData` by converting to an IPC message
pub fn flight_data_from_arrow_schema(schema: &Schema, options: &IpcWriteOptions) -> FlightData {
    let data_header = flight_schema_as_flatbuffer(schema, options);
    FlightData {
        data_header,
        ..Default::default()
    }
}

/// Convert a `Schema` to bytes in the format expected in `FlightInfo.schema`
pub fn ipc_message_from_arrow_schema(
    arrow_schema: &Schema,
    options: &IpcWriteOptions,
) -> Result<Vec<u8>> {
    let encoded_data = flight_schema_as_encoded_data(arrow_schema, options);

    let mut schema = vec![];
    write::common::write_message(&mut schema, encoded_data, options)?;
    Ok(schema)
}

fn flight_schema_as_flatbuffer(arrow_schema: &Schema, options: &IpcWriteOptions) -> Vec<u8> {
    let encoded_data = flight_schema_as_encoded_data(arrow_schema, options);
    encoded_data.ipc_message
}

fn flight_schema_as_encoded_data(arrow_schema: &Schema, options: &IpcWriteOptions) -> EncodedData {
    EncodedData {
        ipc_message: write::schema_to_bytes(arrow_schema, *options.metadata_version()),
        arrow_data: vec![],
    }
}

/// Deserialize an IPC message into a schema
fn schema_from_bytes(bytes: &[u8]) -> Result<Schema> {
    if let Ok(ipc) = ipc::root_as_message(bytes) {
        if let Some((schema, _)) = ipc.header_as_schema().map(ipc::fb_to_schema) {
            Ok(schema)
        } else {
            Err(ArrowError::Ipc("Unable to get head as schema".to_string()))
        }
    } else {
        Err(ArrowError::Ipc("Unable to get root as message".to_string()))
    }
}

/// Try convert `FlightData` into an Arrow Schema
///
/// Returns an error if the `FlightData` header is not a valid IPC schema
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

/// Try convert `SchemaResult` into an Arrow Schema
///
/// Returns an error if the `FlightData` header is not a valid IPC schema
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

/// Convert a FlightData message to a RecordBatch
pub fn flight_data_to_arrow_batch(
    data: &FlightData,
    schema: Arc<Schema>,
    is_little_endian: bool,
    dictionaries_by_field: &[Option<Arc<dyn Array>>],
) -> Result<RecordBatch> {
    // check that the data_header is a record batch message
    let message = ipc::root_as_message(&data.data_header[..])
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
                schema,
                is_little_endian,
                &dictionaries_by_field,
                &mut reader,
                0,
            )
        })?
}
