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

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek};
use std::sync::Arc;

use crate::array::*;
use crate::datatypes::{DataType, Field, Schema};
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

use super::super::gen;
use super::deserialize::read;

type ArrayRef = Arc<dyn Array>;

/// Creates a record batch from binary data using the `ipc::RecordBatch` indexes and the `Schema`
pub fn read_record_batch<R: Read + Seek>(
    batch: gen::Message::RecordBatch,
    schema: Schema,
    dictionaries: &[Option<ArrayRef>],
    reader: &mut R,
    block_offset: u64,
) -> Result<RecordBatch> {
    let buffers = batch
        .buffers()
        .ok_or_else(|| ArrowError::IPC("Unable to get buffers from IPC RecordBatch".to_string()))?;
    let mut buffers: VecDeque<&gen::Schema::Buffer> = buffers.iter().collect();
    let field_nodes = batch.nodes().ok_or_else(|| {
        ArrowError::IPC("Unable to get field nodes from IPC RecordBatch".to_string())
    })?;

    // This is a bug fix: we should have one dictionary per node, not schema field
    let dictionaries = dictionaries.iter().chain(std::iter::repeat(&None));

    let mut field_nodes = field_nodes
        .iter()
        .zip(dictionaries)
        .collect::<VecDeque<_>>();

    let arrays = schema
        .fields()
        .iter()
        .map(|field| {
            read(
                &mut field_nodes,
                field.data_type().clone(),
                &mut buffers,
                reader,
                block_offset,
                schema.is_little_endian,
            )
        })
        .collect::<std::io::Result<Vec<_>>>()?;

    RecordBatch::try_new(schema.clone(), arrays)
}

/// Read the dictionary from the buffer and provided metadata,
/// updating the `dictionaries_by_field` with the resulting dictionary
pub fn read_dictionary<R: Read + Seek>(
    batch: gen::Message::DictionaryBatch,
    schema: &Schema,
    dictionaries_by_field: &mut [Option<ArrayRef>],
    reader: &mut R,
    block_offset: u64,
) -> Result<()> {
    if batch.isDelta() {
        return Err(ArrowError::NotYetImplemented(
            "delta dictionary batches not supported".to_string(),
        ));
    }

    let id = batch.id();
    let fields_using_this_dictionary = schema.fields_with_dict_id(id);
    let first_field = fields_using_this_dictionary.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError("dictionary id not found in schema".to_string())
    })?;

    // As the dictionary batch does not contain the type of the
    // values array, we need to retrieve this from the schema.
    // Get an array representing this dictionary's values.
    let dictionary_values: ArrayRef = match first_field.data_type() {
        DataType::Dictionary(_, ref value_type) => {
            // Make a fake schema for the dictionary batch.
            let schema = Schema {
                fields: vec![Field::new("", value_type.as_ref().clone(), false)],
                metadata: HashMap::new(),
                is_little_endian: schema.is_little_endian,
            };
            // Read a single column
            let record_batch = read_record_batch(
                batch.data().unwrap(),
                schema,
                dictionaries_by_field,
                reader,
                block_offset,
            )?;
            Some(record_batch.column(0).clone())
        }
        _ => None,
    }
    .ok_or_else(|| {
        ArrowError::InvalidArgumentError("dictionary id not found in schema".to_string())
    })?;

    // for all fields with this dictionary id, update the dictionaries vector
    // in the reader. Note that a dictionary batch may be shared between many fields.
    // We don't currently record the isOrdered field. This could be general
    // attributes of arrays.
    for (i, field) in schema.fields().iter().enumerate() {
        if field.dict_id() == Some(id) {
            // Add (possibly multiple) array refs to the dictionaries array.
            dictionaries_by_field[i] = Some(dictionary_values.clone());
        }
    }

    Ok(())
}
