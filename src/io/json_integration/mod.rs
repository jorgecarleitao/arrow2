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

//! Utils for JSON integration testing
//!
//! These utilities define structs that read the integration JSON format for integration testing purposes.

use serde_derive::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::datatypes::*;
use crate::io::json::ToJson;

mod read;
pub use read::to_record_batch;

/// A struct that represents an Arrow file with a schema and record batches
#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJson {
    pub schema: ArrowJsonSchema,
    pub batches: Vec<ArrowJsonBatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionaries: Option<Vec<ArrowJsonDictionaryBatch>>,
}

/// A struct that partially reads the Arrow JSON schema.
///
/// Fields are left as JSON `Value` as they vary by `DataType`
#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJsonSchema {
    pub fields: Vec<ArrowJsonField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

/// Fields are left as JSON `Value` as they vary by `DataType`
#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJsonField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: Value,
    pub nullable: bool,
    pub children: Vec<ArrowJsonField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dictionary: Option<ArrowJsonFieldDictionary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

impl From<&Field> for ArrowJsonField {
    fn from(field: &Field) -> Self {
        let metadata_value = match field.metadata() {
            Some(kv_list) => {
                let mut array = Vec::new();
                for (k, v) in kv_list {
                    let mut kv_map = Map::new();
                    kv_map.insert(k.clone(), Value::String(v.clone()));
                    array.push(Value::Object(kv_map));
                }
                if !array.is_empty() {
                    Some(Value::Array(array))
                } else {
                    None
                }
            }
            _ => None,
        };

        Self {
            name: field.name().to_string(),
            field_type: field.data_type().to_json(),
            nullable: field.is_nullable(),
            children: vec![],
            dictionary: None, // TODO: not enough info
            metadata: metadata_value,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJsonFieldDictionary {
    pub id: i64,
    #[serde(rename = "indexType")]
    pub index_type: DictionaryIndexType,
    #[serde(rename = "isOrdered")]
    pub is_ordered: bool,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DictionaryIndexType {
    pub name: String,
    #[serde(rename = "isSigned")]
    pub is_signed: bool,
    #[serde(rename = "bitWidth")]
    pub bit_width: i64,
}

/// A struct that partially reads the Arrow JSON record batch
#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJsonBatch {
    count: usize,
    pub columns: Vec<ArrowJsonColumn>,
}

/// A struct that partially reads the Arrow JSON dictionary batch
#[derive(Deserialize, Serialize, Debug)]
#[allow(non_snake_case)]
pub struct ArrowJsonDictionaryBatch {
    pub id: i64,
    pub data: ArrowJsonBatch,
}

/// A struct that partially reads the Arrow JSON column/array
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ArrowJsonColumn {
    name: String,
    pub count: usize,
    #[serde(rename = "VALIDITY")]
    pub validity: Option<Vec<u8>>,
    #[serde(rename = "DATA")]
    pub data: Option<Vec<Value>>,
    #[serde(rename = "OFFSET")]
    pub offset: Option<Vec<Value>>, // leaving as Value as 64-bit offsets are strings
    pub children: Option<Vec<ArrowJsonColumn>>,
}
