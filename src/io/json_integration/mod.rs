//! Utils for JSON integration testing
//!
//! These utilities define structs that read the integration JSON format for integration testing purposes.

use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

pub mod read;
pub mod write;

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

#[derive(Deserialize, Serialize, Debug)]
pub struct ArrowJsonFieldDictionary {
    pub id: i64,
    #[serde(rename = "indexType")]
    pub index_type: IntegerType,
    #[serde(rename = "isOrdered")]
    pub is_ordered: bool,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct IntegerType {
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
    #[serde(rename = "TYPE_ID")]
    pub type_id: Option<Vec<Value>>, // for union types
    pub children: Option<Vec<ArrowJsonColumn>>,
}
