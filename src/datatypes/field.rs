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

use std::collections::BTreeMap;

use crate::error::{ArrowError, Result};

use super::DataType;

/// Contains the meta-data for a single relative type.
///
/// The `Schema` object is an ordered collection of `Field` objects.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Field {
    pub(super) name: String,
    pub(super) data_type: DataType,
    pub(super) nullable: bool,
    pub(super) dict_id: i64,
    pub(super) dict_is_ordered: bool,
    /// A map of key-value pairs containing additional custom meta data.
    pub(super) metadata: Option<BTreeMap<String, String>>,
}

impl Field {
    /// Creates a new field
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: None,
        }
    }

    /// Creates a new field
    pub fn new_dict(
        name: &str,
        data_type: DataType,
        nullable: bool,
        dict_id: i64,
        dict_is_ordered: bool,
    ) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            nullable,
            dict_id,
            dict_is_ordered,
            metadata: None,
        }
    }

    /// Sets the `Field`'s optional custom metadata.
    /// The metadata is set as `None` for empty map.
    #[inline]
    pub fn set_metadata(&mut self, metadata: Option<BTreeMap<String, String>>) {
        // To make serde happy, convert Some(empty_map) to None.
        self.metadata = None;
        if let Some(v) = metadata {
            if !v.is_empty() {
                self.metadata = Some(v);
            }
        }
    }

    /// Returns the immutable reference to the `Field`'s optional custom metadata.
    #[inline]
    pub const fn metadata(&self) -> &Option<BTreeMap<String, String>> {
        &self.metadata
    }

    /// Returns an immutable reference to the `Field`'s name.
    #[inline]
    pub const fn name(&self) -> &String {
        &self.name
    }

    /// Returns an immutable reference to the `Field`'s  data-type.
    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Indicates whether this `Field` supports null values.
    #[inline]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the dictionary ID, if this is a dictionary type.
    #[inline]
    pub const fn dict_id(&self) -> Option<i64> {
        match self.data_type {
            DataType::Dictionary(_, _) => Some(self.dict_id),
            _ => None,
        }
    }

    /// Returns whether this `Field`'s dictionary is ordered, if this is a dictionary type.
    #[inline]
    pub const fn dict_is_ordered(&self) -> Option<bool> {
        match self.data_type {
            DataType::Dictionary(_, _) => Some(self.dict_is_ordered),
            _ => None,
        }
    }

    /// Merge field into self if it is compatible. Struct will be merged recursively.
    /// NOTE: `self` may be updated to unexpected state in case of merge failure.
    ///
    /// Example:
    ///
    /// ```
    /// use arrow::datatypes::*;
    ///
    /// let mut field = Field::new("c1", DataType::Int64, false);
    /// assert!(field.try_merge(&Field::new("c1", DataType::Int64, true)).is_ok());
    /// assert!(field.is_nullable());
    /// ```
    pub fn try_merge(&mut self, from: &Field) -> Result<()> {
        // merge metadata
        match (self.metadata(), from.metadata()) {
            (Some(self_metadata), Some(from_metadata)) => {
                let mut merged = self_metadata.clone();
                for (key, from_value) in from_metadata {
                    if let Some(self_value) = self_metadata.get(key) {
                        if self_value != from_value {
                            return Err(ArrowError::Schema(format!(
                                "Fail to merge field due to conflicting metadata data value for key {}", key),
                            ));
                        }
                    } else {
                        merged.insert(key.clone(), from_value.clone());
                    }
                }
                self.set_metadata(Some(merged));
            }
            (None, Some(from_metadata)) => {
                self.set_metadata(Some(from_metadata.clone()));
            }
            _ => {}
        }
        if from.dict_id != self.dict_id {
            return Err(ArrowError::Schema(
                "Fail to merge schema Field due to conflicting dict_id".to_string(),
            ));
        }
        if from.dict_is_ordered != self.dict_is_ordered {
            return Err(ArrowError::Schema(
                "Fail to merge schema Field due to conflicting dict_is_ordered".to_string(),
            ));
        }
        match &mut self.data_type {
            DataType::Struct(nested_fields) => match &from.data_type {
                DataType::Struct(from_nested_fields) => {
                    for from_field in from_nested_fields {
                        let mut is_new_field = true;
                        for self_field in nested_fields.iter_mut() {
                            if self_field.name != from_field.name {
                                continue;
                            }
                            is_new_field = false;
                            self_field.try_merge(&from_field)?;
                        }
                        if is_new_field {
                            nested_fields.push(from_field.clone());
                        }
                    }
                }
                _ => {
                    return Err(ArrowError::Schema(
                        "Fail to merge schema Field due to conflicting datatype".to_string(),
                    ));
                }
            },
            DataType::Union(nested_fields) => match &from.data_type {
                DataType::Union(from_nested_fields) => {
                    for from_field in from_nested_fields {
                        let mut is_new_field = true;
                        for self_field in nested_fields.iter_mut() {
                            if from_field == self_field {
                                is_new_field = false;
                                break;
                            }
                        }
                        if is_new_field {
                            nested_fields.push(from_field.clone());
                        }
                    }
                }
                _ => {
                    return Err(ArrowError::Schema(
                        "Fail to merge schema Field due to conflicting datatype".to_string(),
                    ));
                }
            },
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Interval(_)
            | DataType::LargeList(_)
            | DataType::List(_)
            | DataType::Dictionary(_, _)
            | DataType::FixedSizeList(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Decimal(_, _) => {
                if self.data_type != from.data_type {
                    return Err(ArrowError::Schema(
                        "Fail to merge schema Field due to conflicting datatype".to_string(),
                    ));
                }
            }
        }
        if from.nullable {
            self.nullable = from.nullable;
        }

        Ok(())
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
