use std::collections::BTreeMap;

use serde_derive::{Deserialize, Serialize};

use super::DataType;

/// Contains the meta-data for a single relative type.
///
/// The `Schema` object is an ordered collection of `Field` objects.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Field {
    name: String,
    data_type: DataType,
    nullable: bool,
    dict_id: i64,
    dict_is_ordered: bool,
    /// A map of key-value pairs containing additional custom meta data.
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<BTreeMap<String, String>>,
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
}
