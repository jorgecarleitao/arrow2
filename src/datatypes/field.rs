use crate::error::{ArrowError, Result};

use super::{DataType, Metadata};

/// Represents the metadata of a "column".
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Field {
    /// Its name
    pub name: String,
    /// Its logical [`DataType`]
    pub data_type: DataType,
    /// Its nullability
    pub nullable: bool,
    /// Additional custom (opaque) metadata.
    pub metadata: Metadata,
}

impl Field {
    /// Creates a new [`Field`].
    pub fn new<T: Into<String>>(name: T, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.into(),
            data_type,
            nullable,
            metadata: Default::default(),
        }
    }

    /// Creates a new [`Field`] with metadata.
    #[inline]
    pub fn with_metadata(self, metadata: Metadata) -> Self {
        Self {
            name: self.name,
            data_type: self.data_type,
            nullable: self.nullable,
            metadata,
        }
    }

    /// Returns the [`Field`]'s metadata.
    #[inline]
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Returns the [`Field`]'s name.
    #[inline]
    pub const fn name(&self) -> &String {
        &self.name
    }

    /// Returns the [`Field`]'s [`DataType`].
    #[inline]
    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns whether the [`Field`] should contain null values.
    #[inline]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Merge field into self if it is compatible. Struct will be merged recursively.
    /// NOTE: `self` may be updated to unexpected state in case of merge failure.
    ///
    /// Example:
    ///
    /// ```
    /// use arrow2::datatypes::*;
    ///
    /// let mut field = Field::new("c1", DataType::Int64, false);
    /// assert!(field.try_merge(&Field::new("c1", DataType::Int64, true)).is_ok());
    /// assert!(field.is_nullable());
    /// ```
    pub fn try_merge(&mut self, from: &Field) -> Result<()> {
        // merge metadata
        for (key, from_value) in from.metadata() {
            if let Some(self_value) = self.metadata.get(key) {
                if self_value != from_value {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Fail to merge field due to conflicting metadata data value for key {}",
                        key
                    )));
                }
            } else {
                self.metadata.insert(key.clone(), from_value.clone());
            }
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
                            self_field.try_merge(from_field)?;
                        }
                        if is_new_field {
                            nested_fields.push(from_field.clone());
                        }
                    }
                }
                _ => {
                    return Err(ArrowError::InvalidArgumentError(
                        "Fail to merge schema Field due to conflicting datatype".to_string(),
                    ));
                }
            },
            DataType::Union(nested_fields, _, _) => match &from.data_type {
                DataType::Union(from_nested_fields, _, _) => {
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
                    return Err(ArrowError::InvalidArgumentError(
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
            | DataType::Dictionary(_, _, _)
            | DataType::FixedSizeList(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Extension(_, _, _)
            | DataType::Map(_, _)
            | DataType::Decimal(_, _) => {
                if self.data_type != from.data_type {
                    return Err(ArrowError::InvalidArgumentError(
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
