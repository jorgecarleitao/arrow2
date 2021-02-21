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

//! A two-dimensional batch of column-oriented data with a defined
//! [schema](crate::datatypes::Schema).

use std::sync::Arc;

use crate::array::*;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

type ArrayRef = Arc<dyn Array>;

/// A two-dimensional batch of column-oriented data with a defined
/// [schema](crate::datatypes::Schema).
///
/// A `RecordBatch` is a two-dimensional dataset of a number of
/// contiguous arrays, each the same length.
/// A record batch has a schema which must match its arrays’
/// datatypes.
///
/// Record batches are a convenient unit of work for various
/// serialization and computation functions, possibly incremental.
/// See also [CSV reader](crate::csv::Reader) and
/// [JSON reader](crate::json::Reader).
#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    schema: Schema,
    columns: Vec<ArrayRef>,
}

impl RecordBatch {
    /// Creates a `RecordBatch` from a schema and columns.
    ///
    /// Expects the following:
    ///  * the vec of columns to not be empty
    ///  * the schema and column data types to have equal lengths
    ///    and match
    ///  * each array in columns to have the same length
    ///
    /// If the conditions are not met, an error is returned.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use arrow::record_batch::RecordBatch;
    ///
    /// # fn main() -> arrow::error::Result<()> {
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema),
    ///     vec![Arc::new(id_array)]
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_new(schema: Schema, columns: Vec<ArrayRef>) -> Result<Self> {
        let options = RecordBatchOptions::default();
        Self::validate_new_batch(&schema, columns.as_slice(), &options)?;
        Ok(RecordBatch { schema, columns })
    }

    /// Creates a `RecordBatch` from a schema and columns, with additional options,
    /// such as whether to strictly validate field names.
    ///
    /// See [`RecordBatch::try_new`] for the expected conditions.
    pub fn try_new_with_options(
        schema: Schema,
        columns: Vec<ArrayRef>,
        options: &RecordBatchOptions,
    ) -> Result<Self> {
        Self::validate_new_batch(&schema, columns.as_slice(), options)?;
        Ok(RecordBatch { schema, columns })
    }

    /// Creates a new empty [`RecordBatch`].
    pub fn new_empty(schema: Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type().clone()).into())
            .collect();
        RecordBatch { schema, columns }
    }

    /// Validate the schema and columns using [`RecordBatchOptions`]. Returns an error
    /// if any validation check fails.
    fn validate_new_batch(
        schema: &Schema,
        columns: &[ArrayRef],
        options: &RecordBatchOptions,
    ) -> Result<()> {
        // check that there are some columns
        if columns.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "at least one column must be defined to create a record batch".to_string(),
            ));
        }
        // check that number of fields in schema match column length
        if schema.fields().len() != columns.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "number of columns({}) must match number of fields({}) in schema",
                columns.len(),
                schema.fields().len(),
            )));
        }
        // check that all columns have the same row count, and match the schema
        let len = columns[0].len();

        // This is a bit repetitive, but it is better to check the condition outside the loop
        if options.match_field_names {
            for (i, column) in columns.iter().enumerate() {
                if column.len() != len {
                    return Err(ArrowError::InvalidArgumentError(
                        "all columns in a record batch must have the same length".to_string(),
                    ));
                }
                if column.data_type() != schema.field(i).data_type() {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "column types must match schema types, expected {:?} but found {:?} at column index {}",
                        schema.field(i).data_type(),
                        column.data_type(),
                        i)));
                }
            }
        } else {
            for (i, column) in columns.iter().enumerate() {
                if column.len() != len {
                    return Err(ArrowError::InvalidArgumentError(
                        "all columns in a record batch must have the same length".to_string(),
                    ));
                }
                if !column
                    .data_type()
                    .equals_datatype(schema.field(i).data_type())
                {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "column types must match schema types, expected {:?} but found {:?} at column index {}",
                        schema.field(i).data_type(),
                        column.data_type(),
                        i)));
                }
            }
        }

        Ok(())
    }

    /// Returns the [`Schema`](crate::datatypes::Schema) of the record batch.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the number of columns in the record batch.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use arrow::record_batch::RecordBatch;
    ///
    /// # fn main() -> arrow::error::Result<()> {
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)])?;
    ///
    /// assert_eq!(batch.num_columns(), 1);
    /// # Ok(())
    /// # }
    /// ```
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns the number of rows in each column.
    ///
    /// # Panics
    ///
    /// Panics if the `RecordBatch` contains no columns.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use arrow::record_batch::RecordBatch;
    ///
    /// # fn main() -> arrow::error::Result<()> {
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)])?;
    ///
    /// assert_eq!(batch.num_rows(), 5);
    /// # Ok(())
    /// # }
    /// ```
    pub fn num_rows(&self) -> usize {
        self.columns[0].len()
    }

    /// Get a reference to a column's array by index.
    ///
    /// # Panics
    ///
    /// Panics if `index` is outside of `0..num_columns`.
    pub fn column(&self, index: usize) -> &ArrayRef {
        &self.columns[index]
    }

    /// Get a reference to all columns in the record batch.
    pub fn columns(&self) -> &[ArrayRef] {
        &self.columns[..]
    }
}

/// Options that control the behaviour used when creating a [`RecordBatch`].
#[derive(Debug)]
pub struct RecordBatchOptions {
    /// Match field names of structs and lists. If set to `true`, the names must match.
    pub match_field_names: bool,
}

impl Default for RecordBatchOptions {
    fn default() -> Self {
        Self {
            match_field_names: true,
        }
    }
}

impl From<&StructArray> for RecordBatch {
    /// Create a record batch from struct array.
    ///
    /// This currently does not flatten and nested struct types
    fn from(struct_array: &StructArray) -> Self {
        if let DataType::Struct(fields) = struct_array.data_type() {
            let schema = Schema::new(fields.clone());
            let columns = struct_array.values().to_vec();
            RecordBatch { schema, columns }
        } else {
            unreachable!("unable to get datatype as struct")
        }
    }
}

impl Into<StructArray> for RecordBatch {
    fn into(self) -> StructArray {
        let (fields, values) = self
            .schema
            .fields
            .iter()
            .zip(self.columns.iter())
            .map(|t| (t.0.clone(), t.1.clone()))
            .unzip()
            .into();
        StructArray::from_data(fields, values, None)
    }
}

/// Trait for types that can read `RecordBatch`'s.
pub trait RecordBatchReader: Iterator<Item = Result<RecordBatch>> {
    /// Returns the schema of this `RecordBatchReader`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// reader should have the same schema as returned from this method.
    fn schema(&self) -> &Schema;

    /// Reads the next `RecordBatch`.
    #[deprecated(
        since = "2.0.0",
        note = "This method is deprecated in favour of `next` from the trait Iterator."
    )]
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.next().transpose()
    }
}
