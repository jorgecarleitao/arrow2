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

//! Dense Arrow Tensor defined in
//! [`format/Tensor.fbs`](https://github.com/apache/arrow/blob/master/format/Tensor.fbs).

use crate::{
    buffer::Buffer,
    error::{ArrowError, Result},
    types::NativeType,
};

// Computes the strides required assuming a row major memory layout
fn compute_row_major_strides(shape: &[usize]) -> Vec<usize> {
    let mut total_locations = shape.iter().product();

    shape
        .iter()
        .map(|val| {
            total_locations /= *val;
            total_locations
        })
        .collect()
}

/// Dense Arrow Tensor
///
/// This dense tensor stores the data information in a Buffer<T>. This
/// means that all the values that define the tensor must be buffer
/// contained within the tensor
#[derive(Debug)]
pub struct Tensor<T: NativeType> {
    /// Internal buffer data of type T
    buffer: Buffer<T>,
    shape: Option<Vec<usize>>,
    strides: Option<Vec<usize>>,
    names: Option<Vec<String>>,
}

impl<'a, T: NativeType> Tensor<T> {
    /// Creates a new dense tensor using a buffer of Type T.
    ///
    /// When no shape is given, then the buffer has to be of len 1. This
    /// represents a 1x1 tensor. If no stride is used, then by default the
    /// tensor strides are row based calculated.
    ///
    /// # Examples
    /// ```
    /// use arrow2::tensor::dense::Tensor;
    /// use arrow2::buffer::{Buffer, MutableBuffer};
    /// let mut b = MutableBuffer::<i32>::new();
    /// for i in 0..12 {
    ///     b.push(i);
    /// }
    /// let buffer: Buffer<i32> = b.into();
    /// let shape = Some(vec![3, 4]);
    /// let strides = Some(vec![4, 1]);
    /// let names = Some(vec!["a".to_string(), "b".to_string()]);
    /// let tensor = Tensor::try_new(buffer, shape, strides, names).unwrap();
    /// ```
    pub fn try_new(
        buffer: Buffer<T>,
        shape: Option<Vec<usize>>,
        strides: Option<Vec<usize>>,
        names: Option<Vec<String>>,
    ) -> Result<Self> {
        match shape {
            None => Self::validate_no_shape(buffer.len(), &strides, &names)?,
            Some(ref s) => Self::validate_shape(buffer.len(), &s, &strides, &names)?,
        }

        // Checking that the tensor strides used for construction are correct
        // otherwise a row major stride is calculated and used as value for the tensor
        let tensor_strides = {
            if let Some(st) = strides {
                if let Some(ref s) = shape {
                    if compute_row_major_strides(s) == st {
                        Some(st)
                    } else {
                        return Err(ArrowError::InvalidArgumentError(
                            "the input stride does not match the selected shape".to_string(),
                        ));
                    }
                } else {
                    Some(st)
                }
            } else if let Some(ref s) = shape {
                Some(compute_row_major_strides(s))
            } else {
                None
            }
        };

        Ok(Self {
            buffer,
            shape,
            strides: tensor_strides,
            names,
        })
    }

    /// Creates a new dense tensor using an slice of type T.
    ///
    /// When no shape is given, then the buffer has to be of len 1. This
    /// represents a 1x1 tensor. If no stride is used, then by default the
    /// tensor strides are row based calculated.
    ///
    /// # Examples
    /// ```
    /// use arrow2::tensor::dense::Tensor;
    /// let values: Vec<i32> = vec![0i32, 1, 2, 3];
    /// let shape = Some(vec![2, 2]);
    /// let tensor = Tensor::try_from_values(&values, shape, None, None).unwrap();
    /// ```
    pub fn try_from_values(
        values: &[T],
        shape: Option<Vec<usize>>,
        strides: Option<Vec<usize>>,
        names: Option<Vec<String>>,
    ) -> Result<Self> {
        match shape {
            None => Self::validate_no_shape(values.len(), &strides, &names)?,
            Some(ref s) => Self::validate_shape(values.len(), &s, &strides, &names)?,
        }

        // Checking that the tensor strides used for construction are correct
        // otherwise a row major stride is calculated and used as value for the tensor
        let tensor_strides = {
            if let Some(st) = strides {
                if let Some(ref s) = shape {
                    if compute_row_major_strides(s) == st {
                        Some(st)
                    } else {
                        return Err(ArrowError::InvalidArgumentError(
                            "the input stride does not match the selected shape".to_string(),
                        ));
                    }
                } else {
                    Some(st)
                }
            } else if let Some(ref s) = shape {
                Some(compute_row_major_strides(s))
            } else {
                None
            }
        };

        let buffer: Buffer<T> = values.into();

        Ok(Self {
            buffer,
            shape,
            strides: tensor_strides,
            names,
        })
    }

    // Helper function to test that when no shape is used as argument
    // then the buffer, strides and names match the expected values
    fn validate_no_shape(
        size: usize,
        strides: &Option<Vec<usize>>,
        names: &Option<Vec<String>>,
    ) -> Result<()> {
        // Since the tensor doesn't have shape it is assumed to be a 1x1 tensor
        if size > 1 {
            return Err(ArrowError::InvalidArgumentError(
                "underlying buffer should only contain a single tensor element".to_string(),
            ));
        }

        if strides != &None {
            return Err(ArrowError::InvalidArgumentError(
                "expected no strides for tensor with no shape".to_string(),
            ));
        }

        if names != &None {
            return Err(ArrowError::InvalidArgumentError(
                "expected no names for tensor with no shape".to_string(),
            ));
        }

        Ok(())
    }

    // Helper function to test that when there is a shape input as argument
    // then the buffer, strides and names match the expected values
    fn validate_shape(
        size: usize,
        shape: &[usize],
        strides: &Option<Vec<usize>>,
        names: &Option<Vec<String>>,
    ) -> Result<()> {
        // We check that the shape and stride have the same dimensions
        if let Some(ref st) = strides {
            if st.len() != shape.len() {
                return Err(ArrowError::InvalidArgumentError(
                    "shape and stride dimensions differ".to_string(),
                ));
            }
        }

        // We check that the shape and names have the same dimensions
        if let Some(ref n) = names {
            if n.len() != shape.len() {
                return Err(ArrowError::InvalidArgumentError(
                    "number of dimensions and number of dimension names differ".to_string(),
                ));
            }
        }

        let total_elements: usize = shape.iter().product();
        if total_elements != size {
            return Err(ArrowError::InvalidArgumentError(
                "number of elements in buffer does not match dimensions".to_string(),
            ));
        }

        Ok(())
    }

    /// The sizes of the dimensions
    pub fn shape(&self) -> Option<&Vec<usize>> {
        self.shape.as_ref()
    }

    /// Returns a reference to the underlying `Buffer`
    #[inline]
    pub fn values_buffer(&self) -> &Buffer<T> {
        &self.buffer
    }

    /// Returns a reference to the values
    #[inline]
    pub fn values(&self) -> &[T] {
        &self.buffer.as_slice()
    }

    /// Returns a reference to a value in the buffer
    /// The value is accessed via an index of the same size
    /// as the strides vector
    ///
    /// # Examples
    /// Creating a 2x2 vector and accessing the (1,0) element in the tensor
    /// ```
    /// use arrow2::tensor::dense::Tensor;
    /// use arrow2::buffer::Buffer;
    /// let buffer: Buffer<i32> = vec![0i32, 1, 2, 3].into();
    /// let shape = Some(vec![2, 2]);
    /// let tensor = Tensor::try_new(buffer, shape, None, None).unwrap();
    /// assert_eq!(tensor.value(&[1, 0]), Some(&2));
    /// ```
    pub fn value(&self, index: &[usize]) -> Option<&T> {
        // Check if this tensor has strides. The 1x1 doesn't
        // have strides that define the tensor
        match self.strides.as_ref() {
            None => Some(&self.values()[0]),
            Some(strides) => {
                // If the index doesn't have the same len as
                // the strides vector then a None is returned
                if index.len() != strides.len() {
                    return None;
                }

                // The index in the buffer is calculated using the
                // row strides.
                // index = sum(index[i] * stride[i])
                let buf_index = strides
                    .iter()
                    .zip(index)
                    .fold(0usize, |acc, (s, i)| acc + (s * i));

                Some(&self.values()[buf_index])
            }
        }
    }

    /// The number of bytes between elements in each dimension
    pub fn strides(&self) -> Option<&Vec<usize>> {
        self.strides.as_ref()
    }

    /// The names of the dimensions
    pub fn names(&self) -> Option<&Vec<String>> {
        self.names.as_ref()
    }

    /// The number of dimensions
    pub fn ndim(&self) -> usize {
        match &self.shape {
            None => 0,
            Some(v) => v.len(),
        }
    }

    /// The name of dimension i
    pub fn dim_name(&self, i: usize) -> Option<&String> {
        self.names.as_ref().map(|ref names| &names[i])
    }

    /// The total number of elements in the `Tensor`
    pub fn size(&self) -> usize {
        match self.shape {
            None => 0,
            Some(ref s) => s.iter().product(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::MutableBuffer;

    #[test]
    fn test_compute_row_major_strides() {
        assert_eq!(vec![4_usize, 1], compute_row_major_strides(&[3_usize, 4]));
        assert_eq!(vec![6_usize, 1], compute_row_major_strides(&[4_usize, 6]));
        assert_eq!(
            vec![6_usize, 2, 1],
            compute_row_major_strides(&[4_usize, 3, 2])
        );
    }

    #[test]
    fn creating_tensor() {
        let mut b = MutableBuffer::<i32>::new();
        for i in 0..12 {
            b.push(i);
        }
        let buffer: Buffer<i32> = b.into();
        let shape = Some(vec![3, 4]);
        let strides = Some(vec![4, 1]);
        let names = Some(vec!["a".to_string(), "b".to_string()]);

        let tensor = Tensor::try_new(buffer, shape, strides, names).unwrap();

        assert_eq!(tensor.shape(), Some(&vec![3, 4]));
        assert_eq!(tensor.strides(), Some(&vec![4, 1]));
        assert_eq!(tensor.ndim(), 2);
        assert_eq!(
            tensor.names(),
            Some(&vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(tensor.dim_name(0), Some(&"a".to_string()));
        assert_eq!(tensor.dim_name(1), Some(&"b".to_string()));
        assert_eq!(tensor.size(), 12);
    }

    #[test]
    fn creating_1x1_tensor() {
        let buffer: Buffer<i32> = vec![111i32].into();
        let tensor = Tensor::try_new(buffer, None, None, None).unwrap();

        let mut data = tensor.values().iter();
        assert_eq!(data.next(), Some(&111));
    }

    #[test]
    fn test_creating_from_values() {
        let values: Vec<i32> = vec![0i32, 1, 2, 3];
        let shape = Some(vec![2, 2]);

        let tensor = Tensor::try_from_values(&values, shape, None, None).unwrap();
        assert_eq!(tensor.value(&[0, 0]), Some(&0));
        assert_eq!(tensor.value(&[0, 1]), Some(&1));
        assert_eq!(tensor.value(&[1, 0]), Some(&2));
        assert_eq!(tensor.value(&[1, 1]), Some(&3));
    }

    #[test]
    fn test_inconsistent_names() {
        let mut b = MutableBuffer::<i32>::new();
        for i in 0..12 {
            b.push(i);
        }
        let buffer: Buffer<i32> = b.into();
        let shape = Some(vec![3, 4]);
        let strides = Some(vec![4, 1]);
        let names = Some(vec!["a".to_string()]);

        let result = Tensor::try_new(buffer, shape, strides, names);

        if result.is_ok() {
            panic!("shape and names dimensions should be different")
        }
    }

    #[test]
    fn test_inconsistent_strides() {
        let mut b = MutableBuffer::<i32>::new();
        for i in 0..12 {
            b.push(i);
        }
        let buffer: Buffer<i32> = b.into();
        let shape = Some(vec![3, 4]);
        let strides = Some(vec![4, 1, 1]);
        let names = Some(vec!["a".to_string(), "b".to_string()]);

        let result = Tensor::try_new(buffer, shape, strides, names);

        if result.is_ok() {
            panic!("shape and stride dimensions should be different")
        }
    }

    #[test]
    fn test_incorrect_shape() {
        let mut b = MutableBuffer::<i32>::new();
        for i in 0..12 {
            b.push(i);
        }
        let buffer: Buffer<i32> = b.into();
        let shape = Some(vec![3, 10]);

        let result = Tensor::try_new(buffer, shape, None, None);

        if result.is_ok() {
            panic!("number of elements and shape should not match")
        }
    }

    #[test]
    fn test_incorrect_stride() {
        let mut b = MutableBuffer::<i32>::new();
        for i in 0..12 {
            b.push(i);
        }
        let buffer: Buffer<i32> = b.into();
        let shape = Some(vec![3, 4]);
        let strides = Some(vec![4, 3]);

        let result = Tensor::try_new(buffer, shape, strides, None);

        if result.is_ok() {
            panic!("shape and stride should not match")
        }
    }

    #[test]
    fn test_data() {
        let buffer: Buffer<i32> = vec![0i32, 1, 2, 3].into();
        let shape = Some(vec![2, 2]);

        let tensor = Tensor::try_new(buffer, shape, None, None).unwrap();
        let mut data = tensor.values().iter();

        assert_eq!(data.next(), Some(&0));
        assert_eq!(data.next(), Some(&1));
        assert_eq!(data.next(), Some(&2));
        assert_eq!(data.next(), Some(&3));
    }

    #[test]
    fn test_access_data() {
        let buffer: Buffer<i32> = vec![0i32, 1, 2, 3].into();
        let shape = Some(vec![2, 2]);

        let tensor = Tensor::try_new(buffer, shape, None, None).unwrap();
        assert_eq!(tensor.value(&[0, 0]), Some(&0));
        assert_eq!(tensor.value(&[0, 1]), Some(&1));
        assert_eq!(tensor.value(&[1, 0]), Some(&2));
        assert_eq!(tensor.value(&[1, 1]), Some(&3));
    }

    #[test]
    fn test_access_data_1x1() {
        let buffer: Buffer<i32> = vec![111i32].into();
        let tensor = Tensor::try_new(buffer, None, None, None).unwrap();

        // It doesnt matter what index is used, the tensor will
        // return its only value stored
        assert_eq!(tensor.value(&[0, 0]), Some(&111));
        assert_eq!(tensor.value(&[0]), Some(&111));
        assert_eq!(tensor.value(&[1, 1]), Some(&111));
    }
}
