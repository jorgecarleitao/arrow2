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

use csv::StringRecord;
use lexical_core;

use crate::{array::Primitive, datatypes::*};
use crate::{array::PrimitiveArray, buffer::NativeType};

pub trait PrimitiveParser<T: NativeType + lexical_core::FromLexical, E> {
    fn parse(&self, string: &str, _: &DataType, _: usize) -> Result<Option<T>, E> {
        // default behavior is infalible: `None` if unable to parse
        Ok(lexical_core::parse(string.as_bytes()).ok())
    }
}

pub fn new_primitive_array<
    T: NativeType + lexical_core::FromLexical,
    E,
    P: PrimitiveParser<T, E>,
>(
    line_number: usize,
    rows: &[StringRecord],
    col_idx: usize,
    data_type: &DataType,
    parser: &P,
) -> Result<PrimitiveArray<T>, E> {
    let iter = rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| match row.get(col_idx) {
            Some(s) => {
                if s.is_empty() {
                    return Ok(None);
                }
                parser.parse(s, &data_type, line_number + row_index)
            }
            None => Ok(None),
        });
    // Soundness: slice is trusted len.
    Ok(unsafe { Primitive::<T>::try_from_trusted_len_iter(iter) }?.to(data_type.clone()))
}
