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
use super::DataType;
use crate::array::Array;
use core::hash::Hash;
use std::collections::HashMap;

pub trait Extension: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
    /// Returns physical_type
    fn data_type(&self) -> &DataType;
    fn metadata(&self) -> &Option<HashMap<String, String>>;

    /// Returns a function of index returning the string representation of the _value_ of `array`
    /// optional, fall back to the physical data_type's `get_display`
    fn get_display<'a>(&self, _array: &'a dyn Array) -> Option<Box<dyn Fn(usize) -> String + 'a>> {
        None
    }
}

impl PartialEq for dyn Extension + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
            && self.data_type() == other.data_type()
            && self.metadata() == other.metadata()
    }
}

impl Eq for dyn Extension + '_ {}

impl Hash for dyn Extension + '_ {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.name().as_bytes());
        state.write(self.data_type().to_string().as_bytes());
        if let Some(metadata) = self.metadata() {
            for (k, v) in metadata.iter() {
                state.write(k.as_bytes());
                state.write(v.as_bytes());
            }
        }
    }
}

impl PartialOrd for dyn Extension + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name().partial_cmp(other.name())
    }
}

impl Ord for dyn Extension + '_ {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name().cmp(other.name())
    }
}
