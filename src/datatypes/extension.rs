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
use crate::error::{ArrowError, Result};

use core::hash::Hash;
use lazy_static::lazy_static;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

pub trait Extension: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
    /// Returns physical_type
    fn data_type(&self) -> &DataType;

    // Eg: metadata: Some({"ARROW:extension:metadata": "uuid-serialized", "ARROW:extension:name": "uuid"})
    fn metadata(&self) -> &BTreeMap<String, String>;

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

        for (k, v) in self.metadata().iter() {
            state.write(k.as_bytes());
            state.write(v.as_bytes());
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

type ExtensionFactoryRef = Arc<RwLock<BTreeMap<String, Arc<dyn Extension>>>>;

lazy_static! {
    static ref EXTENSION_FACTORY: ExtensionFactoryRef = {
        let map: ExtensionFactoryRef = Arc::new(RwLock::new(BTreeMap::new()));
        map
    };
}

pub fn register_extension_type(ex: Box<dyn Extension>) -> Result<()> {
    // validate the metadata for the extension type
    let name = ex.name();
    match ex.metadata().get("ARROW:extension:name") {
        Some(v) if v == name => {}
        _ => {
            return Err(ArrowError::Schema(
                "Extension metadata must has right value in key: 'ARROW:extension:name'"
                    .to_string(),
            ))
        }
    }

    let mut map = EXTENSION_FACTORY.write().unwrap();
    let ex: Arc<dyn Extension> = Arc::from(ex);
    map.insert(ex.name().to_owned(), ex);

    Ok(())
}

pub fn unregister_extension_type(ex: Box<dyn Extension>) {
    let mut map = EXTENSION_FACTORY.write().unwrap();
    map.remove(ex.name());
}

pub fn get_extension_type(ex: Box<dyn Extension>) -> Option<Arc<dyn Extension>> {
    let map = EXTENSION_FACTORY.read().unwrap();
    let result = map.get(ex.name()).map(|f| f.clone());

    result
}
