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

#[cfg(test)]
pub(crate) mod tests {
    use crate::{
        datatypes::Schema,
        io::json_integration::{to_record_batch, ArrowJson},
        record_batch::RecordBatch,
    };
    use crate::{error::Result, io::ipc::read::read_stream_metadata};

    use crate::io::ipc::read::StreamReader;

    use std::{collections::HashMap, convert::TryFrom, fs::File, io::Read};

    use flate2::read::GzDecoder;

    /// Read gzipped JSON file
    pub fn read_gzip_json(version: &str, file_name: &str) -> (Schema, Vec<RecordBatch>) {
        let testdata = crate::util::test_util::arrow_test_data();
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.json.gz",
            testdata, version, file_name
        ))
        .unwrap();
        let mut gz = GzDecoder::new(&file);
        let mut s = String::new();
        gz.read_to_string(&mut s).unwrap();
        // convert to Arrow JSON
        let arrow_json: ArrowJson = serde_json::from_str(&s).unwrap();

        let schema = serde_json::to_value(arrow_json.schema).unwrap();
        let schema = Schema::try_from(&schema).unwrap();

        // read dictionaries
        let mut dictionaries = HashMap::new();
        if let Some(dicts) = arrow_json.dictionaries {
            for json_dict in dicts {
                // TODO: convert to a concrete Arrow type
                dictionaries.insert(json_dict.id, json_dict);
            }
        }

        let batches = arrow_json
            .batches
            .iter()
            .map(|batch| to_record_batch(&schema, batch, &dictionaries))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        (schema, batches)
    }

    pub fn read_arrow_stream(version: &str, file_name: &str) -> (Schema, Vec<RecordBatch>) {
        let testdata = crate::util::test_util::arrow_test_data();
        let mut file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.stream",
            testdata, version, file_name
        ))
        .unwrap();

        let metadata = read_stream_metadata(&mut file).unwrap();
        let reader = StreamReader::new(file, metadata);

        let schema = reader.schema();

        (
            schema.as_ref().clone(),
            reader.collect::<Result<_>>().unwrap(),
        )
    }
}
