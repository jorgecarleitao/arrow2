mod common;
mod deserialize;
mod reader;
mod stream;

pub use reader::FileReader;
pub use stream::StreamReader;

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::{
        datatypes::Schema,
        io::json_integration::{to_record_batch, ArrowJson},
        record_batch::RecordBatch,
    };

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
        if let Some(dicts) = &arrow_json.dictionaries {
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
}
