//! Convert data between the Arrow memory format and JSON line-delimited records.

mod read;
mod schema;
mod write;

pub use read::*;
pub use write::*;

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::Value;
    use std::fs::{read_to_string, File};

    fn test_write_for_file(test_file: &str) {
        let builder = ReaderBuilder::new()
            .infer_schema(None)
            .with_batch_size(1024);
        let mut reader: Reader<File> = builder
            .build::<File>(File::open(test_file).unwrap())
            .unwrap();
        let batch = reader.next().unwrap().unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(&mut buf);
            writer.write_batches(&[batch]).unwrap();
        }

        let result = String::from_utf8(buf).unwrap();
        let expected = read_to_string(test_file).unwrap();
        for (r, e) in result.lines().zip(expected.lines()) {
            let mut result_json = serde_json::from_str::<Value>(r).unwrap();
            let expected_json = serde_json::from_str::<Value>(e).unwrap();
            if let Value::Object(e) = &expected_json {
                // remove null value from object to make comparision consistent:
                if let Value::Object(r) = result_json {
                    result_json = Value::Object(
                        r.into_iter()
                            .filter(|(k, v)| e.contains_key(k) || *v != Value::Null)
                            .collect(),
                    );
                }
                assert_eq!(result_json, expected_json);
            }
        }
    }

    #[test]
    fn write_basic_rows() {
        test_write_for_file("test/data/basic.json");
    }

    #[test]
    fn write_arrays() {
        test_write_for_file("test/data/arrays.json");
    }

    #[test]
    fn write_basic_nulls() {
        test_write_for_file("test/data/basic_nulls.json");
    }
}
