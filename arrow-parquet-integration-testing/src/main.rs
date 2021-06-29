use std::fs::File;
use std::{collections::HashMap, convert::TryFrom, io::Read};

use arrow2::error::Result;
use arrow2::io::parquet::write::RowGroupIterator;
use arrow2::io::{
    json_integration::ArrowJson,
    parquet::write::{write_file, CompressionCodec, WriteOptions},
};
use arrow2::{datatypes::Schema, io::json_integration::to_record_batch, record_batch::RecordBatch};

use clap::{App, Arg};

use flate2::read::GzDecoder;

/// Read gzipped JSON file
fn read_gzip_json(version: &str, file_name: &str) -> (Schema, Vec<RecordBatch>) {
    let path = format!(
        "../testing/arrow-testing/data/arrow-ipc-stream/integration/{}/{}.json.gz",
        version, file_name
    );
    let file = File::open(path).unwrap();
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

fn main() -> Result<()> {
    let matches = App::new("json-parquet-integration")
        .arg(
            Arg::with_name("json")
                .long("json")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("write_path")
                .long("output")
                .required(true)
                .takes_value(true),
        )
        .get_matches();
    let json_file = matches
        .value_of("json")
        .expect("must provide path to json file");
    let write_path = matches
        .value_of("write_path")
        .expect("must provide path to write parquet");

    let (schema, batches) = read_gzip_json("1.0.0-littleendian", json_file);

    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionCodec::Uncompressed,
    };

    let row_groups = RowGroupIterator::try_new(batches.into_iter().map(Ok), &schema, options)?;
    let parquet_schema = row_groups.parquet_schema().clone();

    let mut writer = File::create(write_path)?;

    write_file(
        &mut writer,
        row_groups,
        &schema,
        parquet_schema,
        options,
        None,
    )
}
