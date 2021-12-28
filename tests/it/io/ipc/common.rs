use std::{collections::HashMap, fs::File, io::Read, sync::Arc};

use arrow2::{
    array::Array, columns::Columns, datatypes::Schema, error::Result,
    io::ipc::read::read_stream_metadata, io::ipc::read::StreamReader, io::ipc::IpcField,
    io::json_integration::read, io::json_integration::ArrowJson,
};

use flate2::read::GzDecoder;

/// Read gzipped JSON file
pub fn read_gzip_json(
    version: &str,
    file_name: &str,
) -> Result<(Schema, Vec<IpcField>, Vec<Columns<Arc<dyn Array>>>)> {
    let testdata = crate::test_util::arrow_test_data();
    let file = File::open(format!(
        "{}/arrow-ipc-stream/integration/{}/{}.json.gz",
        testdata, version, file_name
    ))
    .unwrap();
    let mut gz = GzDecoder::new(&file);
    let mut s = String::new();
    gz.read_to_string(&mut s).unwrap();
    // convert to Arrow JSON
    let arrow_json: ArrowJson = serde_json::from_str(&s)?;

    let schema = serde_json::to_value(arrow_json.schema).unwrap();

    let (schema, ipc_fields) = read::deserialize_schema(&schema)?;

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
        .map(|batch| read::deserialize_columns(&schema, &ipc_fields, batch, &dictionaries))
        .collect::<Result<Vec<_>>>()?;

    Ok((schema, ipc_fields, batches))
}

pub fn read_arrow_stream(
    version: &str,
    file_name: &str,
) -> (Schema, Vec<IpcField>, Vec<Columns<Arc<dyn Array>>>) {
    let testdata = crate::test_util::arrow_test_data();
    let mut file = File::open(format!(
        "{}/arrow-ipc-stream/integration/{}/{}.stream",
        testdata, version, file_name
    ))
    .unwrap();

    let metadata = read_stream_metadata(&mut file).unwrap();
    let reader = StreamReader::new(file, metadata);

    let schema = reader.metadata().schema.as_ref().clone();
    let ipc_fields = reader.metadata().ipc_schema.fields.clone();

    (
        schema,
        ipc_fields,
        reader
            .map(|x| x.map(|x| x.unwrap()))
            .collect::<Result<_>>()
            .unwrap(),
    )
}
