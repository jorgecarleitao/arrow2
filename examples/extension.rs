use std::io::{Cursor, Seek, Write};
use std::sync::Arc;

use arrow2::array::*;
use arrow2::datatypes::*;
use arrow2::error::Result;
use arrow2::io::ipc::read;
use arrow2::io::ipc::write;
use arrow2::record_batch::RecordBatch;

fn main() -> Result<()> {
    // declare an extension.
    let extension_type =
        DataType::Extension("date16".to_string(), Box::new(DataType::UInt16), None);

    // initialize an array with it.
    let array = UInt16Array::from_slice([1, 2]).to(extension_type.clone());

    // from here on, it works as usual
    let buffer = Cursor::new(vec![]);

    // write to IPC
    let result_buffer = write_ipc(buffer, array)?;

    // read it back
    let batch = read_ipc(&result_buffer.into_inner())?;

    // and verify that the datatype is preserved.
    let array = &batch.columns()[0];
    assert_eq!(array.data_type(), &extension_type);

    // see https://arrow.apache.org/docs/format/Columnar.html#extension-types
    // for consuming by other consumers.
    Ok(())
}

fn write_ipc<W: Write + Seek>(writer: W, array: impl Array + 'static) -> Result<W> {
    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), false)]);

    let mut writer = write::FileWriter::try_new(writer, &schema)?;

    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])?;

    writer.write(&batch);

    writer.into_inner()
}

fn read_ipc(reader: &[u8]) -> Result<RecordBatch> {
    let mut reader = Cursor::new(reader);
    let metadata = read::read_file_metadata(&mut reader)?;
    let mut reader = read::FileReader::new(&mut reader, metadata, None);
    reader.next().unwrap()
}
