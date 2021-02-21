use std::io::{BufReader, Read};
use std::sync::Arc;

use crate::array::*;
use crate::datatypes::Schema;
use crate::error::{ArrowError, Result};
use crate::record_batch::{RecordBatch, RecordBatchReader};

use super::super::CONTINUATION_MARKER;
use super::super::{convert, gen};
use super::common::*;

type SchemaRef = Arc<Schema>;
type ArrayRef = Arc<dyn Array>;

/// Arrow Stream reader
pub struct StreamReader<R: Read> {
    /// Buffered stream reader
    reader: BufReader<R>,

    /// The schema that is read from the stream's first message
    schema: SchemaRef,

    /// Optional dictionaries for each schema field.
    ///
    /// Dictionaries may be appended to in the streaming format.
    dictionaries_by_field: Vec<Option<ArrayRef>>,

    /// An indicator of whether the stream is complete.
    ///
    /// This value is set to `true` the first time the reader's `next()` returns `None`.
    finished: bool,
}

impl<R: Read> StreamReader<R> {
    /// Try to create a new stream reader
    ///
    /// The first message in the stream is the schema, the reader will fail if it does not
    /// encounter a schema.
    /// To check if the reader is done, use `is_finished(self)`
    pub fn try_new(reader: R) -> Result<Self> {
        let mut reader = BufReader::new(reader);
        // determine metadata length
        let mut meta_size: [u8; 4] = [0; 4];
        reader.read_exact(&mut meta_size)?;
        let meta_len = {
            // If a continuation marker is encountered, skip over it and read
            // the size from the next four bytes.
            if meta_size == CONTINUATION_MARKER {
                reader.read_exact(&mut meta_size)?;
            }
            i32::from_le_bytes(meta_size)
        };

        let mut meta_buffer = vec![0; meta_len as usize];
        reader.read_exact(&mut meta_buffer)?;

        let message = gen::Message::root_as_message(meta_buffer.as_slice())
            .map_err(|err| ArrowError::IPC(format!("Unable to get root as message: {:?}", err)))?;
        // message header is a Schema, so read it
        let ipc_schema: gen::Schema::Schema = message
            .header_as_schema()
            .ok_or_else(|| ArrowError::IPC("Unable to read IPC message as schema".to_string()))?;
        let schema = convert::fb_to_schema(ipc_schema);

        // Create an array of optional dictionary value arrays, one per field.
        // todo: this is wrong for nested types, as there must be one dictionary per node, not per field
        let dictionaries_by_field = vec![None; schema.fields().len()];

        Ok(Self {
            reader,
            schema: Arc::new(schema),
            finished: false,
            dictionaries_by_field,
        })
    }

    /// Return the schema of the stream
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Check if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    fn maybe_next(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        // determine metadata length
        let mut meta_size: [u8; 4] = [0; 4];

        match self.reader.read_exact(&mut meta_size) {
            Ok(()) => (),
            Err(e) => {
                return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    // Handle EOF without the "0xFFFFFFFF 0x00000000"
                    // valid according to:
                    // https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
                    self.finished = true;
                    Ok(None)
                } else {
                    Err(ArrowError::from(e))
                };
            }
        }

        let meta_len = {
            // If a continuation marker is encountered, skip over it and read
            // the size from the next four bytes.
            if meta_size == CONTINUATION_MARKER {
                self.reader.read_exact(&mut meta_size)?;
            }
            i32::from_le_bytes(meta_size)
        };

        if meta_len == 0 {
            // the stream has ended, mark the reader as finished
            self.finished = true;
            return Ok(None);
        }

        let mut meta_buffer = vec![0; meta_len as usize];
        self.reader.read_exact(&mut meta_buffer)?;

        let vecs = &meta_buffer.to_vec();
        let message = gen::Message::root_as_message(vecs)
            .map_err(|err| ArrowError::IPC(format!("Unable to get root as message: {:?}", err)))?;

        match message.header_type() {
            gen::Message::MessageHeader::Schema => Err(ArrowError::IPC(
                "Not expecting a schema when messages are read".to_string(),
            )),
            gen::Message::MessageHeader::RecordBatch => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::IPC("Unable to read IPC message as record batch".to_string())
                })?;
                // read the block that makes up the record batch into a buffer
                let mut buf = vec![0; message.bodyLength() as usize];
                self.reader.read_exact(&mut buf)?;

                let mut reader = std::io::Cursor::new(buf);

                read_record_batch(
                    batch,
                    self.schema(),
                    &self.dictionaries_by_field,
                    &mut reader,
                    0,
                )
                .map(Some)
            }
            gen::Message::MessageHeader::DictionaryBatch => {
                let batch = message.header_as_dictionary_batch().ok_or_else(|| {
                    ArrowError::IPC("Unable to read IPC message as dictionary batch".to_string())
                })?;
                // read the block that makes up the dictionary batch into a buffer
                let mut buf = vec![0; message.bodyLength() as usize];
                self.reader.read_exact(&mut buf)?;

                let mut reader = std::io::Cursor::new(buf);

                read_dictionary(
                    batch,
                    &self.schema,
                    &mut self.dictionaries_by_field,
                    &mut reader,
                    0,
                )?;

                // read the next message until we encounter a RecordBatch
                self.maybe_next()
            }
            gen::Message::MessageHeader::NONE => Ok(None),
            t => Err(ArrowError::IPC(format!(
                "Reading types other than record batches not yet supported, unable to read {:?} ",
                t
            ))),
        }
    }
}

impl<R: Read> Iterator for StreamReader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.maybe_next().transpose()
    }
}

impl<R: Read> RecordBatchReader for StreamReader<R> {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::tests::read_gzip_json;

    use std::fs::File;

    fn test_file(version: &str, file_name: &str) {
        let testdata = crate::util::test_util::arrow_test_data();
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.stream",
            testdata, version, file_name
        ))
        .unwrap();

        let reader = StreamReader::try_new(file).unwrap();

        // read expected JSON output
        let (schema, batches) = read_gzip_json(version, file_name);

        assert_eq!(&schema, reader.schema().as_ref());

        batches
            .iter()
            .zip(reader.map(|x| x.unwrap()))
            .for_each(|(lhs, rhs)| {
                assert_eq!(lhs, &rhs);
            });
    }

    #[test]
    fn read_generated_100_primitive() {
        test_file("1.0.0-littleendian", "generated_primitive");
    }

    #[test]
    fn read_generated_100_datetime() {
        test_file("1.0.0-littleendian", "generated_datetime");
    }

    #[test]
    fn read_generated_100_null_trivial() {
        test_file("1.0.0-littleendian", "generated_null_trivial");
    }

    #[test]
    fn read_generated_100_null() {
        test_file("1.0.0-littleendian", "generated_null");
    }

    #[test]
    fn read_generated_100_primitive_zerolength() {
        test_file("1.0.0-littleendian", "generated_primitive_zerolength");
    }

    #[test]
    fn read_generated_100_primitive_primitive_no_batches() {
        test_file("1.0.0-littleendian", "generated_primitive_no_batches");
    }

    #[test]
    fn read_generated_100_dictionary() {
        test_file("1.0.0-littleendian", "generated_dictionary");
    }

    #[test]
    fn read_generated_100_nested() {
        test_file("1.0.0-littleendian", "generated_nested");
    }

    #[test]
    fn read_generated_100_interval() {
        test_file("1.0.0-littleendian", "generated_interval");
    }
}
