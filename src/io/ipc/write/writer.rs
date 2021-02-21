//! Arrow IPC File and Stream Writers
//!
//! The `FileWriter` and `StreamWriter` have similar interfaces,
//! however the `FileWriter` expects a reader that supports `Seek`ing

use std::io::{BufWriter, Write};

use super::super::ARROW_MAGIC;
use super::{
    super::{convert, gen},
    common::{
        write_continuation, write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
    },
};
use flatbuffers::FlatBufferBuilder;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::record_batch::RecordBatch;

pub struct FileWriter<W: Write> {
    /// The object to write to
    writer: BufWriter<W>,
    /// IPC write options
    write_options: IpcWriteOptions,
    /// A reference to the schema, used in validating record batches
    schema: Schema,
    /// The number of bytes between each block of bytes, as an offset for random access
    block_offsets: usize,
    /// Dictionary blocks that will be written as part of the IPC footer
    dictionary_blocks: Vec<gen::File::Block>,
    /// Record blocks that will be written as part of the IPC footer
    record_blocks: Vec<gen::File::Block>,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,

    data_gen: IpcDataGenerator,
}

impl<W: Write> FileWriter<W> {
    /// Try create a new writer, with the schema written as part of the header
    pub fn try_new(writer: W, schema: &Schema) -> Result<Self> {
        let write_options = IpcWriteOptions::default();
        Self::try_new_with_options(writer, schema, write_options)
    }

    /// Try create a new writer with IpcWriteOptions
    pub fn try_new_with_options(
        writer: W,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self> {
        let data_gen = IpcDataGenerator::default();
        let mut writer = BufWriter::new(writer);
        // write magic to header
        writer.write_all(&ARROW_MAGIC[..])?;
        // create an 8-byte boundary after the header
        writer.write_all(&[0, 0])?;
        // write the schema, set the written bytes to the schema + header
        let encoded_message = data_gen.schema_to_bytes(schema, &write_options);
        let (meta, data) = write_message(&mut writer, encoded_message, &write_options)?;
        Ok(Self {
            writer,
            write_options,
            schema: schema.clone(),
            block_offsets: meta + data + 8,
            dictionary_blocks: vec![],
            record_blocks: vec![],
            finished: false,
            dictionary_tracker: DictionaryTracker::new(true),
            data_gen,
        })
    }

    /// Write a record batch to the file
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.finished {
            return Err(ArrowError::IPC(
                "Cannot write record batch to file writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_message) = self.data_gen.encoded_batch(
            batch,
            &mut self.dictionary_tracker,
            &self.write_options,
        )?;

        for encoded_dictionary in encoded_dictionaries {
            let (meta, data) =
                write_message(&mut self.writer, encoded_dictionary, &self.write_options)?;

            let block = gen::File::Block::new(self.block_offsets as i64, meta as i32, data as i64);
            self.dictionary_blocks.push(block);
            self.block_offsets += meta + data;
        }

        let (meta, data) = write_message(&mut self.writer, encoded_message, &self.write_options)?;
        // add a record block for the footer
        let block = gen::File::Block::new(
            self.block_offsets as i64,
            meta as i32, // TODO: is this still applicable?
            data as i64,
        );
        self.record_blocks.push(block);
        self.block_offsets += meta + data;
        Ok(())
    }

    /// Write footer and closing tag, then mark the writer as done
    pub fn finish(&mut self) -> Result<()> {
        // write EOS
        write_continuation(&mut self.writer, &self.write_options, 0)?;

        let mut fbb = FlatBufferBuilder::new();
        let dictionaries = fbb.create_vector(&self.dictionary_blocks);
        let record_batches = fbb.create_vector(&self.record_blocks);
        let schema = convert::schema_to_fb_offset(&mut fbb, &self.schema);

        let root = {
            let mut footer_builder = gen::File::FooterBuilder::new(&mut fbb);
            footer_builder.add_version(self.write_options.metadata_version);
            footer_builder.add_schema(schema);
            footer_builder.add_dictionaries(dictionaries);
            footer_builder.add_recordBatches(record_batches);
            footer_builder.finish()
        };
        fbb.finish(root, None);
        let footer_data = fbb.finished_data();
        self.writer.write_all(footer_data)?;
        self.writer
            .write_all(&(footer_data.len() as i32).to_le_bytes())?;
        self.writer.write_all(&ARROW_MAGIC)?;
        self.writer.flush()?;
        self.finished = true;

        Ok(())
    }
}

/// Finish the file if it is not 'finished' when it goes out of scope
impl<W: Write> Drop for FileWriter<W> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish().unwrap();
        }
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::Read;
    use std::sync::Arc;

    use flate2::read::GzDecoder;
    use gen::Schema::MetadataVersion;

    use crate::array::*;
    use crate::datatypes::Field;
    use crate::gen::Schema::reader::*;
    use crate::util::integration_util::*;

    #[test]
    fn test_write_file() {
        let schema = Schema::new(vec![Field::new("field1", DataType::UInt32, false)]);
        let values: Vec<Option<u32>> = vec![
            Some(999),
            None,
            Some(235),
            Some(123),
            None,
            None,
            None,
            None,
            None,
        ];
        let array1 = UInt32Array::from(values);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(array1) as ArrayRef],
        )
        .unwrap();
        {
            let file = File::create("target/debug/testdata/arrow.arrow_file").unwrap();
            let mut writer = FileWriter::try_new(file, &schema).unwrap();

            writer.write(&batch).unwrap();
            // this is inside a block to test the implicit finishing of the file on `Drop`
        }

        {
            let file =
                File::open(format!("target/debug/testdata/{}.arrow_file", "arrow"))
                    .unwrap();
            let mut reader = FileReader::try_new(file).unwrap();
            while let Some(Ok(read_batch)) = reader.next() {
                read_batch
                    .columns()
                    .iter()
                    .zip(batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
            }
        }
    }

    fn write_null_file(options: IpcWriteOptions, suffix: &str) {
        let schema = Schema::new(vec![
            Field::new("nulls", DataType::Null, true),
            Field::new("int32s", DataType::Int32, false),
            Field::new("nulls2", DataType::Null, false),
            Field::new("f64s", DataType::Float64, false),
        ]);
        let array1 = NullArray::new(32);
        let array2 = Int32Array::from(vec![1; 32]);
        let array3 = NullArray::new(32);
        let array4 = Float64Array::from(vec![std::f64::NAN; 32]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(array1) as ArrayRef,
                Arc::new(array2) as ArrayRef,
                Arc::new(array3) as ArrayRef,
                Arc::new(array4) as ArrayRef,
            ],
        )
        .unwrap();
        let file_name = format!("target/debug/testdata/nulls_{}.arrow_file", suffix);
        {
            let file = File::create(&file_name).unwrap();
            let mut writer =
                FileWriter::try_new_with_options(file, &schema, options).unwrap();

            writer.write(&batch).unwrap();
            // this is inside a block to test the implicit finishing of the file on `Drop`
        }

        {
            let file = File::open(&file_name).unwrap();
            let reader = FileReader::try_new(file).unwrap();
            reader.for_each(|maybe_batch| {
                maybe_batch
                    .unwrap()
                    .columns()
                    .iter()
                    .zip(batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
            });
        }
    }
    #[test]
    fn test_write_null_file_v4() {
        write_null_file(
            IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap(),
            "v4_a8",
        );
        write_null_file(
            IpcWriteOptions::try_new(8, true, MetadataVersion::V4).unwrap(),
            "v4_a8l",
        );
        write_null_file(
            IpcWriteOptions::try_new(64, false, MetadataVersion::V4).unwrap(),
            "v4_a64",
        );
        write_null_file(
            IpcWriteOptions::try_new(64, true, MetadataVersion::V4).unwrap(),
            "v4_a64l",
        );
    }

    #[test]
    fn test_write_null_file_v5() {
        write_null_file(
            IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap(),
            "v5_a8",
        );
        write_null_file(
            IpcWriteOptions::try_new(64, false, MetadataVersion::V5).unwrap(),
            "v5_a64",
        );
    }

    #[test]
    fn read_and_rewrite_generated_files_014() {
        let testdata = crate::util::test_util::arrow_test_data();
        let version = "0.14.1";
        // the test is repetitive, thus we can read all supported files at once
        let paths = vec![
            "generated_interval",
            "generated_datetime",
            "generated_dictionary",
            "generated_nested",
            "generated_primitive_no_batches",
            "generated_primitive_zerolength",
            "generated_primitive",
            "generated_decimal",
        ];
        paths.iter().for_each(|path| {
            let file = File::open(format!(
                "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
                testdata, version, path
            ))
            .unwrap();

            let mut reader = FileReader::try_new(file).unwrap();

            // read and rewrite the file to a temp location
            {
                let file = File::create(format!(
                    "target/debug/testdata/{}-{}.arrow_file",
                    version, path
                ))
                .unwrap();
                let mut writer = FileWriter::try_new(file, &reader.schema()).unwrap();
                while let Some(Ok(batch)) = reader.next() {
                    writer.write(&batch).unwrap();
                }
                writer.finish().unwrap();
            }

            let file = File::open(format!(
                "target/debug/testdata/{}-{}.arrow_file",
                version, path
            ))
            .unwrap();
            let mut reader = FileReader::try_new(file).unwrap();

            // read expected JSON output
            let arrow_json = read_gzip_json(version, path);
            assert!(arrow_json.equals_reader(&mut reader));
        });
    }

    #[test]
    fn read_and_rewrite_generated_streams_014() {
        let testdata = crate::util::test_util::arrow_test_data();
        let version = "0.14.1";
        // the test is repetitive, thus we can read all supported files at once
        let paths = vec![
            "generated_interval",
            "generated_datetime",
            "generated_dictionary",
            "generated_nested",
            "generated_primitive_no_batches",
            "generated_primitive_zerolength",
            "generated_primitive",
            "generated_decimal",
        ];
        paths.iter().for_each(|path| {
            let file = File::open(format!(
                "{}/arrow-ipc-stream/integration/{}/{}.stream",
                testdata, version, path
            ))
            .unwrap();

            let reader = StreamReader::try_new(file).unwrap();

            // read and rewrite the stream to a temp location
            {
                let file = File::create(format!(
                    "target/debug/testdata/{}-{}.stream",
                    version, path
                ))
                .unwrap();
                let mut writer = StreamWriter::try_new(file, &reader.schema()).unwrap();
                reader.for_each(|batch| {
                    writer.write(&batch.unwrap()).unwrap();
                });
                writer.finish().unwrap();
            }

            let file =
                File::open(format!("target/debug/testdata/{}-{}.stream", version, path))
                    .unwrap();
            let mut reader = StreamReader::try_new(file).unwrap();

            // read expected JSON output
            let arrow_json = read_gzip_json(version, path);
            assert!(arrow_json.equals_reader(&mut reader));
        });
    }

    #[test]
    fn read_and_rewrite_generated_files_100() {
        let testdata = crate::util::test_util::arrow_test_data();
        let version = "1.0.0-littleendian";
        // the test is repetitive, thus we can read all supported files at once
        let paths = vec![
            "generated_custom_metadata",
            "generated_datetime",
            "generated_dictionary_unsigned",
            "generated_dictionary",
            // "generated_duplicate_fieldnames",
            "generated_interval",
            "generated_large_batch",
            "generated_nested",
            // "generated_nested_large_offsets",
            "generated_null_trivial",
            "generated_null",
            "generated_primitive_large_offsets",
            "generated_primitive_no_batches",
            "generated_primitive_zerolength",
            "generated_primitive",
            // "generated_recursive_nested",
        ];
        paths.iter().for_each(|path| {
            let file = File::open(format!(
                "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
                testdata, version, path
            ))
            .unwrap();

            let mut reader = FileReader::try_new(file).unwrap();

            // read and rewrite the file to a temp location
            {
                let file = File::create(format!(
                    "target/debug/testdata/{}-{}.arrow_file",
                    version, path
                ))
                .unwrap();
                // write IPC version 5
                let options =
                    IpcWriteOptions::try_new(8, false, gen::Schema::MetadataVersion::V5).unwrap();
                let mut writer =
                    FileWriter::try_new_with_options(file, &reader.schema(), options)
                        .unwrap();
                while let Some(Ok(batch)) = reader.next() {
                    writer.write(&batch).unwrap();
                }
                writer.finish().unwrap();
            }

            let file = File::open(format!(
                "target/debug/testdata/{}-{}.arrow_file",
                version, path
            ))
            .unwrap();
            let mut reader = FileReader::try_new(file).unwrap();

            // read expected JSON output
            let arrow_json = read_gzip_json(version, path);
            assert!(arrow_json.equals_reader(&mut reader));
        });
    }

    #[test]
    fn read_and_rewrite_generated_streams_100() {
        let testdata = crate::util::test_util::arrow_test_data();
        let version = "1.0.0-littleendian";
        // the test is repetitive, thus we can read all supported files at once
        let paths = vec![
            "generated_custom_metadata",
            "generated_datetime",
            "generated_dictionary_unsigned",
            "generated_dictionary",
            // "generated_duplicate_fieldnames",
            "generated_interval",
            "generated_large_batch",
            "generated_nested",
            // "generated_nested_large_offsets",
            "generated_null_trivial",
            "generated_null",
            "generated_primitive_large_offsets",
            "generated_primitive_no_batches",
            "generated_primitive_zerolength",
            "generated_primitive",
            // "generated_recursive_nested",
        ];
        paths.iter().for_each(|path| {
            let file = File::open(format!(
                "{}/arrow-ipc-stream/integration/{}/{}.stream",
                testdata, version, path
            ))
            .unwrap();

            let reader = StreamReader::try_new(file).unwrap();

            // read and rewrite the stream to a temp location
            {
                let file = File::create(format!(
                    "target/debug/testdata/{}-{}.stream",
                    version, path
                ))
                .unwrap();
                let options =
                    IpcWriteOptions::try_new(8, false, gen::Schema::MetadataVersion::V5).unwrap();
                let mut writer =
                    StreamWriter::try_new_with_options(file, &reader.schema(), options)
                        .unwrap();
                reader.for_each(|batch| {
                    writer.write(&batch.unwrap()).unwrap();
                });
                writer.finish().unwrap();
            }

            let file =
                File::open(format!("target/debug/testdata/{}-{}.stream", version, path))
                    .unwrap();
            let mut reader = StreamReader::try_new(file).unwrap();

            // read expected JSON output
            let arrow_json = read_gzip_json(version, path);
            assert!(arrow_json.equals_reader(&mut reader));
        });
    }

    /// Read gzipped JSON file
    fn read_gzip_json(version: &str, path: &str) -> ArrowJson {
        let testdata = crate::util::test_util::arrow_test_data();
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.json.gz",
            testdata, version, path
        ))
        .unwrap();
        let mut gz = GzDecoder::new(&file);
        let mut s = String::new();
        gz.read_to_string(&mut s).unwrap();
        // convert to Arrow JSON
        let arrow_json: ArrowJson = serde_json::from_str(&s).unwrap();
        arrow_json
    }
}
*/
