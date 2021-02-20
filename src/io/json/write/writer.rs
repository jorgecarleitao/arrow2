use std::io::{BufWriter, Write};

use serde_json::Value;

use crate::error::Result;
use crate::record_batch::RecordBatch;

use super::serialize::write_record_batches;

/// A JSON writer
#[derive(Debug)]
pub struct Writer<W: Write> {
    writer: BufWriter<W>,
}

impl<W: Write> Writer<W> {
    pub fn new(writer: W) -> Self {
        Self::from_buf_writer(BufWriter::new(writer))
    }

    pub fn from_buf_writer(writer: BufWriter<W>) -> Self {
        Self { writer }
    }

    pub fn write_row(&mut self, row: &Value) -> Result<()> {
        self.writer.write_all(&serde_json::to_vec(row)?)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    pub fn write_batches(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for row in write_record_batches(batches) {
            self.write_row(&Value::Object(row))?;
        }
        Ok(())
    }
}
