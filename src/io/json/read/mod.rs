//! APIs to read and deserialize from JSON
mod deserialize;
mod infer_schema;
mod iterator;

use crate::error::{ArrowError, Result};

pub use deserialize::deserialize;
pub use infer_schema::*;

/// Reads rows from `reader` into `rows`. Returns the number of read items.
/// IO-bounded.
pub fn read_rows<R: std::io::BufRead>(reader: &mut R, rows: &mut [String]) -> Result<usize> {
    let mut row_number = 0;
    for row in rows.iter_mut() {
        loop {
            row.truncate(0);
            let _ = reader.read_line(row).map_err(|e| {
                ArrowError::External(format!(" at line {}", row_number), Box::new(e))
            })?;
            if row.is_empty() {
                break;
            }
            if !row.trim().is_empty() {
                break;
            }
        }
        if row.is_empty() {
            break;
        }
        row_number += 1;
    }
    Ok(row_number)
}
