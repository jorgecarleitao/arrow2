//! APIs to represent [`RecordBatch`] as a formatted table.

use crate::{array::get_display, record_batch::RecordBatch};

use comfy_table::{Cell, Table};

/// Returns a visual representation of multiple [`RecordBatch`]es.
pub fn write(batches: &[RecordBatch]) -> String {
    create_table(batches).to_string()
}

/// Prints a visual representation of record batches to stdout
pub fn print(results: &[RecordBatch]) {
    println!("{}", create_table(results))
}

/// Convert a series of record batches into a table
fn create_table(results: &[RecordBatch]) -> Table {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if results.is_empty() {
        return table;
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_header(header);

    for batch in results {
        let displayes = batch
            .columns()
            .iter()
            .map(|array| get_display(array.as_ref()))
            .collect::<Vec<_>>();

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            (0..batch.num_columns()).for_each(|col| {
                let string = displayes[col](row);
                cells.push(Cell::new(&string));
            });
            table.add_row(cells);
        }
    }
    table
}
