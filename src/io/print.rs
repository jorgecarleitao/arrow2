// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
