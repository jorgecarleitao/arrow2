use crate::record_batch::RecordBatch;
use crate::{array::PrimitiveArray, datatypes::DataType};

use super::{ArrowJsonBatch, ArrowJsonColumn};

pub fn from_record_batch(batch: &RecordBatch) -> ArrowJsonBatch {
    let mut json_batch = ArrowJsonBatch {
        count: batch.num_rows(),
        columns: Vec::with_capacity(batch.num_columns()),
    };

    for (col, field) in batch.columns().iter().zip(batch.schema().fields.iter()) {
        let json_col = match field.data_type() {
            DataType::Int8 => {
                let array = col.as_any().downcast_ref::<PrimitiveArray<i8>>().unwrap();

                let (validity, data) = array
                    .iter()
                    .map(|x| (x.is_some() as u8, x.copied().unwrap_or_default().into()))
                    .unzip();

                ArrowJsonColumn {
                    name: field.name().clone(),
                    count: col.len(),
                    validity: Some(validity),
                    data: Some(data),
                    offset: None,
                    children: None,
                }
            }
            _ => ArrowJsonColumn {
                name: field.name().clone(),
                count: col.len(),
                validity: None,
                data: None,
                offset: None,
                children: None,
            },
        };

        json_batch.columns.push(json_col);
    }

    json_batch
}
