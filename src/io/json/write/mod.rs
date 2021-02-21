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

//! JSON Writer
//!
//! This JSON writer allows converting Arrow record batches into array of JSON objects. It also
//! provides a Writer struct to help serialize record batches directly into line-delimited JSON
//! objects as bytes.
//!
//! Serialize record batches into array of JSON objects:
//!
//! ```
//!
//! use arrow::array::Int32Array;
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::json;
//! use arrow::record_batch::RecordBatch;
//!
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from(vec![1, 2, 3]);
//! let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
//!
//! let json_rows = json::writer::record_batches_to_json_rows(&[batch]);
//! assert_eq!(
//!     serde_json::Value::Object(json_rows[1].clone()),
//!     serde_json::json!({"a": 2}),
//! );
//! ```
//!
//! Serialize record batches into line-delimited JSON bytes:
//!
//! ```
//!
//! use arrow::array::Int32Array;
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::json;
//! use arrow::record_batch::RecordBatch;
//!
//! let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
//! let a = Int32Array::from(vec![1, 2, 3]);
//! let batch = RecordBatch::try_new(schema, vec![Arc::new(a)]).unwrap();
//!
//! let buf = Vec::new();
//! let mut writer = json::Writer::new(buf);
//! writer.write_batches(&vec![batch]).unwrap();
//! ```

mod serialize;
mod writer;
pub use writer::Writer;

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;
    use std::sync::Arc;

    use crate::array::*;
    use crate::buffer::Buffer;
    use crate::datatypes::{DataType, Field, Schema};
    use crate::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn write_simple_rows() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Utf8, false),
        ]);

        let a =
            Primitive::<i32>::from([Some(1), Some(2), Some(3), None, Some(5)]).to(DataType::Int32);
        let b = Utf8Array::<i32>::from(&vec![Some("a"), Some("b"), Some("c"), Some("d"), None]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(&mut buf);
            writer.write_batches(&[batch]).unwrap();
        }

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            r#"{"c1":1,"c2":"a"}
{"c1":2,"c2":"b"}
{"c1":3,"c2":"c"}
{"c1":null,"c2":"d"}
{"c1":5,"c2":null}
"#
        );
    }

    #[test]
    fn write_nested_structs() {
        let c121 = Field::new("c121", DataType::Utf8, false);
        let fields = vec![
            Field::new("c11", DataType::Int32, false),
            Field::new("c12", DataType::Struct(vec![c121.clone()]), false),
        ];
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Struct(fields.clone()), false),
            Field::new("c2", DataType::Utf8, false),
        ]);

        let c1 = StructArray::from_data(
            fields,
            vec![
                Arc::new(Primitive::<i32>::from(&[Some(1), None, Some(5)]).to(DataType::Int32)),
                Arc::new(StructArray::from_data(
                    vec![c121],
                    vec![Arc::new(Utf8Array::<i32>::from(&vec![
                        Some("e"),
                        Some("f"),
                        Some("g"),
                    ]))],
                    None,
                )),
            ],
            None,
        );

        let c2 = Utf8Array::<i32>::from(&vec![Some("a"), Some("b"), Some("c")]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(c1), Arc::new(c2)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(&mut buf);
            writer.write_batches(&[batch]).unwrap();
        }

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            r#"{"c1":{"c11":1,"c12":{"c121":"e"}},"c2":"a"}
{"c1":{"c11":null,"c12":{"c121":"f"}},"c2":"b"}
{"c1":{"c11":5,"c12":{"c121":"g"}},"c2":"c"}
"#
        );
    }

    #[test]
    fn write_struct_with_list_field() {
        let list_datatype = DataType::List(Box::new(Field::new("c_list", DataType::Utf8, false)));
        let field_c1 = Field::new("c1", list_datatype.clone(), false);
        let field_c2 = Field::new("c2", DataType::Int32, false);
        let schema = Schema::new(vec![field_c1.clone(), field_c2]);

        let iter = vec![vec!["a", "a1"], vec!["b"], vec!["c"], vec!["d"], vec!["e"]];

        let iter = iter
            .into_iter()
            .map(|x| x.into_iter().map(Some).collect::<Vec<_>>())
            .map(Some)
            .map(Result::Ok);
        let a = ListPrimitive::<i32, Utf8Primitive<i32>, _>::try_from_iter(iter)
            .unwrap()
            .to(list_datatype);

        let b = Primitive::from_slice(&vec![1, 2, 3, 4, 5]).to(DataType::Int32);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(&mut buf);
            writer.write_batches(&[batch]).unwrap();
        }

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            r#"{"c1":["a","a1"],"c2":1}
{"c1":["b"],"c2":2}
{"c1":["c"],"c2":3}
{"c1":["d"],"c2":4}
{"c1":["e"],"c2":5}
"#
        );
    }

    #[test]
    fn write_nested_list() {
        let list_inner = DataType::List(Box::new(Field::new("b", DataType::Int32, false)));
        let list_inner_type = Field::new("a", list_inner.clone(), false);
        let list_datatype = DataType::List(Box::new(list_inner_type.clone()));
        let field_c1 = Field::new("c1", list_datatype.clone(), false);
        let field_c2 = Field::new("c2", DataType::Utf8, false);
        let schema = Schema::new(vec![field_c1.clone(), field_c2]);

        let iter = vec![
            vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3)])],
            vec![],
            vec![Some(vec![Some(4), Some(5), Some(6)])],
        ];

        let iter = iter.into_iter().map(Some);
        let c1 = ListPrimitive::<i32, ListPrimitive<i32, Primitive<i32>, _>, _>::from_iter(iter)
            .to(list_datatype);

        let c2 = Utf8Array::<i32>::from(&vec![Some("foo"), Some("bar"), None]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(c1), Arc::new(c2)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(&mut buf);
            writer.write_batches(&[batch]).unwrap();
        }

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            r#"{"c1":[[1,2],[3]],"c2":"foo"}
{"c1":[],"c2":"bar"}
{"c1":[[4,5,6]],"c2":null}
"#
        );
    }

    #[test]
    fn write_list_of_struct() {
        let inner = vec![Field::new("c121", DataType::Utf8, false)];
        let fields = vec![
            Field::new("c11", DataType::Int32, false),
            Field::new("c12", DataType::Struct(inner.clone()), false),
        ];
        let c1_datatype = DataType::List(Box::new(Field::new(
            "s",
            DataType::Struct(fields.clone()),
            false,
        )));
        let field_c1 = Field::new("c1", c1_datatype.clone(), true);
        let field_c2 = Field::new("c2", DataType::Int32, false);
        let schema = Schema::new(vec![field_c1.clone(), field_c2]);

        let s = StructArray::from_data(
            fields,
            vec![
                Arc::new(Primitive::<i32>::from(vec![Some(1), None, Some(5)]).to(DataType::Int32)),
                Arc::new(StructArray::from_data(
                    inner,
                    vec![Arc::new(Utf8Array::<i32>::from(&vec![
                        Some("e"),
                        Some("f"),
                        Some("g"),
                    ]))],
                    None,
                )),
            ],
            None,
        );

        // list column rows (c1):
        // [{"c11": 1, "c12": {"c121": "e"}}, {"c12": {"c121": "f"}}],
        // null,
        // [{"c11": 5, "c12": {"c121": "g"}}]
        let c1 = ListArray::<i32>::from_data(
            c1_datatype,
            Buffer::from(&[0, 2, 2, 3]),
            Arc::new(s),
            Some(([0b00000101], 3).into()),
        );

        let c2 = Primitive::<i32>::from_slice(&[1, 2, 3]).to(DataType::Int32);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(c1), Arc::new(c2)]).unwrap();

        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(&mut buf);
            writer.write_batches(&[batch]).unwrap();
        }

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            r#"{"c1":[{"c11":1,"c12":{"c121":"e"}},{"c11":null,"c12":{"c121":"f"}}],"c2":1}
{"c1":null,"c2":2}
{"c1":[{"c11":5,"c12":{"c121":"g"}}],"c2":3}
"#
        );
    }
}
