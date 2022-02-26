use std::borrow::Borrow;
use std::io::{BufRead, Seek, SeekFrom};

use indexmap::map::IndexMap as HashMap;
use indexmap::set::IndexSet as HashSet;
use serde_json::Value;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use super::iterator::ValueIter;

type Tracker = HashMap<String, HashSet<DataType>>;

const ITEM_NAME: &str = "item";

/// Infers the fields of a JSON file by reading the first `number_of_rows` rows.
/// # Examples
/// ```
/// use std::io::Cursor;
/// use arrow2::io::json::read::infer;
///
/// let data = r#"{"a":1, "b":[2.0, 1.3, -6.1], "c":[false, true], "d":4.1}
/// {"a":-10, "b":[2.0, 1.3, -6.1], "c":null, "d":null}
/// {"a":2, "b":[2.0, null, -6.1], "c":[false, null], "d":"text"}
/// {"a":3, "b":4, "c": true, "d":[1, false, "array", 2.4]}
/// "#;
///
/// // file's cursor's offset at 0
/// let mut reader = Cursor::new(data);
/// let fields = infer(&mut reader, None).unwrap();
/// ```
pub fn infer<R: BufRead>(reader: &mut R, number_of_rows: Option<usize>) -> Result<Vec<Field>> {
    infer_iterator(ValueIter::new(reader, number_of_rows))
}

/// Infer [`Field`]s from an iterator of [`Value`].
pub fn infer_iterator<I, A>(value_iter: I) -> Result<Vec<Field>>
where
    I: Iterator<Item = Result<A>>,
    A: Borrow<Value>,
{
    let mut values: Tracker = Tracker::new();

    for record in value_iter {
        match record?.borrow() {
            Value::Object(map) => map.iter().try_for_each(|(k, v)| {
                let data_type = infer_value(v)?;
                add_or_insert(&mut values, k, data_type);
                Result::Ok(())
            }),
            value => Err(ArrowError::ExternalFormat(format!(
                "Expected JSON record to be an object, found {:?}",
                value
            ))),
        }?;
    }

    Ok(resolve_fields(values))
}

/// Infer the fields of a JSON file from `number_of_rows` in `reader`.
///
/// This function seeks back to the start of the `reader`.
///
/// # Examples
/// ```
/// use std::fs::File;
/// use std::io::Cursor;
/// use arrow2::io::json::read::infer_and_reset;
///
/// let data = r#"{"a":1, "b":[2.0, 1.3, -6.1], "c":[false, true], "d":4.1}
/// {"a":-10, "b":[2.0, 1.3, -6.1], "c":null, "d":null}
/// {"a":2, "b":[2.0, null, -6.1], "c":[false, null], "d":"text"}
/// {"a":3, "b":4, "c": true, "d":[1, false, "array", 2.4]}
/// "#;
/// let mut reader = Cursor::new(data);
/// let fields = infer_and_reset(&mut reader, None).unwrap();
/// // cursor's position automatically set at 0
/// ```
pub fn infer_and_reset<R: BufRead + Seek>(
    reader: &mut R,
    number_of_rows: Option<usize>,
) -> Result<Vec<Field>> {
    let fields = infer(reader, number_of_rows);
    reader.seek(SeekFrom::Start(0))?;
    fields
}

pub(crate) fn infer_value(value: &Value) -> Result<DataType> {
    Ok(match value {
        Value::Bool(_) => DataType::Boolean,
        Value::Array(array) => infer_array(array)?,
        Value::Null => DataType::Null,
        Value::Number(number) => infer_number(number),
        Value::String(_) => DataType::Utf8,
        Value::Object(inner) => {
            let fields = inner
                .iter()
                .map(|(key, value)| infer_value(value).map(|dt| Field::new(key, dt, true)))
                .collect::<Result<Vec<_>>>()?;
            DataType::Struct(fields)
        }
    })
}

/// Infers a [`DataType`] from a list of JSON values
pub fn infer_rows(rows: &[Value]) -> Result<DataType> {
    let types = rows.iter().map(|a| {
        Ok(match a {
            Value::Null => None,
            Value::Number(n) => Some(infer_number(n)),
            Value::Bool(_) => Some(DataType::Boolean),
            Value::String(_) => Some(DataType::Utf8),
            Value::Array(array) => Some(infer_array(array)?),
            Value::Object(inner) => {
                let fields = inner
                    .iter()
                    .map(|(key, value)| infer_value(value).map(|dt| Field::new(key, dt, true)))
                    .collect::<Result<Vec<_>>>()?;
                Some(DataType::Struct(fields))
            }
        })
    });
    // discard None values and deduplicate entries
    let types = types
        .into_iter()
        .filter_map(|x| x.transpose())
        .collect::<Result<HashSet<_>>>()?;

    Ok(if !types.is_empty() {
        let types = types.into_iter().collect::<Vec<_>>();
        coerce_data_type(&types)
    } else {
        DataType::Null
    })
}

fn infer_array(values: &[Value]) -> Result<DataType> {
    let dt = infer_rows(values)?;

    // if a record contains only nulls, it is not
    // added to values
    Ok(if dt == DataType::Null {
        dt
    } else {
        DataType::List(Box::new(Field::new(ITEM_NAME, dt, true)))
    })
}

fn infer_number(n: &serde_json::Number) -> DataType {
    if n.is_f64() {
        DataType::Float64
    } else {
        DataType::Int64
    }
}

fn add_or_insert(values: &mut Tracker, key: &str, data_type: DataType) {
    if data_type == DataType::Null {
        return;
    }
    if values.contains_key(key) {
        let x = values.get_mut(key).unwrap();
        x.insert(data_type);
    } else {
        // create hashset and add value type
        let mut hs = HashSet::new();
        hs.insert(data_type);
        values.insert(key.to_string(), hs);
    }
}

fn resolve_fields(spec: HashMap<String, HashSet<DataType>>) -> Vec<Field> {
    spec.iter()
        .map(|(k, hs)| {
            let v: Vec<&DataType> = hs.iter().collect();
            Field::new(k, coerce_data_type(&v), true)
        })
        .collect()
}

/// Coerce an heterogeneous set of [`DataType`] into a single one. Rules:
/// * `Int64` and `Float64` are `Float64`
/// * Lists and scalars are coerced to a list of a compatible scalar
/// * Structs contain the union of all fields
/// * All other types are coerced to `Utf8`
pub(crate) fn coerce_data_type<A: Borrow<DataType>>(datatypes: &[A]) -> DataType {
    use DataType::*;

    let are_all_equal = datatypes.windows(2).all(|w| w[0].borrow() == w[1].borrow());

    if are_all_equal {
        return datatypes[0].borrow().clone();
    }

    let are_all_structs = datatypes.iter().all(|x| matches!(x.borrow(), Struct(_)));

    if are_all_structs {
        // all are structs => union of all fields (that may have equal names)
        let fields = datatypes.iter().fold(vec![], |mut acc, dt| {
            if let Struct(new_fields) = dt.borrow() {
                acc.extend(new_fields);
            };
            acc
        });
        // group fields by unique
        let fields = fields.iter().fold(
            HashMap::<&String, Vec<&DataType>>::new(),
            |mut acc, field| {
                match acc.entry(&field.name) {
                    indexmap::map::Entry::Occupied(mut v) => {
                        v.get_mut().push(&field.data_type);
                    }
                    indexmap::map::Entry::Vacant(v) => {
                        v.insert(vec![&field.data_type]);
                    }
                }
                acc
            },
        );
        // and finally, coerce each of the fields within the same name
        let fields = fields
            .into_iter()
            .map(|(name, dts)| Field::new(name, coerce_data_type(&dts), true))
            .collect();
        return Struct(fields);
    } else if datatypes.len() > 2 {
        return Utf8;
    }
    let (lhs, rhs) = (datatypes[0].borrow(), datatypes[1].borrow());

    return match (lhs, rhs) {
        (lhs, rhs) if lhs == rhs => lhs.clone(),
        (List(lhs), List(rhs)) => {
            let inner = coerce_data_type(&[lhs.data_type(), rhs.data_type()]);
            List(Box::new(Field::new(ITEM_NAME, inner, true)))
        }
        (scalar, List(list)) => {
            let inner = coerce_data_type(&[scalar, list.data_type()]);
            List(Box::new(Field::new(ITEM_NAME, inner, true)))
        }
        (List(list), scalar) => {
            let inner = coerce_data_type(&[scalar, list.data_type()]);
            List(Box::new(Field::new(ITEM_NAME, inner, true)))
        }
        (Float64, Int64) => Float64,
        (Int64, Float64) => Float64,
        (Int64, Boolean) => Int64,
        (Boolean, Int64) => Int64,
        (_, _) => Utf8,
    };
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_coersion_scalar_and_list() {
        use crate::datatypes::DataType::*;

        assert_eq!(
            coerce_data_type(&[
                Float64,
                List(Box::new(Field::new(ITEM_NAME, Float64, true)))
            ]),
            List(Box::new(Field::new(ITEM_NAME, Float64, true))),
        );
        assert_eq!(
            coerce_data_type(&[Float64, List(Box::new(Field::new(ITEM_NAME, Int64, true)))]),
            List(Box::new(Field::new(ITEM_NAME, Float64, true))),
        );
        assert_eq!(
            coerce_data_type(&[Int64, List(Box::new(Field::new(ITEM_NAME, Int64, true)))]),
            List(Box::new(Field::new(ITEM_NAME, Int64, true))),
        );
        // boolean and number are incompatible, return utf8
        assert_eq!(
            coerce_data_type(&[
                Boolean,
                List(Box::new(Field::new(ITEM_NAME, Float64, true)))
            ]),
            List(Box::new(Field::new(ITEM_NAME, Utf8, true))),
        );
    }
}
