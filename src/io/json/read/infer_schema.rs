use std::borrow::Borrow;
use std::io::{BufRead, Seek, SeekFrom};

use indexmap::map::IndexMap as HashMap;
use indexmap::set::IndexSet as HashSet;
use serde_json::Value;

use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use super::iterator::ValueIter;

type Tracker = HashMap<String, HashSet<DataType>>;

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

fn infer_value(value: &Value) -> Result<DataType> {
    Ok(match value {
        Value::Bool(_) => DataType::Boolean,
        Value::Array(array) => infer_array(array)?,
        Value::Null => DataType::Null,
        Value::Number(number) => infer_number(number),
        Value::String(_) => DataType::Utf8,
        Value::Object(_) => {
            return Err(ArrowError::NotYetImplemented(
                "Inferring schema from nested JSON structs currently not supported".to_string(),
            ))
        }
    })
}

fn infer_array(array: &[Value]) -> Result<DataType> {
    let types = array.iter().map(|a| {
        Ok(match a {
            Value::Null => None,
            Value::Number(n) => Some(infer_number(n)),
            Value::Bool(_) => Some(DataType::Boolean),
            Value::String(_) => Some(DataType::Utf8),
            Value::Array(array) => Some(infer_array(array)?),
            Value::Object(_) => {
                return Err(ArrowError::NotYetImplemented(
                    "Nested structs not yet supported".to_string(),
                ))
            }
        })
    });
    // discard None values and deduplicate entries
    let types = types
        .into_iter()
        .map(|x| x.transpose())
        .flatten()
        .collect::<Result<HashSet<_>>>()?;

    // if a record contains only nulls, it is not
    // added to values
    Ok(if !types.is_empty() {
        let types = types.into_iter().collect::<Vec<_>>();
        let dt = coerce_data_type(&types);
        DataType::List(Box::new(Field::new("item", dt, true)))
    } else {
        DataType::Null
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
/// * All other types are coerced to `Utf8`
fn coerce_data_type<A: Borrow<DataType>>(dt: &[A]) -> DataType {
    use DataType::*;
    if dt.len() == 1 {
        return dt[0].borrow().clone();
    } else if dt.len() > 2 {
        return List(Box::new(Field::new("item", Utf8, true)));
    }
    let (lhs, rhs) = (dt[0].borrow(), dt[1].borrow());

    return match (lhs, rhs) {
        (lhs, rhs) if lhs == rhs => lhs.clone(),
        (List(lhs), List(rhs)) => {
            let inner = coerce_data_type(&[lhs.data_type(), rhs.data_type()]);
            List(Box::new(Field::new("item", inner, true)))
        }
        (scalar, List(list)) => {
            let inner = coerce_data_type(&[scalar, list.data_type()]);
            List(Box::new(Field::new("item", inner, true)))
        }
        (List(list), scalar) => {
            let inner = coerce_data_type(&[scalar, list.data_type()]);
            List(Box::new(Field::new("item", inner, true)))
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
            coerce_data_type(&[Float64, List(Box::new(Field::new("item", Float64, true)))]),
            List(Box::new(Field::new("item", Float64, true))),
        );
        assert_eq!(
            coerce_data_type(&[Float64, List(Box::new(Field::new("item", Int64, true)))]),
            List(Box::new(Field::new("item", Float64, true))),
        );
        assert_eq!(
            coerce_data_type(&[Int64, List(Box::new(Field::new("item", Int64, true)))]),
            List(Box::new(Field::new("item", Int64, true))),
        );
        // boolean and number are incompatible, return utf8
        assert_eq!(
            coerce_data_type(&[Boolean, List(Box::new(Field::new("item", Float64, true)))]),
            List(Box::new(Field::new("item", Utf8, true))),
        );
    }
}
