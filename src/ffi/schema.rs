use std::{ffi::CStr, ffi::CString, ptr};

use crate::{
    datatypes::{DataType, Field, IntervalUnit, TimeUnit},
    error::{ArrowError, Result},
};

#[allow(dead_code)]
struct SchemaPrivateData {
    field: Field,
    children_ptr: Box<[*mut Ffi_ArrowSchema]>,
    dictionary: Option<*mut Ffi_ArrowSchema>,
}

/// ABI-compatible struct for `ArrowSchema` from C Data Interface
/// See <https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions>
/// This was created by bindgen
#[repr(C)]
#[derive(Debug)]
pub struct Ffi_ArrowSchema {
    format: *const ::std::os::raw::c_char,
    name: *const ::std::os::raw::c_char,
    metadata: *const ::std::os::raw::c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut Ffi_ArrowSchema,
    dictionary: *mut Ffi_ArrowSchema,
    release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut Ffi_ArrowSchema)>,
    private_data: *mut ::std::os::raw::c_void,
}

// callback used to drop [Ffi_ArrowSchema] when it is exported.
unsafe extern "C" fn c_release_schema(schema: *mut Ffi_ArrowSchema) {
    if schema.is_null() {
        return;
    }
    let schema = &mut *schema;

    // take ownership back to release it.
    CString::from_raw(schema.format as *mut std::os::raw::c_char);
    CString::from_raw(schema.name as *mut std::os::raw::c_char);
    let private = Box::from_raw(schema.private_data as *mut SchemaPrivateData);
    for child in private.children_ptr.iter() {
        let _ = Box::from_raw(*child);
    }

    if let Some(ptr) = private.dictionary {
        let _ = Box::from_raw(ptr);
    }

    schema.release = None;
}

impl Ffi_ArrowSchema {
    /// create a new [`Ffi_ArrowSchema`]. This fails if the fields' [`DataType`] is not supported.
    pub fn try_new(field: Field) -> Result<Ffi_ArrowSchema> {
        let format = to_format(field.data_type())?;
        let name = field.name().clone();

        // allocate (and hold) the children
        let children_vec = match field.data_type() {
            DataType::List(field) => {
                vec![Box::new(Ffi_ArrowSchema::try_new(field.as_ref().clone())?)]
            }
            DataType::LargeList(field) => {
                vec![Box::new(Ffi_ArrowSchema::try_new(field.as_ref().clone())?)]
            }
            DataType::Struct(fields) => fields
                .iter()
                .map(|field| Ok(Box::new(Ffi_ArrowSchema::try_new(field.clone())?)))
                .collect::<Result<Vec<_>>>()?,
            _ => vec![],
        };
        // note: this cannot be done along with the above because the above is fallible and this op leaks.
        let children_ptr = children_vec
            .into_iter()
            .map(Box::into_raw)
            .collect::<Box<_>>();
        let n_children = children_ptr.len() as i64;

        let flags = field.is_nullable() as i64 * 2;

        let dictionary = if let DataType::Dictionary(_, values) = field.data_type() {
            // we do not store field info in the dict values, so can't recover it all :(
            let field = Field::new("item", values.as_ref().clone(), true);
            Some(Box::new(Ffi_ArrowSchema::try_new(field)?))
        } else {
            None
        };

        let mut private = Box::new(SchemaPrivateData {
            field,
            children_ptr,
            dictionary: dictionary.map(Box::into_raw),
        });

        // <https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema>
        Ok(Ffi_ArrowSchema {
            format: CString::new(format).unwrap().into_raw(),
            name: CString::new(name).unwrap().into_raw(),
            metadata: std::ptr::null_mut(),
            flags,
            n_children,
            children: private.children_ptr.as_mut_ptr(),
            dictionary: private.dictionary.unwrap_or(std::ptr::null_mut()),
            release: Some(c_release_schema),
            private_data: Box::into_raw(private) as *mut ::std::os::raw::c_void,
        })
    }

    /// create an empty [Ffi_ArrowSchema]
    pub fn empty() -> Self {
        Self {
            format: std::ptr::null_mut(),
            name: std::ptr::null_mut(),
            metadata: std::ptr::null_mut(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    /// returns the format of this schema.
    pub fn format(&self) -> &str {
        assert!(!self.format.is_null());
        // safe because the lifetime of `self.format` equals `self`
        unsafe { CStr::from_ptr(self.format) }
            .to_str()
            .expect("The external API has a non-utf8 as format")
    }

    /// returns the name of this schema.
    pub fn name(&self) -> &str {
        assert!(!self.name.is_null());
        // safe because the lifetime of `self.name` equals `self`
        unsafe { CStr::from_ptr(self.name) }.to_str().unwrap()
    }

    pub fn child(&self, index: usize) -> &'static Self {
        assert!(index < self.n_children as usize);
        assert!(!self.name.is_null());
        unsafe { self.children.add(index).as_ref().unwrap().as_ref().unwrap() }
    }

    pub fn dictionary(&self) -> Option<&'static Self> {
        if self.dictionary.is_null() {
            return None;
        };
        Some(unsafe { self.dictionary.as_ref().unwrap() })
    }

    pub fn nullable(&self) -> bool {
        (self.flags / 2) & 1 == 1
    }
}

impl Drop for Ffi_ArrowSchema {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

pub fn to_field(schema: &Ffi_ArrowSchema) -> Result<Field> {
    let dictionary = schema.dictionary();
    let data_type = if let Some(dictionary) = dictionary {
        let indices_data_type = to_data_type(schema)?;
        let values_data_type = to_data_type(dictionary)?;
        DataType::Dictionary(Box::new(indices_data_type), Box::new(values_data_type))
    } else {
        to_data_type(schema)?
    };
    Ok(Field::new(schema.name(), data_type, schema.nullable()))
}

fn to_data_type(schema: &Ffi_ArrowSchema) -> Result<DataType> {
    Ok(match schema.format() {
        "n" => DataType::Null,
        "b" => DataType::Boolean,
        "c" => DataType::Int8,
        "C" => DataType::UInt8,
        "s" => DataType::Int16,
        "S" => DataType::UInt16,
        "i" => DataType::Int32,
        "I" => DataType::UInt32,
        "l" => DataType::Int64,
        "L" => DataType::UInt64,
        "e" => DataType::Float16,
        "f" => DataType::Float32,
        "g" => DataType::Float64,
        "z" => DataType::Binary,
        "Z" => DataType::LargeBinary,
        "u" => DataType::Utf8,
        "U" => DataType::LargeUtf8,
        "tdD" => DataType::Date32,
        "tdm" => DataType::Date64,
        "tts" => DataType::Time32(TimeUnit::Second),
        "ttm" => DataType::Time32(TimeUnit::Millisecond),
        "ttu" => DataType::Time64(TimeUnit::Microsecond),
        "ttn" => DataType::Time64(TimeUnit::Nanosecond),
        "tDs" => DataType::Duration(TimeUnit::Second),
        "tDm" => DataType::Duration(TimeUnit::Millisecond),
        "tDu" => DataType::Duration(TimeUnit::Microsecond),
        "tDn" => DataType::Duration(TimeUnit::Nanosecond),
        "tiM" => DataType::Interval(IntervalUnit::YearMonth),
        "tiD" => DataType::Interval(IntervalUnit::DayTime),
        "+l" => {
            let child = schema.child(0);
            DataType::List(Box::new(to_field(child)?))
        }
        "+L" => {
            let child = schema.child(0);
            DataType::LargeList(Box::new(to_field(child)?))
        }
        "+s" => {
            let children = (0..schema.n_children as usize)
                .map(|x| to_field(schema.child(x)))
                .collect::<Result<Vec<_>>>()?;
            DataType::Struct(children)
        }
        other => {
            let parts = other.split(':').collect::<Vec<_>>();
            if parts.len() == 2 && parts[0] == "tss" {
                DataType::Timestamp(TimeUnit::Second, Some(parts[1].to_string()))
            } else if parts.len() == 2 && parts[0] == "tsm" {
                DataType::Timestamp(TimeUnit::Millisecond, Some(parts[1].to_string()))
            } else if parts.len() == 2 && parts[0] == "tsu" {
                DataType::Timestamp(TimeUnit::Microsecond, Some(parts[1].to_string()))
            } else if parts.len() == 2 && parts[0] == "tsn" {
                DataType::Timestamp(TimeUnit::Nanosecond, Some(parts[1].to_string()))
            } else if parts.len() == 2 && parts[0] == "d" {
                let parts = parts[1].split(',').collect::<Vec<_>>();
                if parts.len() < 2 || parts.len() > 3 {
                    return Err(ArrowError::Ffi(
                        "Decimal must contain 2 or 3 comma-separated values".to_string(),
                    ));
                };
                if parts.len() == 3 {
                    let bit_width = parts[0].parse::<usize>().map_err(|_| {
                        ArrowError::Ffi("Decimal bit width is not a valid integer".to_string())
                    })?;
                    if bit_width != 128 {
                        return Err(ArrowError::Ffi("Decimal256 is not supported".to_string()));
                    }
                }
                let precision = parts[0].parse::<usize>().map_err(|_| {
                    ArrowError::Ffi("Decimal precision is not a valid integer".to_string())
                })?;
                let scale = parts[1].parse::<usize>().map_err(|_| {
                    ArrowError::Ffi("Decimal scale is not a valid integer".to_string())
                })?;
                DataType::Decimal(precision, scale)
            } else {
                return Err(ArrowError::Ffi(format!(
                    "The datatype \"{}\" is still not supported in Rust implementation",
                    other
                )));
            }
        }
    })
}

/// the inverse of [to_field]
fn to_format(data_type: &DataType) -> Result<String> {
    Ok(match data_type {
        DataType::Null => "n",
        DataType::Boolean => "b",
        DataType::Int8 => "c",
        DataType::UInt8 => "C",
        DataType::Int16 => "s",
        DataType::UInt16 => "S",
        DataType::Int32 => "i",
        DataType::UInt32 => "I",
        DataType::Int64 => "l",
        DataType::UInt64 => "L",
        DataType::Float16 => "e",
        DataType::Float32 => "f",
        DataType::Float64 => "g",
        DataType::Binary => "z",
        DataType::LargeBinary => "Z",
        DataType::Utf8 => "u",
        DataType::LargeUtf8 => "U",
        DataType::Date32 => "tdD",
        DataType::Date64 => "tdm",
        DataType::Time32(TimeUnit::Second) => "tts",
        DataType::Time32(TimeUnit::Millisecond) => "ttm",
        DataType::Time64(TimeUnit::Microsecond) => "ttu",
        DataType::Time64(TimeUnit::Nanosecond) => "ttn",
        DataType::Duration(TimeUnit::Second) => "tDs",
        DataType::Duration(TimeUnit::Millisecond) => "tDm",
        DataType::Duration(TimeUnit::Microsecond) => "tDu",
        DataType::Duration(TimeUnit::Nanosecond) => "tDn",
        DataType::Interval(IntervalUnit::YearMonth) => "tiM",
        DataType::Interval(IntervalUnit::DayTime) => "tiD",
        DataType::Timestamp(unit, tz) => {
            let unit = match unit {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "m",
                TimeUnit::Microsecond => "u",
                TimeUnit::Nanosecond => "n",
            };
            return Ok(format!(
                "ts{}:{}",
                unit,
                tz.as_ref().map(|x| x.as_ref()).unwrap_or("")
            ));
        }
        DataType::Decimal(precision, scale) => return Ok(format!("d:{},{}", precision, scale)),
        DataType::List(_) => "+l",
        DataType::LargeList(_) => "+L",
        DataType::Struct(_) => "+s",
        DataType::FixedSizeBinary(size) => return Ok(format!("w{}", size)),
        DataType::FixedSizeList(_, size) => return Ok(format!("+w:{}", size)),
        DataType::Union(_) => todo!(),
        DataType::Dictionary(index, _) => return to_format(index.as_ref()),
        _ => todo!(),
    }
    .to_string())
}
