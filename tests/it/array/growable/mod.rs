mod binary;
mod boolean;
mod dictionary;
mod fixed_binary;
mod list;
mod null;
mod primitive;
mod structure;
mod utf8;

/*
#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use super::*;

    use crate::{
        array::{
            Array, ArrayDataRef, ArrayRef, BooleanArray, DictionaryArray,
            FixedSizeBinaryArray, Int16Array, Int16Type, Int32Array, Int64Array,
            Int64Builder, ListBuilder, NullArray, PrimitiveBuilder, StringArray,
            StringDictionaryBuilder, StructArray, UInt8Array,
        },
        buffer::Buffer,
        datatypes::Field,
    };
    use crate::{
        array::{ListArray, StringBuilder},
        error::Result,
    };

    fn create_dictionary_array(values: &[&str], keys: &[Option<&str>]) -> ArrayDataRef {
        let values = StringArray::from(values.to_vec());
        let mut builder = StringDictionaryBuilder::new_with_dictionary(
            PrimitiveBuilder::<Int16Type>::new(3),
            &values,
        )
        .unwrap();
        for key in keys {
            if let Some(v) = key {
                builder.append(v).unwrap();
            } else {
                builder.append_null().unwrap()
            }
        }
        builder.finish().data()
    }

    /*
    // this is an old test used on a meanwhile removed dead code
    // that is still useful when `MutableArrayData` supports fixed-size lists.
    #[test]
    fn test_fixed_size_list_append() -> Result<()> {
        let int_builder = UInt16Builder::new(64);
        let mut builder = FixedSizeListBuilder::<UInt16Builder>::new(int_builder, 2);
        builder.values().append_slice(&[1, 2])?;
        builder.append(true)?;
        builder.values().append_slice(&[3, 4])?;
        builder.append(false)?;
        builder.values().append_slice(&[5, 6])?;
        builder.append(true)?;

        let a_builder = UInt16Builder::new(64);
        let mut a_builder = FixedSizeListBuilder::<UInt16Builder>::new(a_builder, 2);
        a_builder.values().append_slice(&[7, 8])?;
        a_builder.append(true)?;
        a_builder.values().append_slice(&[9, 10])?;
        a_builder.append(true)?;
        a_builder.values().append_slice(&[11, 12])?;
        a_builder.append(false)?;
        a_builder.values().append_slice(&[13, 14])?;
        a_builder.append(true)?;
        a_builder.values().append_null()?;
        a_builder.values().append_null()?;
        a_builder.append(true)?;
        let a = a_builder.finish();

        // append array
        builder.append_data(&[
            a.data(),
            a.slice(1, 3).data(),
            a.slice(2, 1).data(),
            a.slice(5, 0).data(),
        ])?;
        let finished = builder.finish();

        let expected_int_array = UInt16Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            // append first array
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            None,
            None,
            // append slice(1, 3)
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            // append slice(2, 1)
            Some(11),
            Some(12),
        ]);
        let expected_list_data = ArrayData::new(
            DataType::FixedSizeList(
                Box::new(Field::new("item", DataType::UInt16, true)),
                2,
            ),
            12,
            None,
            None,
            0,
            vec![],
            vec![expected_int_array.data()],
        );
        let expected_list =
            FixedSizeListArray::from(Arc::new(expected_list_data) as ArrayDataRef);
        assert_eq!(&expected_list.values(), &finished.values());
        assert_eq!(expected_list.len(), finished.len());

        Ok(())
    }
    */
}
*/
