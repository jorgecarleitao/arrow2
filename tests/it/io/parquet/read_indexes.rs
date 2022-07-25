use std::io::Cursor;

use arrow2::error::Error;
use arrow2::{array::*, datatypes::*, error::Result, io::parquet::read::*, io::parquet::write::*};
use parquet2::indexes::{compute_rows, select_pages};
use parquet2::read::IndexedPageReader;

/// Returns 2 sets of pages with different the same number of rows distributed un-evenly
fn pages(
    arrays: &[&dyn Array],
    encoding: Encoding,
) -> Result<(Vec<EncodedPage>, Vec<EncodedPage>, Schema)> {
    // create pages with different number of rows
    let array11 = PrimitiveArray::<i64>::from_slice([1, 2, 3, 4]);
    let array12 = PrimitiveArray::<i64>::from_slice([5]);
    let array13 = PrimitiveArray::<i64>::from_slice([6]);

    let schema = Schema::from(vec![
        Field::new("a1", DataType::Int64, false),
        Field::new(
            "a2",
            arrays[0].data_type().clone(),
            arrays.iter().map(|x| x.null_count()).sum::<usize>() != 0usize,
        ),
    ]);

    let parquet_schema = to_parquet_schema(&schema)?;

    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V1,
    };

    let pages1 = [array11, array12, array13]
        .into_iter()
        .map(|array| {
            array_to_page(
                &array,
                parquet_schema.columns()[0]
                    .descriptor
                    .primitive_type
                    .clone(),
                &[Nested::Primitive(None, true, array.len())],
                options,
                Encoding::Plain,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let pages2 = arrays
        .iter()
        .flat_map(|array| {
            array_to_pages(
                *array,
                parquet_schema.columns()[1]
                    .descriptor
                    .primitive_type
                    .clone(),
                &[Nested::Primitive(None, true, array.len())],
                options,
                encoding,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
        })
        .collect::<Vec<_>>();

    Ok((pages1, pages2, schema))
}

/// Tests reading pages while skipping indexes
fn read_with_indexes(
    (pages1, pages2, schema): (Vec<EncodedPage>, Vec<EncodedPage>, Schema),
    expected: Box<dyn Array>,
) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V1,
    };

    let to_compressed = |pages: Vec<EncodedPage>| {
        let encoded_pages = DynIter::new(pages.into_iter().map(Ok));
        let compressed_pages =
            Compressor::new(encoded_pages, options.compression, vec![]).map_err(Error::from);
        Result::Ok(DynStreamingIterator::new(compressed_pages))
    };

    let row_group = DynIter::new(vec![to_compressed(pages1), to_compressed(pages2)].into_iter());

    let writer = vec![];
    let mut writer = FileWriter::try_new(writer, schema, options)?;

    writer.write(row_group)?;
    writer.end(None)?;
    let data = writer.into_inner();

    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    let schema = infer_schema(&metadata)?;

    let row_group = &metadata.row_groups[0];

    let pages = read_pages_locations(&mut reader, row_group.columns())?;

    // say we concluded from the indexes that we only needed the "6" from the first column, so second page.
    let _indexes = read_columns_indexes(&mut reader, row_group.columns(), &schema.fields)?;
    let intervals = compute_rows(&[false, true, false], &pages[0], row_group.num_rows())?;

    // based on the intervals from c1, we compute which pages from the second column are required:
    let pages = select_pages(&intervals, &pages[1], row_group.num_rows())?;

    // and read them:
    let c1 = &metadata.row_groups[0].columns()[1];

    let pages = IndexedPageReader::new(reader, c1, pages, vec![], vec![]);
    let pages = BasicDecompressor::new(pages, vec![]);

    let arrays = column_iter_to_arrays(
        vec![pages],
        vec![&c1.descriptor().descriptor.primitive_type],
        schema.fields[1].clone(),
        None,
        row_group.num_rows() as usize,
    )?;

    let arrays = arrays.collect::<Result<Vec<_>>>()?;

    assert_eq!(arrays, vec![expected]);
    Ok(())
}

#[test]
fn indexed_required_utf8() -> Result<()> {
    let array21 = Utf8Array::<i32>::from_slice(["a", "b", "c"]);
    let array22 = Utf8Array::<i32>::from_slice(["d", "e", "f"]);
    let expected = Utf8Array::<i32>::from_slice(["e"]).boxed();

    read_with_indexes(pages(&[&array21, &array22], Encoding::Plain)?, expected)
}

#[test]
fn indexed_required_i32() -> Result<()> {
    let array21 = Int32Array::from_slice([1, 2, 3]);
    let array22 = Int32Array::from_slice([4, 5, 6]);
    let expected = Int32Array::from_slice([5]).boxed();

    read_with_indexes(pages(&[&array21, &array22], Encoding::Plain)?, expected)
}

#[test]
fn indexed_optional_i32() -> Result<()> {
    let array21 = Int32Array::from([Some(1), Some(2), None]);
    let array22 = Int32Array::from([None, Some(5), Some(6)]);
    let expected = Int32Array::from_slice([5]).boxed();

    read_with_indexes(pages(&[&array21, &array22], Encoding::Plain)?, expected)
}

#[test]
fn indexed_optional_utf8() -> Result<()> {
    let array21 = Utf8Array::<i32>::from([Some("a"), Some("b"), None]);
    let array22 = Utf8Array::<i32>::from([None, Some("e"), Some("f")]);
    let expected = Utf8Array::<i32>::from_slice(["e"]).boxed();

    read_with_indexes(pages(&[&array21, &array22], Encoding::Plain)?, expected)
}

#[test]
fn indexed_required_fixed_len() -> Result<()> {
    let array21 = FixedSizeBinaryArray::from_slice([[127], [128], [129]]);
    let array22 = FixedSizeBinaryArray::from_slice([[130], [131], [132]]);
    let expected = FixedSizeBinaryArray::from_slice([[131]]).boxed();

    read_with_indexes(pages(&[&array21, &array22], Encoding::Plain)?, expected)
}

#[test]
fn indexed_optional_fixed_len() -> Result<()> {
    let array21 = FixedSizeBinaryArray::from([Some([127]), Some([128]), None]);
    let array22 = FixedSizeBinaryArray::from([None, Some([131]), Some([132])]);
    let expected = FixedSizeBinaryArray::from_slice([[131]]).boxed();

    read_with_indexes(pages(&[&array21, &array22], Encoding::Plain)?, expected)
}

#[test]
fn indexed_required_boolean() -> Result<()> {
    let array21 = BooleanArray::from_slice([true, false, true]);
    let array22 = BooleanArray::from_slice([false, false, true]);
    let expected = BooleanArray::from_slice([false]).boxed();

    read_with_indexes(pages(&[&array21, &array22], Encoding::Plain)?, expected)
}

#[test]
fn indexed_optional_boolean() -> Result<()> {
    let array21 = BooleanArray::from([Some(true), Some(false), None]);
    let array22 = BooleanArray::from([None, Some(false), Some(true)]);
    let expected = BooleanArray::from_slice([false]).boxed();

    read_with_indexes(pages(&[&array21, &array22], Encoding::Plain)?, expected)
}

#[test]
fn indexed_dict() -> Result<()> {
    let indices = PrimitiveArray::from_values((0..6u64).map(|x| x % 2));
    let values = PrimitiveArray::from_slice([4i32, 6i32]).boxed();
    let array = DictionaryArray::try_from_keys(indices, values).unwrap();

    let indices = PrimitiveArray::from_slice(&[0u64]);
    let values = PrimitiveArray::from_slice([4i32, 6i32]).boxed();
    let expected = DictionaryArray::try_from_keys(indices, values).unwrap();

    let expected = expected.boxed();

    read_with_indexes(pages(&[&array], Encoding::RleDictionary)?, expected)
}
