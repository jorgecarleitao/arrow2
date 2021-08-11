# Changelog

## [v0.4.0](https://github.com/jorgecarleitao/arrow2/tree/v0.4.0) (2021-08-24)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.3.0...v0.4.0)

**Breaking changes:**

- Change dictionary iterator of values from `Array`s of one element to `Scalar`s [\#335](https://github.com/jorgecarleitao/arrow2/issues/335)
- Align FFI API with arrow's C++ API [\#328](https://github.com/jorgecarleitao/arrow2/issues/328)
- Make `*_compare_scalar` not return `Result` [\#316](https://github.com/jorgecarleitao/arrow2/issues/316)
- Make `io::print`, `get_value_display` and `get_display` not return `Result` [\#286](https://github.com/jorgecarleitao/arrow2/issues/286)
- Add `MetadataVersion` to IPC interfaces [\#282](https://github.com/jorgecarleitao/arrow2/issues/282)
- Change `DataType::Union` to enable round trips in IPC [\#281](https://github.com/jorgecarleitao/arrow2/issues/281)
- Removed clone requirement in `StructArray -> RecordBatch` [\#307](https://github.com/jorgecarleitao/arrow2/pull/307) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in reading a non-finished IPC stream. [\#302](https://github.com/jorgecarleitao/arrow2/pull/302) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Generalized ZipIterator to accept a `BitmapIter` [\#296](https://github.com/jorgecarleitao/arrow2/pull/296) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added API to FFI `Field` [\#321](https://github.com/jorgecarleitao/arrow2/pull/321) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `compare_scalar` [\#317](https://github.com/jorgecarleitao/arrow2/pull/317) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Add `UnionArray` [\#283](https://github.com/jorgecarleitao/arrow2/pull/283) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- SliceIterator of last bytes is not correct [\#292](https://github.com/jorgecarleitao/arrow2/issues/292)
- Fixed error in displaying dictionaries with nulls in values [\#334](https://github.com/jorgecarleitao/arrow2/pull/334) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in dict equality [\#333](https://github.com/jorgecarleitao/arrow2/pull/333) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed small inconsistencies between `compute::cast` and `compute::can_cast` [\#295](https://github.com/jorgecarleitao/arrow2/pull/295) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed order implementation for `days_ms` / `Interval(DayTime)` [\#285](https://github.com/jorgecarleitao/arrow2/pull/285) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added support for remaining non-nested datatypes [\#336](https://github.com/jorgecarleitao/arrow2/pull/336) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `multiversion` and `lexical-core` optional [\#324](https://github.com/jorgecarleitao/arrow2/pull/324) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of utf8 comparison \(1.7x-4x\) [\#322](https://github.com/jorgecarleitao/arrow2/pull/322) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of boolean comparison \(5x-14x\) [\#318](https://github.com/jorgecarleitao/arrow2/pull/318) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added trait `TryPush` [\#314](https://github.com/jorgecarleitao/arrow2/pull/314) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added cast `date32 -> i64` and `date64 -> i32` [\#308](https://github.com/jorgecarleitao/arrow2/pull/308) ([ritchie46](https://github.com/ritchie46))
- Improved performance of comparison with SIMD feature flag \(2x-3.5x\) [\#305](https://github.com/jorgecarleitao/arrow2/pull/305) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read json to `BinaryArray` [\#304](https://github.com/jorgecarleitao/arrow2/pull/304) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved `MutableFixedSizeBinaryArray` [\#303](https://github.com/jorgecarleitao/arrow2/pull/303) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved `MutablePrimitiveArray` and `MutableUtf8Array` [\#299](https://github.com/jorgecarleitao/arrow2/pull/299) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved `MutableBooleanArray` [\#297](https://github.com/jorgecarleitao/arrow2/pull/297) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of concatenating non-aligned validities \(15x\) [\#291](https://github.com/jorgecarleitao/arrow2/pull/291) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for timestamps with tz and interval to `io::print::write` [\#287](https://github.com/jorgecarleitao/arrow2/pull/287) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved debug repr of buffers and bitmaps. [\#284](https://github.com/jorgecarleitao/arrow2/pull/284) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Cleaned up internals of json integration [\#280](https://github.com/jorgecarleitao/arrow2/pull/280) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `serde_derive` dependency [\#279](https://github.com/jorgecarleitao/arrow2/pull/279) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified IPC code. [\#277](https://github.com/jorgecarleitao/arrow2/pull/277) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Reduced dependencies from confi-table and enabled `wasm` on `io_print` feature. [\#276](https://github.com/jorgecarleitao/arrow2/pull/276) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improve performance of `rem_scalar/div_scalar` for integer types \(4x-10x\) [\#275](https://github.com/jorgecarleitao/arrow2/pull/275) ([ritchie46](https://github.com/ritchie46))

**Documentation updates:**

- Cleaned examples and docs from old API. [\#330](https://github.com/jorgecarleitao/arrow2/pull/330) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved documentation [\#306](https://github.com/jorgecarleitao/arrow2/pull/306) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Improved naming of testing workflows [\#315](https://github.com/jorgecarleitao/arrow2/pull/315) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added tests to scalar API [\#300](https://github.com/jorgecarleitao/arrow2/pull/300) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made CSV and JSON tests not use files. [\#290](https://github.com/jorgecarleitao/arrow2/pull/290) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved tests to integration tests [\#289](https://github.com/jorgecarleitao/arrow2/pull/289) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Closed issues:**

- Make parquet\_read\_record support async [\#331](https://github.com/jorgecarleitao/arrow2/issues/331)
- Panic due to SIMD comparison [\#312](https://github.com/jorgecarleitao/arrow2/issues/312)
- Bitmap::mutable line 155 may Panic/segfault [\#309](https://github.com/jorgecarleitao/arrow2/issues/309)
- IPC's `StreamReader` may abort due to excessive memory by overflowing a `usize`d variable [\#301](https://github.com/jorgecarleitao/arrow2/issues/301)
- Improve performance of `rem_scalar/div_scalar` for integer types \(4x-10x\) [\#259](https://github.com/jorgecarleitao/arrow2/issues/259)

## [v0.3.0](https://github.com/jorgecarleitao/arrow2/tree/v0.3.0) (2021-08-11)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.2.0...v0.3.0)

**Breaking changes:**

- Renamed `sum` to `sum_primitive` [\#273](https://github.com/jorgecarleitao/arrow2/issues/273)
- Moved trait `Index` from `array::Index` to `types::Index` [\#272](https://github.com/jorgecarleitao/arrow2/issues/272)
- Added optional `projection` to IPC FileReader [\#271](https://github.com/jorgecarleitao/arrow2/issues/271)
- Added optional `page_filter` to parquet's `RecordReader` and `get_page_iterator` [\#270](https://github.com/jorgecarleitao/arrow2/issues/270)
- Renamed parquets' `CompressionCodec` to `Compression` [\#269](https://github.com/jorgecarleitao/arrow2/issues/269)

**New features:**

- Added support for FFI of dictionary-encoded arrays [\#267](https://github.com/jorgecarleitao/arrow2/pull/267) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for projection pushdown on IPC files [\#264](https://github.com/jorgecarleitao/arrow2/pull/264) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read parquet asynchronously [\#260](https://github.com/jorgecarleitao/arrow2/pull/260) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to filter parquet pages. [\#256](https://github.com/jorgecarleitao/arrow2/pull/256) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added wrapping\_cast to cast kernels [\#254](https://github.com/jorgecarleitao/arrow2/pull/254) ([sundy-li](https://github.com/sundy-li))
- Added support to parquet IO on wasm32 [\#239](https://github.com/jorgecarleitao/arrow2/pull/239) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to round-trip dictionary arrays on parquet [\#232](https://github.com/jorgecarleitao/arrow2/pull/232) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added Scalar API [\#56](https://github.com/jorgecarleitao/arrow2/pull/56) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error in computing remainder of chunk iterator [\#262](https://github.com/jorgecarleitao/arrow2/pull/262) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in slicing bitmap. [\#250](https://github.com/jorgecarleitao/arrow2/pull/250) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Improve the performance in cast kernel using AsPrimitive trait in generic dispatch [\#252](https://github.com/jorgecarleitao/arrow2/issues/252)
- Poor performance in `sort::sort_to_indices`  with limit option in arrow2 [\#245](https://github.com/jorgecarleitao/arrow2/issues/245)
- Support loading Feather v2 \(IPC\) files with more than 1 million tables [\#231](https://github.com/jorgecarleitao/arrow2/issues/231)
- Migrated to parquet2 v0.3 [\#265](https://github.com/jorgecarleitao/arrow2/pull/265) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests to cast and min/max [\#253](https://github.com/jorgecarleitao/arrow2/pull/253) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Prettytable is unmaintained. Change to comfy-table [\#251](https://github.com/jorgecarleitao/arrow2/pull/251) ([PsiACE](https://github.com/PsiACE))
- Added IndexRange to remove checks in hot loops [\#247](https://github.com/jorgecarleitao/arrow2/pull/247) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Make merge\_sort\_slices MergeSortSlices public [\#243](https://github.com/jorgecarleitao/arrow2/pull/243) ([sundy-li](https://github.com/sundy-li))

**Documentation updates:**

- Added example and guide section on compute [\#242](https://github.com/jorgecarleitao/arrow2/pull/242) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Closed issues:**

- Allow projection pushdown to IPC files [\#261](https://github.com/jorgecarleitao/arrow2/issues/261)
- Add support to write dictionary-encoded pages [\#211](https://github.com/jorgecarleitao/arrow2/issues/211)
- Make IpcWriteOptions easier to find. [\#120](https://github.com/jorgecarleitao/arrow2/issues/120)

## [v0.2.0](https://github.com/jorgecarleitao/arrow2/tree/v0.2.0) (2021-07-30)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.1.0...v0.2.0)

**Breaking changes:**

- Simplified `new` signature of growable API [\#238](https://github.com/jorgecarleitao/arrow2/pull/238) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Add support to merge sort with a limit [\#222](https://github.com/jorgecarleitao/arrow2/pull/222) ([sundy-li](https://github.com/sundy-li))
- Generalized sort to accept indices other than i32. [\#220](https://github.com/jorgecarleitao/arrow2/pull/220) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for limited sort [\#218](https://github.com/jorgecarleitao/arrow2/pull/218) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Merge sort support limit option [\#221](https://github.com/jorgecarleitao/arrow2/issues/221)
- Introduce limit option to sort [\#215](https://github.com/jorgecarleitao/arrow2/issues/215)
- Added support for take of interval of days\_ms [\#219](https://github.com/jorgecarleitao/arrow2/pull/219) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added FFI for remaining types [\#213](https://github.com/jorgecarleitao/arrow2/pull/213) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Filter operation on sliced utf8 arrays are incorrect [\#233](https://github.com/jorgecarleitao/arrow2/issues/233)
- Fixed error in slicing bitmap. [\#237](https://github.com/jorgecarleitao/arrow2/pull/237) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed nested FFI. [\#212](https://github.com/jorgecarleitao/arrow2/pull/212) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Avoid materialization of indices in filter\_record\_batch for single arrays [\#234](https://github.com/jorgecarleitao/arrow2/issues/234)
- Add integration tests for writing to parquet [\#80](https://github.com/jorgecarleitao/arrow2/issues/80)
- Short-circuited boolean evaluation in GrowableList [\#228](https://github.com/jorgecarleitao/arrow2/pull/228) ([ritchie46](https://github.com/ritchie46))
- Add extra inlining to speed up take [\#226](https://github.com/jorgecarleitao/arrow2/pull/226) ([Dandandan](https://github.com/Dandandan))
- Removed un-needed `unsafe` [\#225](https://github.com/jorgecarleitao/arrow2/pull/225) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Add documentation to guide [\#96](https://github.com/jorgecarleitao/arrow2/issues/96)
- Add git submodule command to correct the test doc [\#223](https://github.com/jorgecarleitao/arrow2/pull/223) ([sundy-li](https://github.com/sundy-li))
- Added badges to README [\#216](https://github.com/jorgecarleitao/arrow2/pull/216) ([sundy-li](https://github.com/sundy-li))
- Clarified differences with arrow crate [\#210](https://github.com/jorgecarleitao/arrow2/pull/210) ([alamb](https://github.com/alamb))
- Clarified differences with arrow crate [\#209](https://github.com/jorgecarleitao/arrow2/pull/209) ([alamb](https://github.com/alamb))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
