# Changelog

## [v0.6.2](https://github.com/jorgecarleitao/arrow2/tree/v0.6.2) (2021-10-09)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.6.1...v0.6.2)

**New features:**

- Added wrapping version arithmetics for `PrimitiveArray` [\#496](https://github.com/jorgecarleitao/arrow2/pull/496) ([yjhmelody](https://github.com/yjhmelody))

**Fixed bugs:**

- Do not check offsets or utf8 validity in ffi \(\#505\) [\#510](https://github.com/jorgecarleitao/arrow2/pull/510) ([NilsBarlaug](https://github.com/NilsBarlaug))
- Made `try_push_valid` public again [\#509](https://github.com/jorgecarleitao/arrow2/pull/509) ([ritchie46](https://github.com/ritchie46))

**Enhancements:**

- Use static-typed equal functions directly [\#507](https://github.com/jorgecarleitao/arrow2/pull/507) ([yjhmelody](https://github.com/yjhmelody))

## [v0.6.1](https://github.com/jorgecarleitao/arrow2/tree/v0.6.1) (2021-10-07)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.5.3...v0.6.1)

**Breaking changes:**

- Bring `MutableFixedSizeListArray` to the spec used by the rest of the Mutable API [\#475](https://github.com/jorgecarleitao/arrow2/issues/475)
- Removed `ALIGNMENT` invariant from `[Mutable]Buffer` [\#449](https://github.com/jorgecarleitao/arrow2/issues/449)
- Un-nested `compute::arithemtics::basic` [\#461](https://github.com/jorgecarleitao/arrow2/pull/461) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more serialization options for csv writer. [\#453](https://github.com/jorgecarleitao/arrow2/pull/453) ([ritchie46](https://github.com/ritchie46))
- Changed validity from `&Option<Bitmap>` to `Option<&Bitmap>`. [\#431](https://github.com/jorgecarleitao/arrow2/pull/431) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped parquet2 [\#422](https://github.com/jorgecarleitao/arrow2/pull/422) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Changed IPC `FileWriter` to own the `writer`. [\#420](https://github.com/jorgecarleitao/arrow2/pull/420) ([yjshen](https://github.com/yjshen))
- Made `DynComparator` `Send+Sync` [\#414](https://github.com/jorgecarleitao/arrow2/pull/414) ([yjshen](https://github.com/yjshen))

**New features:**

- Read Decimal from Parquet File [\#444](https://github.com/jorgecarleitao/arrow2/issues/444)
- Add IO read for Avro [\#401](https://github.com/jorgecarleitao/arrow2/issues/401)
- Added support to read Avro logical types, `List`,`Enum`, `Duration` and `Fixed`. [\#493](https://github.com/jorgecarleitao/arrow2/pull/493) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added read `Decimal` from parquet [\#489](https://github.com/jorgecarleitao/arrow2/pull/489) ([potter420](https://github.com/potter420))
- Implement `BitXor` trait for `Bitmap` [\#485](https://github.com/jorgecarleitao/arrow2/pull/485) ([houqp](https://github.com/houqp))
- Added `extend`/`extend_unchecked` for `MutableBooleanArray` [\#478](https://github.com/jorgecarleitao/arrow2/pull/478) ([VasanthakumarV](https://github.com/VasanthakumarV))
- expose `shrink_to_fit` to mutable arrays [\#467](https://github.com/jorgecarleitao/arrow2/pull/467) ([ritchie46](https://github.com/ritchie46))
- Added support for `DataType::Map` and `MapArray` [\#464](https://github.com/jorgecarleitao/arrow2/pull/464) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Extract parts of datetime [\#433](https://github.com/jorgecarleitao/arrow2/pull/433) ([VasanthakumarV](https://github.com/VasanthakumarV))
- Added support to add an interval to a timestamp [\#417](https://github.com/jorgecarleitao/arrow2/pull/417) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read Avro. [\#406](https://github.com/jorgecarleitao/arrow2/pull/406) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced own allocator by `std::Vec`. [\#385](https://github.com/jorgecarleitao/arrow2/pull/385) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- crash in parquet read [\#459](https://github.com/jorgecarleitao/arrow2/issues/459)
- Made writing stream to parquet require a non-static lifetime [\#471](https://github.com/jorgecarleitao/arrow2/pull/471) ([GrandChaman](https://github.com/GrandChaman))
- Made importing from FFI `unsafe` [\#458](https://github.com/jorgecarleitao/arrow2/pull/458) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed panic in division using nulls. [\#438](https://github.com/jorgecarleitao/arrow2/pull/438) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error writing dictionary extension to IPC [\#397](https://github.com/jorgecarleitao/arrow2/pull/397) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in extending `MutableBitmap` [\#393](https://github.com/jorgecarleitao/arrow2/pull/393) ([jorgecarleitao](https://github.com/jorgecarleitao))


**Enhancements:**

- Some `compare` function are not exported [\#349](https://github.com/jorgecarleitao/arrow2/issues/349)
- Investigate how to add support for timezones in timestamp [\#23](https://github.com/jorgecarleitao/arrow2/issues/23)
- Made `hash` work for extension type [\#487](https://github.com/jorgecarleitao/arrow2/pull/487) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `extend`/`extend_unchecked` for `MutableBinaryArray` [\#486](https://github.com/jorgecarleitao/arrow2/pull/486) ([VasanthakumarV](https://github.com/VasanthakumarV))
- Improved inference and deserialization of CSV [\#483](https://github.com/jorgecarleitao/arrow2/pull/483) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `GrowableFixedSizeList` and improved `MutableFixedSizeListArray` [\#470](https://github.com/jorgecarleitao/arrow2/pull/470) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `MutableBitmap::shrink_to_fit` [\#468](https://github.com/jorgecarleitao/arrow2/pull/468) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `MutableArray::as_box` [\#450](https://github.com/jorgecarleitao/arrow2/pull/450) ([sd2k](https://github.com/sd2k))
- Improved performance of sum aggregation via aligned loads \(-10%\) [\#445](https://github.com/jorgecarleitao/arrow2/pull/445) ([ritchie46](https://github.com/ritchie46))
- Removed `assert` from `MutableBuffer::set_len` [\#443](https://github.com/jorgecarleitao/arrow2/pull/443) ([ritchie46](https://github.com/ritchie46))
- Optimized `null_count` [\#442](https://github.com/jorgecarleitao/arrow2/pull/442) ([ritchie46](https://github.com/ritchie46))
- Improved performance of list iterator \(- 10-20%\) [\#441](https://github.com/jorgecarleitao/arrow2/pull/441) ([ritchie46](https://github.com/ritchie46))
- Improved performance of `PrimitiveGrowable` for nulls \(-10%\) [\#434](https://github.com/jorgecarleitao/arrow2/pull/434) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Allowed accessing validity without importing `Array` [\#432](https://github.com/jorgecarleitao/arrow2/pull/432) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Optimize hashing using `ahash` and `multiversion` \(-30%\) [\#428](https://github.com/jorgecarleitao/arrow2/pull/428) ([Dandandan](https://github.com/Dandandan))
- Improved performance of iterator of `Utf8Array` and `BinaryArray` \(3-4x\) [\#427](https://github.com/jorgecarleitao/arrow2/pull/427) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of utf8 validation of large strings via `simdutf8` \(-40%\) [\#426](https://github.com/jorgecarleitao/arrow2/pull/426) ([Dandandan](https://github.com/Dandandan))
- Added reading of parquet required dictionary-encoded binary. [\#419](https://github.com/jorgecarleitao/arrow2/pull/419) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Add `extend`/`extend_unchecked` for `MutableUtf8Array` [\#413](https://github.com/jorgecarleitao/arrow2/pull/413) ([VasanthakumarV](https://github.com/VasanthakumarV))
- Added support to extract hours and years from timestamps with timezone [\#412](https://github.com/jorgecarleitao/arrow2/pull/412) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `io_csv_read` and `io_csv_write` feature [\#408](https://github.com/jorgecarleitao/arrow2/pull/408) ([ritchie46](https://github.com/ritchie46))
- Improve `comparison` docs and re-export the array-comparing function [\#404](https://github.com/jorgecarleitao/arrow2/pull/404) ([HagaiHargil](https://github.com/HagaiHargil))
- Added support to read dict-encoded required primitive types from parquet [\#402](https://github.com/jorgecarleitao/arrow2/pull/402) ([Dandandan](https://github.com/Dandandan))
- Added `Array::with_validity` [\#399](https://github.com/jorgecarleitao/arrow2/pull/399) ([ritchie46](https://github.com/ritchie46))

**Documentation updates:**

- Improved documentation [\#491](https://github.com/jorgecarleitao/arrow2/pull/491) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more API docs. [\#479](https://github.com/jorgecarleitao/arrow2/pull/479) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more documentation [\#476](https://github.com/jorgecarleitao/arrow2/pull/476) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved documentation [\#462](https://github.com/jorgecarleitao/arrow2/pull/462) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added example showing parallel writes to parquet \(x num\_cores\) [\#436](https://github.com/jorgecarleitao/arrow2/pull/436) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved documentation [\#430](https://github.com/jorgecarleitao/arrow2/pull/430) ([jorgecarleitao](https://github.com/jorgecarleitao))
- \[0.5\] The docs `io` module has no submodules [\#390](https://github.com/jorgecarleitao/arrow2/issues/390)
- Made docs be compiled with feature `full` [\#391](https://github.com/jorgecarleitao/arrow2/pull/391) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- DRY via macro. [\#477](https://github.com/jorgecarleitao/arrow2/pull/477) ([jorgecarleitao](https://github.com/jorgecarleitao))
- DRY of type check and len check code in `compute` [\#474](https://github.com/jorgecarleitao/arrow2/pull/474) ([yjhmelody](https://github.com/yjhmelody))
- Added property testing [\#460](https://github.com/jorgecarleitao/arrow2/pull/460) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added fmt to CI. [\#455](https://github.com/jorgecarleitao/arrow2/pull/455) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified CI [\#452](https://github.com/jorgecarleitao/arrow2/pull/452) ([jorgecarleitao](https://github.com/jorgecarleitao))
- fix filter kernels bench [\#440](https://github.com/jorgecarleitao/arrow2/pull/440) ([ritchie46](https://github.com/ritchie46))
- Reduced number of combinations in feature tests. [\#429](https://github.com/jorgecarleitao/arrow2/pull/429) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Move tests from `src/compute/` to `tests/` [\#423](https://github.com/jorgecarleitao/arrow2/pull/423) ([VasanthakumarV](https://github.com/VasanthakumarV))
- Skipped some feature permutations. [\#411](https://github.com/jorgecarleitao/arrow2/pull/411) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added tests to some invariants of `unsafe` [\#403](https://github.com/jorgecarleitao/arrow2/pull/403) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read and write extension types to and from parquet [\#396](https://github.com/jorgecarleitao/arrow2/pull/396) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fix testing of SIMD [\#394](https://github.com/jorgecarleitao/arrow2/pull/394) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.5.3](https://github.com/jorgecarleitao/arrow2/tree/v0.5.3) (2021-09-14)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.5.2...v0.5.3)

**New features:**

- Added support to read and write extension types to and from parquet [\#396](https://github.com/jorgecarleitao/arrow2/pull/396) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error writing dictionary extension to IPC [\#397](https://github.com/jorgecarleitao/arrow2/pull/397) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in extending `MutableBitmap` [\#393](https://github.com/jorgecarleitao/arrow2/pull/393) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added support to read dict-encoded required primitive types from parquet [\#402](https://github.com/jorgecarleitao/arrow2/pull/402) ([Dandandan](https://github.com/Dandandan))
- Added `Array::with_validity` [\#399](https://github.com/jorgecarleitao/arrow2/pull/399) ([ritchie46](https://github.com/ritchie46))

**Testing updates:**

- Fix testing of SIMD [\#394](https://github.com/jorgecarleitao/arrow2/pull/394) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.5.1](https://github.com/jorgecarleitao/arrow2/tree/v0.5.1) (2021-09-09)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.5.0...v0.5.1)

**Documentation updates:**

- \[0.5\] The docs `io` module has no submodules [\#390](https://github.com/jorgecarleitao/arrow2/issues/390)
- Made docs be compiled with feature `full` [\#391](https://github.com/jorgecarleitao/arrow2/pull/391) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.5.0](https://github.com/jorgecarleitao/arrow2/tree/v0.5.0) (2021-09-07)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.4.0...v0.5.0)

**Breaking changes:**

- Added `Extension` to `DataType` [\#361](https://github.com/jorgecarleitao/arrow2/issues/361)
- `MonthDayNano` added to enum `IntervalUnit` [\#360](https://github.com/jorgecarleitao/arrow2/issues/360)
- Make `io::parquet::write::write_*` return size of file in bytes [\#354](https://github.com/jorgecarleitao/arrow2/issues/354)
- Renamed `bitmap::utils::null_count` to `bitmap::utils::count_zeros` [\#342](https://github.com/jorgecarleitao/arrow2/issues/342)
- Made `GroupFilter` optional in parquet's`RecordReader` and added method to set it. [\#386](https://github.com/jorgecarleitao/arrow2/pull/386) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `PartialOrd` and `Ord` of all enums in `datatypes` [\#379](https://github.com/jorgecarleitao/arrow2/pull/379) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `cargo` features not default [\#369](https://github.com/jorgecarleitao/arrow2/pull/369) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Prepare APIs for extension types [\#357](https://github.com/jorgecarleitao/arrow2/pull/357) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added support for `async` parquet write [\#372](https://github.com/jorgecarleitao/arrow2/pull/372) ([GrandChaman](https://github.com/GrandChaman))
- Add support to extension types in FFI [\#363](https://github.com/jorgecarleitao/arrow2/pull/363) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for field's metadata via FFI [\#362](https://github.com/jorgecarleitao/arrow2/pull/362) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `Extension` \(logical\) type [\#359](https://github.com/jorgecarleitao/arrow2/pull/359) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for compute to `BinaryArray` [\#346](https://github.com/jorgecarleitao/arrow2/pull/346) ([zhyass](https://github.com/zhyass))
- Added support for reading binary from CSV [\#337](https://github.com/jorgecarleitao/arrow2/pull/337) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `MONTH_DAY_NANO` interval type [\#268](https://github.com/jorgecarleitao/arrow2/pull/268) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Parquet read skips a few rows at the end of the page [\#373](https://github.com/jorgecarleitao/arrow2/issues/373)
- `parquet_read` fails when a column has too many rows with string values [\#366](https://github.com/jorgecarleitao/arrow2/issues/366)
- `parquet_read` panics with `index_out_of_bounds` [\#351](https://github.com/jorgecarleitao/arrow2/issues/351)
- Fixed error in `MutableBitmap::push_unchecked` [\#384](https://github.com/jorgecarleitao/arrow2/pull/384) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed display of timestamp with tz. [\#375](https://github.com/jorgecarleitao/arrow2/pull/375) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Added `extend_*values` to `MutablePrimitiveArray` [\#383](https://github.com/jorgecarleitao/arrow2/pull/383) ([ritchie46](https://github.com/ritchie46))
- Improved performance of writing to CSV \(20-25%\) [\#382](https://github.com/jorgecarleitao/arrow2/pull/382) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped `lexical-core` [\#378](https://github.com/jorgecarleitao/arrow2/pull/378) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed casting of utf8 \<\> Timestamp with and without timezone [\#376](https://github.com/jorgecarleitao/arrow2/pull/376) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `Send+Sync` to `MutableBuffer` [\#368](https://github.com/jorgecarleitao/arrow2/pull/368) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of unary \_not\_ for aligned bitmaps \(3x\) [\#365](https://github.com/jorgecarleitao/arrow2/pull/365) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Reduced dependencies within `num` [\#353](https://github.com/jorgecarleitao/arrow2/pull/353) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped to parquet2 v0.4 [\#352](https://github.com/jorgecarleitao/arrow2/pull/352) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped tonic and prost in flight [\#344](https://github.com/jorgecarleitao/arrow2/pull/344) ([PsiACE](https://github.com/PsiACE))
- Improved null count calculation \(5x\) [\#343](https://github.com/jorgecarleitao/arrow2/pull/343) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved perf of deserializing integers from json \(30%\) [\#340](https://github.com/jorgecarleitao/arrow2/pull/340) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified code of json schema inference [\#339](https://github.com/jorgecarleitao/arrow2/pull/339) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Moved guide examples to examples/ [\#387](https://github.com/jorgecarleitao/arrow2/pull/387) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more docs. [\#358](https://github.com/jorgecarleitao/arrow2/pull/358) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved API docs. [\#355](https://github.com/jorgecarleitao/arrow2/pull/355) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Moved tests to `tests/` [\#389](https://github.com/jorgecarleitao/arrow2/pull/389) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved compute tests to tests/ [\#388](https://github.com/jorgecarleitao/arrow2/pull/388) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests. [\#380](https://github.com/jorgecarleitao/arrow2/pull/380) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Pinned nightly in SIMD tests [\#364](https://github.com/jorgecarleitao/arrow2/pull/364) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved benches for take [\#348](https://github.com/jorgecarleitao/arrow2/pull/348) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made IPC integration tests run tests that are not run by arrow-rs [\#278](https://github.com/jorgecarleitao/arrow2/pull/278) ([jorgecarleitao](https://github.com/jorgecarleitao))

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
