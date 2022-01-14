# Changelog

## [v0.9.0](https://github.com/jorgecarleitao/arrow2/tree/v0.9.0) (2022-01-14)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.8.1...v0.9.0)

**Breaking changes:**

- Added number of rows read in CSV inference [\#765](https://github.com/jorgecarleitao/arrow2/pull/765) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Refactored `nullif` [\#753](https://github.com/jorgecarleitao/arrow2/pull/753) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Migrated to latest parquet2 [\#752](https://github.com/jorgecarleitao/arrow2/pull/752) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replace flatbuffers dependency by Planus [\#732](https://github.com/jorgecarleitao/arrow2/pull/732) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified `Schema` and `Field` [\#728](https://github.com/jorgecarleitao/arrow2/pull/728) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced `RecordBatch` by `Chunk` [\#717](https://github.com/jorgecarleitao/arrow2/pull/717) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed `Option` from fields' metadata [\#715](https://github.com/jorgecarleitao/arrow2/pull/715) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved dict\_id to IPC-specific IO [\#713](https://github.com/jorgecarleitao/arrow2/pull/713) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Moved is\_ordered from `Field` to `DataType::Dictionary` [\#711](https://github.com/jorgecarleitao/arrow2/pull/711) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Refactored JSON writing \(5-10x\) [\#709](https://github.com/jorgecarleitao/arrow2/pull/709) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made Avro read API use `Block` and `CompressedBlock` [\#698](https://github.com/jorgecarleitao/arrow2/pull/698) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified most traits [\#696](https://github.com/jorgecarleitao/arrow2/pull/696) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced `Display` by `Debug` for `Array` [\#694](https://github.com/jorgecarleitao/arrow2/pull/694) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced `MutableBuffer` by `std::Vec` [\#693](https://github.com/jorgecarleitao/arrow2/pull/693) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified `Utf8Scalar` and `BinaryScalar` [\#660](https://github.com/jorgecarleitao/arrow2/pull/660) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified Primitive and Boolean scalar [\#648](https://github.com/jorgecarleitao/arrow2/pull/648) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Add `and_scalar` and `or_scalar` for boolean\_kleene [\#662](https://github.com/jorgecarleitao/arrow2/issues/662)
- Add `lower` and `upper` support for string [\#635](https://github.com/jorgecarleitao/arrow2/issues/635)
- Added support to cast decimal [\#761](https://github.com/jorgecarleitao/arrow2/pull/761) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to deserialize JSON \(!= NDJSON\) [\#758](https://github.com/jorgecarleitao/arrow2/pull/758) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to infer nested json structs [\#750](https://github.com/jorgecarleitao/arrow2/pull/750) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to compare intervals [\#746](https://github.com/jorgecarleitao/arrow2/pull/746) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `any` and `all` kernel [\#739](https://github.com/jorgecarleitao/arrow2/pull/739) ([ritchie46](https://github.com/ritchie46))
- Added support to write Avro async [\#736](https://github.com/jorgecarleitao/arrow2/pull/736) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write interval to Avro [\#734](https://github.com/jorgecarleitao/arrow2/pull/734) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `and_scalar` and `or_scalar` for boolean kleene [\#723](https://github.com/jorgecarleitao/arrow2/pull/723) ([silathdiir](https://github.com/silathdiir))
- Added `and_scalar` and `or_scalar` for boolean [\#707](https://github.com/jorgecarleitao/arrow2/pull/707) ([silathdiir](https://github.com/silathdiir))
- Refactored JSON read to split IO-bounded from CPU-bounded tasks [\#706](https://github.com/jorgecarleitao/arrow2/pull/706) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more conversions from parquet [\#701](https://github.com/jorgecarleitao/arrow2/pull/701) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for compressed Avro write [\#699](https://github.com/jorgecarleitao/arrow2/pull/699) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write to Avro [\#690](https://github.com/jorgecarleitao/arrow2/pull/690) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added dynamic version of negation [\#685](https://github.com/jorgecarleitao/arrow2/pull/685) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read dictionary-encoded required parquet pages [\#683](https://github.com/jorgecarleitao/arrow2/pull/683) ([mdrach](https://github.com/mdrach))
- Added `upper` [\#664](https://github.com/jorgecarleitao/arrow2/pull/664) ([Xuanwo](https://github.com/Xuanwo))
- Added `lower` [\#641](https://github.com/jorgecarleitao/arrow2/pull/641) ([Xuanwo](https://github.com/Xuanwo))
- Added support for `async` read of Avro [\#620](https://github.com/jorgecarleitao/arrow2/pull/620) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Pyarrow and Arrow2 don't agree on Timestamp resolution [\#700](https://github.com/jorgecarleitao/arrow2/issues/700)
- Writing compressed dictionary in parquet corrupts the files [\#667](https://github.com/jorgecarleitao/arrow2/issues/667)
- Replaced assert by error in IPC read [\#748](https://github.com/jorgecarleitao/arrow2/pull/748) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made all panics in IPC read errors [\#722](https://github.com/jorgecarleitao/arrow2/pull/722) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in compare booleans [\#721](https://github.com/jorgecarleitao/arrow2/pull/721) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in dispatching scalar arithmetics [\#682](https://github.com/jorgecarleitao/arrow2/pull/682) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in reading negative decimals from parquet [\#679](https://github.com/jorgecarleitao/arrow2/pull/679) ([mdrach](https://github.com/mdrach))
- Made IPC reader less restrictive [\#678](https://github.com/jorgecarleitao/arrow2/pull/678) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in trait constraint in compute [\#665](https://github.com/jorgecarleitao/arrow2/pull/665) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed performance regression of CSV reading [\#657](https://github.com/jorgecarleitao/arrow2/pull/657) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed filter of predicate with validity [\#653](https://github.com/jorgecarleitao/arrow2/pull/653) ([ritchie46](https://github.com/ritchie46))
- Made `Scalar: Send+Sync` [\#644](https://github.com/jorgecarleitao/arrow2/pull/644) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Feature: JSON IO? [\#712](https://github.com/jorgecarleitao/arrow2/issues/712)
- Simplified code [\#760](https://github.com/jorgecarleitao/arrow2/pull/760) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added iterator of values of `FixedBinaryArray` [\#757](https://github.com/jorgecarleitao/arrow2/pull/757) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Remove un-needed `unsafe` [\#756](https://github.com/jorgecarleitao/arrow2/pull/756) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Replaced un-needed `unsafe` [\#755](https://github.com/jorgecarleitao/arrow2/pull/755) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made IO `#[forbid(unsafe)]` [\#749](https://github.com/jorgecarleitao/arrow2/pull/749) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved reading nullable Avro arrays [\#727](https://github.com/jorgecarleitao/arrow2/pull/727) ([Igosuki](https://github.com/Igosuki))
- Allow to create primitive array by vec without extra memcopy [\#710](https://github.com/jorgecarleitao/arrow2/pull/710) ([sundy-li](https://github.com/sundy-li))
- Removed requirement of `use Array` to access primitives' `data_type` [\#697](https://github.com/jorgecarleitao/arrow2/pull/697) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Cleaned up trait usage and added forbid\_unsafe to parts [\#695](https://github.com/jorgecarleitao/arrow2/pull/695) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Migrated from `avro-rs` to `avro-schema` [\#692](https://github.com/jorgecarleitao/arrow2/pull/692) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `MutablePrimitiveArray::extend_constant` [\#689](https://github.com/jorgecarleitao/arrow2/pull/689) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Do not write validity without nulls in IPC [\#688](https://github.com/jorgecarleitao/arrow2/pull/688) ([jorgecarleitao](https://github.com/jorgecarleitao))
- DRY code via macro [\#681](https://github.com/jorgecarleitao/arrow2/pull/681) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `dyn Array` and `Scalar` usable in `#[derive(PartialEq)]` [\#680](https://github.com/jorgecarleitao/arrow2/pull/680) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made IPC ZSTD-compressed consumable by pyarrow [\#675](https://github.com/jorgecarleitao/arrow2/pull/675) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified trait bounds in arithmetics [\#671](https://github.com/jorgecarleitao/arrow2/pull/671) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of reading utf8 required from parquet \(-15%\) [\#670](https://github.com/jorgecarleitao/arrow2/pull/670) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Avoid double utf8 checks on MutableUtf8 -\> Utf8 [\#655](https://github.com/jorgecarleitao/arrow2/pull/655) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `Buffer::offset` public [\#652](https://github.com/jorgecarleitao/arrow2/pull/652) ([ritchie46](https://github.com/ritchie46))
- Improved performance in cast Primitive to Binary/String \(2x\) [\#646](https://github.com/jorgecarleitao/arrow2/pull/646) ([sundy-li](https://github.com/sundy-li))
- Made `Filter: Send+Sync` [\#645](https://github.com/jorgecarleitao/arrow2/pull/645) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made API to create field accept `String` [\#643](https://github.com/jorgecarleitao/arrow2/pull/643) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Fixed clippy \(coming from 1.58\) [\#763](https://github.com/jorgecarleitao/arrow2/pull/763) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Described how to run part of the tests [\#762](https://github.com/jorgecarleitao/arrow2/pull/762) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved README [\#735](https://github.com/jorgecarleitao/arrow2/pull/735) ([jorgecarleitao](https://github.com/jorgecarleitao))
- clarify boolean value in DataType::Dictionary [\#718](https://github.com/jorgecarleitao/arrow2/pull/718) ([ritchie46](https://github.com/ritchie46))
- readme typo [\#687](https://github.com/jorgecarleitao/arrow2/pull/687) ([max-sixty](https://github.com/max-sixty))
- Added example to read parquet in parallel with rayon [\#658](https://github.com/jorgecarleitao/arrow2/pull/658) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added documentation to `Bitmap::as_slice` [\#654](https://github.com/jorgecarleitao/arrow2/pull/654) ([ritchie46](https://github.com/ritchie46))

**Testing updates:**

- Improved json tests [\#742](https://github.com/jorgecarleitao/arrow2/pull/742) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added integration tests for writing compressed parquet [\#740](https://github.com/jorgecarleitao/arrow2/pull/740) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Updated patch for integration test [\#731](https://github.com/jorgecarleitao/arrow2/pull/731) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added cargo check to benchmarks [\#730](https://github.com/jorgecarleitao/arrow2/pull/730) ([sundy-li](https://github.com/sundy-li))
- More tests to CSV writing [\#724](https://github.com/jorgecarleitao/arrow2/pull/724) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added integration tests for other compressions with parquet from pyarrow [\#674](https://github.com/jorgecarleitao/arrow2/pull/674) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Bumped nightly in CI [\#672](https://github.com/jorgecarleitao/arrow2/pull/672) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Invalidate caches from CI. [\#656](https://github.com/jorgecarleitao/arrow2/pull/656) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.8.1](https://github.com/jorgecarleitao/arrow2/tree/v0.8.1) (2021-11-27)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.8.0...v0.8.1)

**Fixed bugs:**

- Fixed compilation with individual features activated [\#642](https://github.com/jorgecarleitao/arrow2/pull/642) ([ritchie46](https://github.com/ritchie46))

## [v0.8.0](https://github.com/jorgecarleitao/arrow2/tree/v0.8.0) (2021-11-27)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.7.0...v0.8.0)

**Breaking changes:**

- Made CSV write options use chrono formatting by default [\#624](https://github.com/jorgecarleitao/arrow2/issues/624)
- Add `compression` to `IpcWriteOptions` [\#570](https://github.com/jorgecarleitao/arrow2/issues/570)
- Made `cast` accept `CastOptions` parameter [\#569](https://github.com/jorgecarleitao/arrow2/issues/569)
- Simplified `ArrowError` [\#640](https://github.com/jorgecarleitao/arrow2/pull/640) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Use `DynComparator` for `lexsort` and `partition` [\#637](https://github.com/jorgecarleitao/arrow2/pull/637) ([yjshen](https://github.com/yjshen))
- Split "compute" feature [\#634](https://github.com/jorgecarleitao/arrow2/pull/634) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed unneeded trait. [\#628](https://github.com/jorgecarleitao/arrow2/pull/628) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Sealed 2 traits to forbid downstream implementations [\#621](https://github.com/jorgecarleitao/arrow2/pull/621) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified arithmetics compute [\#607](https://github.com/jorgecarleitao/arrow2/pull/607) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Refactored comparison `Operator` [\#604](https://github.com/jorgecarleitao/arrow2/pull/604) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified dictionary indexes [\#584](https://github.com/jorgecarleitao/arrow2/pull/584) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified IPC APIs [\#576](https://github.com/jorgecarleitao/arrow2/pull/576) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified IPC stream writer / remove finish on drop from stream writer [\#575](https://github.com/jorgecarleitao/arrow2/pull/575) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified trait in compute. [\#572](https://github.com/jorgecarleitao/arrow2/pull/572) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Compute: add partial option into CastOptions [\#561](https://github.com/jorgecarleitao/arrow2/pull/561) ([sundy-li](https://github.com/sundy-li))
- Introduced `UnionMode` enum [\#557](https://github.com/jorgecarleitao/arrow2/pull/557) ([simonvandel](https://github.com/simonvandel))
- Changed DataType::FixedSize\*\(i32\) to DataType::FixedSize\*\(usize\) [\#556](https://github.com/jorgecarleitao/arrow2/pull/556) ([simonvandel](https://github.com/simonvandel))

**New features:**

- Added support to write timestamps with timezones for CSV [\#623](https://github.com/jorgecarleitao/arrow2/pull/623) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read Avro files' metadata asynchronously [\#614](https://github.com/jorgecarleitao/arrow2/pull/614) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added iterator for `StructArray` [\#613](https://github.com/jorgecarleitao/arrow2/pull/613) ([illumination-k](https://github.com/illumination-k))
- Added support to read snappy-compressed Avro [\#612](https://github.com/jorgecarleitao/arrow2/pull/612) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to read decimal from csv [\#602](https://github.com/jorgecarleitao/arrow2/pull/602) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to cast `NullArray` to all other types [\#589](https://github.com/jorgecarleitao/arrow2/pull/589) ([flaneur2020](https://github.com/flaneur2020))
- Added support dictionaries in nested types over IPC [\#587](https://github.com/jorgecarleitao/arrow2/pull/587) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write Arrow IPC streams asynchronously [\#577](https://github.com/jorgecarleitao/arrow2/pull/577) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support to write compressed Arrow IPC \(feather v2\) [\#566](https://github.com/jorgecarleitao/arrow2/pull/566) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for ffi for `FixedSizeList` and `FixedSizeBinary` [\#565](https://github.com/jorgecarleitao/arrow2/pull/565) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `async` csv reading. [\#562](https://github.com/jorgecarleitao/arrow2/pull/562) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `bitwise` operations [\#553](https://github.com/jorgecarleitao/arrow2/pull/553) ([1aguna](https://github.com/1aguna))
- Added support to read `StructArray` from parquet [\#547](https://github.com/jorgecarleitao/arrow2/pull/547) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Fixed bugs:**

- Fixed error in reading nullable from Avro. [\#631](https://github.com/jorgecarleitao/arrow2/pull/631) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in union FFI [\#625](https://github.com/jorgecarleitao/arrow2/pull/625) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in computing projection in `io::ipc::read::reader::FileReader` [\#596](https://github.com/jorgecarleitao/arrow2/pull/596) ([illumination-k](https://github.com/illumination-k))
- Fixed error in compressing IPC LZ4 [\#593](https://github.com/jorgecarleitao/arrow2/pull/593) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed growable of dictionaries negative keys [\#582](https://github.com/jorgecarleitao/arrow2/pull/582) ([ritchie46](https://github.com/ritchie46))
- Made substring kernel on utf8 take chars into account. [\#568](https://github.com/jorgecarleitao/arrow2/pull/568) ([ritchie46](https://github.com/ritchie46))
- Fixed error in passing sliced arrays via FFI [\#564](https://github.com/jorgecarleitao/arrow2/pull/564) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Enhancements:**

- Faster `take` with null values \(2-3x\) [\#633](https://github.com/jorgecarleitao/arrow2/pull/633) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved error message for missing feature in compressed parquet [\#632](https://github.com/jorgecarleitao/arrow2/pull/632) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `to` conversion to `FixedSizeBinary` [\#622](https://github.com/jorgecarleitao/arrow2/pull/622) ([ritchie46](https://github.com/ritchie46))
- Bumped `confy-table` [\#618](https://github.com/jorgecarleitao/arrow2/pull/618) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made `MutableArray` `Send + Sync` [\#617](https://github.com/jorgecarleitao/arrow2/pull/617) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Removed most of allocations in IPC reading [\#611](https://github.com/jorgecarleitao/arrow2/pull/611) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Speed up boolean comparison kernels \(~3x\) [\#610](https://github.com/jorgecarleitao/arrow2/pull/610) ([Dandandan](https://github.com/Dandandan))
- Improved performance of decimal arithmetics [\#605](https://github.com/jorgecarleitao/arrow2/pull/605) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Simplified traits and added documentation [\#603](https://github.com/jorgecarleitao/arrow2/pull/603) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved performance of `is_not_null`. [\#600](https://github.com/jorgecarleitao/arrow2/pull/600) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added `len` to every array [\#599](https://github.com/jorgecarleitao/arrow2/pull/599) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added support for `NullArray` at FFI. [\#598](https://github.com/jorgecarleitao/arrow2/pull/598) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Optimized `MutableBinaryArray` [\#597](https://github.com/jorgecarleitao/arrow2/pull/597) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Speedup/simplify bitwise operations \(avoid extra allocation\) [\#586](https://github.com/jorgecarleitao/arrow2/pull/586) ([Dandandan](https://github.com/Dandandan))
- Improved performance of `bitmap::from_trusted` \(3x\) [\#578](https://github.com/jorgecarleitao/arrow2/pull/578) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made bitmap not cache null count [\#563](https://github.com/jorgecarleitao/arrow2/pull/563) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Avoided redundant checks in creating an `Utf8Array` from `MutableUtf8Array` [\#560](https://github.com/jorgecarleitao/arrow2/pull/560) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Avoid unnecessary allocations [\#559](https://github.com/jorgecarleitao/arrow2/pull/559) ([simonvandel](https://github.com/simonvandel))
- Surfaced errors in reading from avro [\#558](https://github.com/jorgecarleitao/arrow2/pull/558) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Documentation updates:**

- Simplified example [\#619](https://github.com/jorgecarleitao/arrow2/pull/619) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Made example of parallel parquet write be over multiple batches [\#544](https://github.com/jorgecarleitao/arrow2/pull/544) ([jorgecarleitao](https://github.com/jorgecarleitao))

**Testing updates:**

- Cleaned up benches [\#636](https://github.com/jorgecarleitao/arrow2/pull/636) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Ignored tests code in coverage report [\#615](https://github.com/jorgecarleitao/arrow2/pull/615) ([yjhmelody](https://github.com/yjhmelody))
- Added more tests [\#601](https://github.com/jorgecarleitao/arrow2/pull/601) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Mitigated `RUSTSEC-2020-0159` [\#595](https://github.com/jorgecarleitao/arrow2/pull/595) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added more tests [\#591](https://github.com/jorgecarleitao/arrow2/pull/591) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [v0.7.0](https://github.com/jorgecarleitao/arrow2/tree/v0.7.0) (2021-10-29)

[Full Changelog](https://github.com/jorgecarleitao/arrow2/compare/v0.6.2...v0.7.0)

**Breaking changes:**

- Simplified reading parquet [\#532](https://github.com/jorgecarleitao/arrow2/pull/532) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Change IPC `FileReader` to own the underlying reader [\#518](https://github.com/jorgecarleitao/arrow2/pull/518) ([blakesmith](https://github.com/blakesmith))
- Migrate to `arrow_format` crate [\#517](https://github.com/jorgecarleitao/arrow2/pull/517) ([jorgecarleitao](https://github.com/jorgecarleitao))

**New features:**

- Added read of 2-level nested lists from parquet [\#548](https://github.com/jorgecarleitao/arrow2/pull/548) ([jorgecarleitao](https://github.com/jorgecarleitao))
- add dictionary serialization for csv-writer [\#515](https://github.com/jorgecarleitao/arrow2/pull/515) ([ritchie46](https://github.com/ritchie46))
- Added `checked_negate` and `wrapping_negate` for `PrimitiveArray` [\#506](https://github.com/jorgecarleitao/arrow2/pull/506) ([yjhmelody](https://github.com/yjhmelody))

**Fixed bugs:**

- Fixed error in reading fixed len binary from parquet [\#549](https://github.com/jorgecarleitao/arrow2/pull/549) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed ffi of sliced arrays [\#540](https://github.com/jorgecarleitao/arrow2/pull/540) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed s3 example [\#536](https://github.com/jorgecarleitao/arrow2/pull/536) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Fixed error in writing compressed parquet dict pages [\#523](https://github.com/jorgecarleitao/arrow2/pull/523) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Validity taken into account when writing `StructArray` to json [\#511](https://github.com/jorgecarleitao/arrow2/pull/511) ([VasanthakumarV](https://github.com/VasanthakumarV))

**Enhancements:**

- Bumped Prost and Tonic [\#550](https://github.com/jorgecarleitao/arrow2/pull/550) ([PsiACE](https://github.com/PsiACE))
- Speedup scalar boolean operations [\#546](https://github.com/jorgecarleitao/arrow2/pull/546) ([Dandandan](https://github.com/Dandandan))
- Added fast path for validating ASCII text \(~1.12-1.89x improvement on reading ASCII parquet data\) [\#542](https://github.com/jorgecarleitao/arrow2/pull/542) ([Dandandan](https://github.com/Dandandan))
- Exposed missing APIs to write parquet in parallel [\#539](https://github.com/jorgecarleitao/arrow2/pull/539) ([jorgecarleitao](https://github.com/jorgecarleitao))
- improve utf8 init validity [\#530](https://github.com/jorgecarleitao/arrow2/pull/530) ([ritchie46](https://github.com/ritchie46))
- export missing `BinaryValueIter` [\#526](https://github.com/jorgecarleitao/arrow2/pull/526) ([yjhmelody](https://github.com/yjhmelody))

**Documentation updates:**

- Added more IPC documentation [\#534](https://github.com/jorgecarleitao/arrow2/pull/534) ([HagaiHargil](https://github.com/HagaiHargil))
- Fixed clippy and fmt [\#521](https://github.com/jorgecarleitao/arrow2/pull/521) ([ritchie46](https://github.com/ritchie46))

**Testing updates:**

- Added more tests for `utf8` [\#543](https://github.com/jorgecarleitao/arrow2/pull/543) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Ignored RUSTSEC-2020-0071 and RUSTSEC-2020-0159 [\#537](https://github.com/jorgecarleitao/arrow2/pull/537) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Improved parquet read benches [\#533](https://github.com/jorgecarleitao/arrow2/pull/533) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added fmt and clippy checks to CI. [\#522](https://github.com/jorgecarleitao/arrow2/pull/522) ([xudong963](https://github.com/xudong963))

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
