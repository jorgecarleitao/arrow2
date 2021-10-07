## Observations

### LSB equivalence between definition levels and bitmaps

* When the maximum repetition level is 0 and the maximum definition level is 1,
  the RLE-encoded definition levels correspond exactly to Arrow's bitmap and can be
  memcopied without further transformations.

* Reading a parquet nested field can be done by reading each individual primitive
  column and "building" the nested struct in arrow.

## Nested parquet groups are deserialized recursively

Rows of nested parquet groups are encoded in the repetition and definition levels.
In arrow, they correspond to:
* list's offsets and validity
* struct's validity

An implementation that leverages this observation:

When nested types are observed in a parquet column, we recurse over the struct to gather
whether the type is a Struct or List and whether it is required or optional, which we store
in a `Vec<Nested>`. `Nested` is an enum that can process definition and repetition
levels depending on the type and nullability.

When processing pages, we process the definition and repetition levels into `Vec`.

When we finish a column chunk, we recursively pop `Vec` as we are building the `StructArray`
or `ListArray`.

With this approach, the only difference vs flat is that we cannot leverage the bitmap
optimization, and instead need to deserialize the repetition and definition
levels to `i32`.
