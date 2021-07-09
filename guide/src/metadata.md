# Metadata

```rust
{{#include ../../examples/metadata.rs}}
```

## `DataType` (Logical types)

The Arrow specification contains a set of logical types, an enumeration of the different
semantical types defined in Arrow.

In Arrow2, logical types are declared as variants of the `enum` `arrow2::datatypes::DataType`.
For example, `DataType::Int32` represents a signed integer of 32 bits.

Each logical type has an associated in-memory physical representation and is associated to specific
semantics. For example, `Date32` has the same in-memory representation as `Int32`, but the value
represents the number of days since UNIX epoch.

Logical types are metadata: they annotate arrays with extra information about in-memory data.

## `Field` (column metadata)

Besides logical types, the arrow format supports other relevant metadata to the format. All this 
information is stored in `arrow2::datatypes::Field`.

A `Field` is arrow's metadata associated to a column in the context of a columnar format. 
It has a name, a logical type `DataType`, whether the column is nullable, etc.

## `Schema` (table metadata)

The most common use of `Field` is to declare a `arrow2::datatypes::Schema`, a sequence of `Field`s
with optional metadata.

`Schema` is essentially metadata of a "table": it has a sequence of named columns and their metadata (`Field`s) with optional metadata.
