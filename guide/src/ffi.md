# Foreign Interfaces

One of the hallmarks of the Arrow format is that its in-memory representation
has a specification, which allows languages to share data
structures via foreign interfaces at zero cost (i.e. via pointers).
This is known as the [C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html).

This crate supports importing from and exporting to all `DataType`s. Follow the
example below to learn how to import and export:

```rust
{{#include ../../examples/ffi.rs}}
```
