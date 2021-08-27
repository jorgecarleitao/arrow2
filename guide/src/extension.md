# Extension types

This crate supports Arrows' ["extension type"](https://arrow.apache.org/docs/format/Columnar.html#extension-types),
to declare, use, and share custom logical types. The follow example shows how
to declare one:

```rust
{{#include ../../../examples/extension.rs}}
```
