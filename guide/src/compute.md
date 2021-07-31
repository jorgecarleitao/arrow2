# Compute API

When compiled with the feature `compute`, this crate offers a wide range of functions to perform both vertical (e.g. add two arrays) and horizontal (compute the sum of an array) operations.

```rust
{{#include ../../examples/arithmetics.rs}}
```

An overview of the implemented functionality.

* arithmetics, checked, saturating, etc.
* `sum`, `min` and `max`
* `unary`, `binary`, etc.
* `comparison`
* `cast`
* `take`, `filter`, `concat`
* `sort`, `hash`, `merge-sort`
* `if-then-else`
* `nullif`
* `lenght` (of string)
* `hour`, `year` (of temporal logical types)
* `regex`
* (list) `contains`
