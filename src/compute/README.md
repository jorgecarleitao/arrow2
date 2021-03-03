# Design

This document outlines the design guide lines of this module.

Generally, this module is composed by independent "kernels", or logical operations common
in analytics. Below are some design principles:

* APIs MUST return an error when either:
    * The arguments are incorrect
    * The execution results in a predictable error

* APIs MAY error when an operation overflows (e.g. `i32 + i32`)

* APIs SHOULD error when an operation on variable sized containers overflows the maximum offset size.

* Kernels SHOULD use the arrays' logical type to decide whether kernels
can be applied on an array. For example, `Date32 + Date32` is meaningless and SHOULD NOT be implemented.

* Kernels SHOULD be implemented via `clone`, `slice` or the `iterator` API provided by `Buffer`, `Bitmap`, `MutableBuffer` or `MutableBitmap`.

* Kernels SHOULD NOT use any API to read bits other than the ones provided by `Bitmap`.

* Implementations SHOULD aim for auto-vectorization, which is usually accomplished via `from_trusted_len_iter`.

* Implementations MUST feature-gate any implementation that requires external dependencies

* When a kernel accepts dynamically-typed arrays, it MUST expect them as `&dyn Array`.

* When an API returns `&dyn Array`, it MUST return `Box<dyn Array>`. The rational is that a `Box` is mutable, while an `Arc` is not. As such, `Box` offers the most flexible API to consumers and the compiler. Users can cast a `Box` into `Arc` via `.into()`.
