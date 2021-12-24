# JSON read

When compiled with feature `io_json`, you can use this crate to read JSON files.

```rust
{{#include ../../../examples/json_read.rs}}
```

Note how deserialization can be performed on a separate thread pool to avoid
blocking the runtime (see also [here](https://ryhl.io/blog/async-what-is-blocking/)).
