# Read Arrow IPC

When compiled with feature `io_ipc`, this crate can be used to read Arrow IPC files.

An Arrow IPC file is composed by a header, a footer, and blocks of `RecordBatch`es.
Reading it generally consists of:

1. read metadata, containing the block positions in the file
2. seek to each block and read it

```rust
{{#include ../../../examples/ipc_file_read.rs}}
```
