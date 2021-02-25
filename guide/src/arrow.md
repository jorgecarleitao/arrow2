# Arrow

[Arrow](https://arrow.apache.org/) as in-memory columnar format for modern analytics that natively supports the concept of a null value, nested structs, and many others.

It has an IPC protocol (based on flat buffers) and a stable C-represented ABI for intra-process communication via foreign interfaces (FFI).

The arrow format recommends allocating memory along cache lines to leverage modern archtectures,
as well as using a shared memory model that enables multi-threading.
