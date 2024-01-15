#[cfg(feature = "io_print")]
mod print;

#[cfg(feature = "io_json")]
mod json;

#[cfg(feature = "io_json")]
mod ndjson;

// #[cfg(feature = "io_json_integration")] // disabled: requiers test data
// mod ipc; // disabled: requiers test data

#[cfg(feature = "io_parquet")]
mod parquet;

#[cfg(feature = "io_avro")]
mod avro;

// #[cfg(feature = "io_orc")] // disabled: requiers test data
// mod orc; // disabled: requiers test data

#[cfg(any(
    feature = "io_csv_read",
    feature = "io_csv_write",
    feature = "io_csv_read_async"
))]
mod csv;

// #[cfg(feature = "io_flight")] // disabled: requiers test data
// mod flight; // disabled: requiers test data
