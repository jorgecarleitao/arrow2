//! Contains modules to interface with other formats such as [`csv`],
//! [`parquet`], [`json`], [`ipc`], [`mod@print`] and [`avro`].
#[cfg(any(feature = "io_csv_read", feature = "io_csv_write"))]
#[cfg_attr(docsrs, doc(cfg(feature = "io_csv")))]
pub mod csv;

#[cfg(feature = "io_json")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_json")))]
pub mod json;

#[cfg(feature = "io_ipc")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_ipc")))]
pub mod ipc;

#[cfg(feature = "io_flight")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_flight")))]
pub mod flight;

#[cfg(feature = "io_json_integration")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_json_integration")))]
pub mod json_integration;

#[cfg(feature = "io_parquet")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_parquet")))]
pub mod parquet;

#[cfg(feature = "io_avro")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_avro")))]
pub mod avro;

#[cfg(feature = "io_print")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_print")))]
pub mod print;
