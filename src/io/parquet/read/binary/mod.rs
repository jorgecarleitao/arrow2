mod basic;
mod dictionary;
mod nested;

pub use basic::iter_to_array;
pub use dictionary::iter_to_array as iter_to_dict_array;
pub use basic::stream_to_array;
pub use nested::iter_to_array as iter_to_array_nested;
