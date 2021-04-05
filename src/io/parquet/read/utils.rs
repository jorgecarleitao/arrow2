use parquet2::encoding::get_length;

pub struct BinaryIter<'a> {
    values: &'a [u8],
}

impl<'a> BinaryIter<'a> {
    pub fn new(values: &'a [u8]) -> Self {
        Self { values }
    }
}

impl<'a> Iterator for BinaryIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.values.is_empty() {
            return None;
        }
        let length = get_length(self.values) as usize;
        self.values = &self.values[4..];
        let result = &self.values[4..length as usize];
        self.values = &self.values[4 + length..];
        Some(result)
    }
}

#[inline]
pub fn split_buffer_v1(buffer: &[u8]) -> (&[u8], &[u8]) {
    let def_level_buffer_length = get_length(&buffer) as usize;
    (
        &buffer[4..4 + def_level_buffer_length],
        &buffer[4 + def_level_buffer_length..],
    )
}

pub fn split_buffer_v2(buffer: &[u8], def_level_buffer_length: usize) -> (&[u8], &[u8]) {
    (
        &buffer[..def_level_buffer_length],
        &buffer[def_level_buffer_length..],
    )
}
