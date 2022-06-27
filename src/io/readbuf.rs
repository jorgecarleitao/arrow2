/// A small wrapper around [`Vec<u8>`] that allows us to reuse memory once it is initialized.
/// This may improve performance of the [`Read`] trait.
#[derive(Clone, Default)]
pub struct ReadBuffer {
    data: Vec<u8>,
    // length to be read or is read
    length: usize,
}

impl ReadBuffer {
    /// Set the minimal length of the [`ReadBuf`]. Contrary to the
    /// method on `Vec` this is `safe` because this function guarantees that
    /// the underlying data always is initialized.
    pub fn set_len(&mut self, length: usize) {
        if length > self.data.capacity() {
            // exponential growing strategy
            // benchmark showed it was ~5% faster
            // in reading lz4 yellow-trip dataset
            self.data = vec![0; length * 2];
        } else if length > self.data.len() {
            self.data.resize(length, 0);
        }
        self.length = length;
    }
}

impl AsRef<[u8]> for ReadBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.data[..self.length]
    }
}

impl AsMut<[u8]> for ReadBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data[..self.length]
    }
}

impl From<Vec<u8>> for ReadBuffer {
    fn from(data: Vec<u8>) -> Self {
        let length = data.len();
        Self { data, length }
    }
}

impl From<ReadBuffer> for Vec<u8> {
    fn from(buf: ReadBuffer) -> Self {
        buf.data
    }
}
