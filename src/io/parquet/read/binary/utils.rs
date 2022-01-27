use crate::{array::Offset, io::parquet::read::utils::Pushable};

/// [`Pushable`] for variable length binary data.
#[derive(Debug)]
pub struct Binary<O: Offset> {
    pub offsets: Offsets<O>,
    pub values: Vec<u8>,
    pub last_offset: O,
}

#[derive(Debug)]
pub struct Offsets<O: Offset>(pub Vec<O>);

impl<O: Offset> Pushable<O> for Offsets<O> {
    #[inline]
    fn len(&self) -> usize {
        self.0.len() - 1
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional)
    }

    #[inline]
    fn push(&mut self, value: O) {
        self.0.push(value)
    }

    #[inline]
    fn push_null(&mut self) {
        self.0.push(*self.0.last().unwrap())
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: O) {
        self.0.extend_constant(additional, value)
    }
}

impl<O: Offset> Binary<O> {
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(1 + capacity);
        offsets.push(O::default());
        Self {
            offsets: Offsets(offsets),
            values: vec![],
            last_offset: O::default(),
        }
    }

    #[inline]
    pub fn push(&mut self, v: &[u8]) {
        self.values.extend(v);
        self.last_offset += O::from_usize(v.len()).unwrap();
        self.offsets.push(self.last_offset)
    }

    #[inline]
    pub fn extend_constant(&mut self, additional: usize) {
        self.offsets
            .0
            .resize(self.offsets.len() + additional, self.last_offset);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }
}

impl<'a, O: Offset> Pushable<&'a [u8]> for Binary<O> {
    #[inline]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional)
    }

    #[inline]
    fn push_null(&mut self) {
        self.push(&[])
    }

    #[inline]
    fn push(&mut self, value: &[u8]) {
        self.push(value)
    }

    #[inline]
    fn extend_constant(&mut self, additional: usize, value: &[u8]) {
        assert_eq!(value.len(), 0);
        self.extend_constant(additional)
    }
}
