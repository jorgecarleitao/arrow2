//! Example showing how to memory map an Arrow IPC file into a [`Chunk`].
use std::sync::Arc;

use arrow2::error::Result;
use arrow2::io::ipc::read;
use arrow2::mmap::mmap_unchecked;

// Arrow2 requires a struct that implements `Clone + AsRef<[u8]>`, which
// usually `Arc<Mmap>` supports. This is how it could look like
#[derive(Clone)]
struct Mmap(Arc<Vec<u8>>);

impl AsRef<[u8]> for Mmap {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

fn main() -> Result<()> {
    // given a mmap
    let mmap = Mmap(Arc::new(vec![]));

    // we read the metadata
    let metadata = read::read_file_metadata(&mut std::io::Cursor::new(mmap.as_ref()))?;

    let chunk = unsafe { mmap_unchecked(&metadata, mmap, 0) }?;

    println!("{chunk:?}");
    Ok(())
}
