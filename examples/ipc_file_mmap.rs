//! Example showing how to memory map an Arrow IPC file into a [`Chunk`].
use std::sync::Arc;

use arrow2::error::Result;
use arrow2::io::ipc::read;
use arrow2::mmap::{mmap_dictionaries_unchecked, mmap_unchecked};

// Arrow2 requires a struct that implements `Clone + AsRef<[u8]>`, which
// usually `Arc<Mmap>` supports. Here we mock it
#[derive(Clone)]
struct Mmap(Vec<u8>);

impl AsRef<[u8]> for Mmap {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

fn main() -> Result<()> {
    // given a mmap
    let mmap = Arc::new(Mmap(vec![]));

    // read the metadata
    let metadata = read::read_file_metadata(&mut std::io::Cursor::new(mmap.as_ref()))?;

    // mmap the dictionaries
    let dictionaries = unsafe { mmap_dictionaries_unchecked(&metadata, mmap.clone())? };

    // and finally mmap a chunk (0 in this case).
    let chunk = unsafe { mmap_unchecked(&metadata, &dictionaries, mmap, 0) }?;

    println!("{chunk:?}");
    Ok(())
}
