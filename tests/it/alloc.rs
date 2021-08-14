use arrow2::alloc::*;

#[test]
fn allocate_dangling() {
    let p = allocate_aligned::<u32>(0);
    assert_eq!(0, (p.as_ptr() as usize) % ALIGNMENT);
}

#[test]
fn allocate() {
    let p = allocate_aligned::<u32>(1024);
    assert_eq!(0, (p.as_ptr() as usize) % ALIGNMENT);
    unsafe { free_aligned(p, 1024) };
}

#[test]
fn allocate_zeroed() {
    let p = allocate_aligned_zeroed::<u32>(1024);
    assert_eq!(0, (p.as_ptr() as usize) % ALIGNMENT);
    unsafe { free_aligned(p, 1024) };
}

#[test]
fn reallocate_from_zero() {
    let ptr = allocate_aligned::<u32>(0);
    let ptr = unsafe { reallocate(ptr, 0, 512) };
    unsafe { free_aligned(ptr, 512) };
}

#[test]
fn reallocate_from_alloc() {
    let ptr = allocate_aligned::<u32>(32);
    let ptr = unsafe { reallocate(ptr, 32, 64) };
    unsafe { free_aligned(ptr, 64) };
}

#[test]
fn reallocate_smaller() {
    let ptr = allocate_aligned::<u32>(32);
    let ptr = unsafe { reallocate(ptr, 32, 16) };
    unsafe { free_aligned(ptr, 16) };
}

#[test]
fn reallocate_to_zero() {
    let ptr = allocate_aligned::<u32>(32);
    let ptr = unsafe { reallocate(ptr, 32, 0) };
    assert_eq!(ptr, unsafe { dangling() });
}
