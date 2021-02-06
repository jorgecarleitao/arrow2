pub unsafe trait NativeType: Sized + 'static {}

unsafe impl NativeType for u8 {}
unsafe impl NativeType for u16 {}
unsafe impl NativeType for u32 {}
unsafe impl NativeType for u64 {}
unsafe impl NativeType for i8 {}
unsafe impl NativeType for i16 {}
unsafe impl NativeType for i32 {}
unsafe impl NativeType for i64 {}
unsafe impl NativeType for f32 {}
unsafe impl NativeType for f64 {}
