//! Defines basic arithmetic kernels for [`PrimitiveArray`](crate::array::PrimitiveArray)s.
//!
//! The Arithmetics module is composed by basic arithmetics operations that can
//! be performed on [`PrimitiveArray`](crate::array::PrimitiveArray).
//!
//! Whenever possible, each operation declares variations
//! of the basic operation that offers different guarantees:
//! * plain: panics on overflowing and underflowing.
//! * checked: turns an overflowing to a null.
//! * saturating: turns the overflowing to the MAX or MIN value respectively.
//! * overflowing: returns an extra [`Bitmap`] denoting whether the operation overflowed.
//! * adaptive: for [`Decimal`](crate::datatypes::DataType::Decimal) only,
//!   adjusts the precision and scale to make the resulting value fit.

pub mod basic;
pub mod decimal;
pub mod time;

use crate::{
    array::Array,
    bitmap::Bitmap,
    datatypes::{DataType, IntervalUnit, TimeUnit},
    scalar::Scalar,
    types::NativeType,
};

// Macro to evaluate match branch in arithmetic function.
macro_rules! primitive {
    ($lhs:expr, $rhs:expr, $op:tt, $type:ty) => {{
        let lhs = $lhs.as_any().downcast_ref().unwrap();
        let rhs = $rhs.as_any().downcast_ref().unwrap();

        let result = basic::$op::<$type>(lhs, rhs);
        Box::new(result) as Box<dyn Array>
    }};
}

// Macro to create a `match` statement with dynamic dispatch to functions based on
// the array's logical types
macro_rules! arith {
    ($lhs:expr, $rhs:expr, $op:tt $(, decimal = $op_decimal:tt )? $(, duration = $op_duration:tt )? $(, interval = $op_interval:tt )? $(, timestamp = $op_timestamp:tt )?) => {{
        let lhs = $lhs;
        let rhs = $rhs;
        use DataType::*;
        match (lhs.data_type(), rhs.data_type()) {
            (Int8, Int8) => primitive!(lhs, rhs, $op, i8),
            (Int16, Int16) => primitive!(lhs, rhs, $op, i16),
            (Int32, Int32) => primitive!(lhs, rhs, $op, i32),
            (Int64, Int64) | (Duration(_), Duration(_)) => {
                primitive!(lhs, rhs, $op, i64)
            }
            (UInt8, UInt8) => primitive!(lhs, rhs, $op, u8),
            (UInt16, UInt16) => primitive!(lhs, rhs, $op, u16),
            (UInt32, UInt32) => primitive!(lhs, rhs, $op, u32),
            (UInt64, UInt64) => primitive!(lhs, rhs, $op, u64),
            (Float32, Float32) => primitive!(lhs, rhs, $op, f32),
            (Float64, Float64) => primitive!(lhs, rhs, $op, f64),
            $ (
            (Decimal(_, _), Decimal(_, _)) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                Box::new(decimal::$op_decimal(lhs, rhs)) as Box<dyn Array>
            }
            )?
            $ (
            (Time32(TimeUnit::Second), Duration(_))
            | (Time32(TimeUnit::Millisecond), Duration(_))
            | (Date32, Duration(_)) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                Box::new(time::$op_duration::<i32>(lhs, rhs)) as Box<dyn Array>
            }
            (Time64(TimeUnit::Microsecond), Duration(_))
            | (Time64(TimeUnit::Nanosecond), Duration(_))
            | (Date64, Duration(_))
            | (Timestamp(_, _), Duration(_)) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                Box::new(time::$op_duration::<i64>(lhs, rhs)) as Box<dyn Array>
            }
            )?
            $ (
            (Timestamp(_, _), Interval(IntervalUnit::MonthDayNano)) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                time::$op_interval(lhs, rhs).map(|x| Box::new(x) as Box<dyn Array>).unwrap()
            }
            )?
            $ (
            (Timestamp(_, None), Timestamp(_, None)) => {
                let lhs = lhs.as_any().downcast_ref().unwrap();
                let rhs = rhs.as_any().downcast_ref().unwrap();
                time::$op_timestamp(lhs, rhs).map(|x| Box::new(x) as Box<dyn Array>).unwrap()
            }
            )?
            _ => todo!(
                "Addition of {:?} with {:?} is not supported",
                lhs.data_type(),
                rhs.data_type()
            ),
        }
    }};
}

/// Adds two [`Array`]s.
/// # Panic
/// This function panics iff
/// * the opertion is not supported for the logical types (use [`can_add`] to check)
/// * the arrays have a different length
/// * one of the arrays is a timestamp with timezone and the timezone is not valid.
pub fn add(lhs: &dyn Array, rhs: &dyn Array) -> Box<dyn Array> {
    arith!(
        lhs,
        rhs,
        add,
        duration = add_duration,
        interval = add_interval
    )
}

/// Adds an [`Array`] and a [`Scalar`].
/// # Panic
/// This function panics iff
/// * the opertion is not supported for the logical types (use [`can_add`] to check)
/// * the arrays have a different length
/// * one of the arrays is a timestamp with timezone and the timezone is not valid.
pub fn add_scalar(lhs: &dyn Array, rhs: &dyn Scalar) -> Box<dyn Array> {
    arith!(
        lhs,
        rhs,
        add_scalar,
        duration = add_duration_scalar,
        interval = add_interval
    )
}

/// Returns whether two [`DataType`]s can be added by [`add`].
pub fn can_add(lhs: &DataType, rhs: &DataType) -> bool {
    use DataType::*;
    matches!(
        (lhs, rhs),
        (Int8, Int8)
            | (Int16, Int16)
            | (Int32, Int32)
            | (Int64, Int64)
            | (UInt8, UInt8)
            | (UInt16, UInt16)
            | (UInt32, UInt32)
            | (UInt64, UInt64)
            | (Float64, Float64)
            | (Float32, Float32)
            | (Duration(_), Duration(_))
            | (Decimal(_, _), Decimal(_, _))
            | (Date32, Duration(_))
            | (Date64, Duration(_))
            | (Time32(TimeUnit::Millisecond), Duration(_))
            | (Time32(TimeUnit::Second), Duration(_))
            | (Time64(TimeUnit::Microsecond), Duration(_))
            | (Time64(TimeUnit::Nanosecond), Duration(_))
            | (Timestamp(_, _), Duration(_))
            | (Timestamp(_, _), Interval(IntervalUnit::MonthDayNano))
    )
}

/// Subtracts two [`Array`]s.
/// # Panic
/// This function panics iff
/// * the opertion is not supported for the logical types (use [`can_sub`] to check)
/// * the arrays have a different length
/// * one of the arrays is a timestamp with timezone and the timezone is not valid.
pub fn sub(lhs: &dyn Array, rhs: &dyn Array) -> Box<dyn Array> {
    arith!(
        lhs,
        rhs,
        sub,
        decimal = sub,
        duration = subtract_duration,
        timestamp = subtract_timestamps
    )
}

/// Returns whether two [`DataType`]s can be subtracted by [`sub`].
pub fn can_sub(lhs: &DataType, rhs: &DataType) -> bool {
    use DataType::*;
    matches!(
        (lhs, rhs),
        (Int8, Int8)
            | (Int16, Int16)
            | (Int32, Int32)
            | (Int64, Int64)
            | (UInt8, UInt8)
            | (UInt16, UInt16)
            | (UInt32, UInt32)
            | (UInt64, UInt64)
            | (Float64, Float64)
            | (Float32, Float32)
            | (Duration(_), Duration(_))
            | (Decimal(_, _), Decimal(_, _))
            | (Date32, Duration(_))
            | (Date64, Duration(_))
            | (Time32(TimeUnit::Millisecond), Duration(_))
            | (Time32(TimeUnit::Second), Duration(_))
            | (Time64(TimeUnit::Microsecond), Duration(_))
            | (Time64(TimeUnit::Nanosecond), Duration(_))
            | (Timestamp(_, _), Duration(_))
            | (Timestamp(_, None), Timestamp(_, None))
    )
}

/// Multiply two [`Array`]s.
/// # Panic
/// This function panics iff
/// * the opertion is not supported for the logical types (use [`can_mul`] to check)
/// * the arrays have a different length
pub fn mul(lhs: &dyn Array, rhs: &dyn Array) -> Box<dyn Array> {
    arith!(lhs, rhs, mul, decimal = mul)
}

/// Returns whether two [`DataType`]s can be multiplied by [`mul`].
pub fn can_mul(lhs: &DataType, rhs: &DataType) -> bool {
    use DataType::*;
    matches!(
        (lhs, rhs),
        (Int8, Int8)
            | (Int16, Int16)
            | (Int32, Int32)
            | (Int64, Int64)
            | (UInt8, UInt8)
            | (UInt16, UInt16)
            | (UInt32, UInt32)
            | (UInt64, UInt64)
            | (Float64, Float64)
            | (Float32, Float32)
            | (Decimal(_, _), Decimal(_, _))
    )
}

/// Divide of two [`Array`]s.
/// # Panic
/// This function panics iff
/// * the opertion is not supported for the logical types (use [`can_div`] to check)
/// * the arrays have a different length
pub fn div(lhs: &dyn Array, rhs: &dyn Array) -> Box<dyn Array> {
    arith!(lhs, rhs, div, decimal = div)
}

/// Returns whether two [`DataType`]s can be divided by [`div`].
pub fn can_div(lhs: &DataType, rhs: &DataType) -> bool {
    can_mul(lhs, rhs)
}

/// Remainder of two [`Array`]s.
/// # Panic
/// This function panics iff
/// * the opertion is not supported for the logical types (use [`can_rem`] to check)
/// * the arrays have a different length
pub fn rem(lhs: &dyn Array, rhs: &dyn Array) -> Box<dyn Array> {
    arith!(lhs, rhs, rem)
}

/// Returns whether two [`DataType`]s "can be remainder" by [`rem`].
pub fn can_rem(lhs: &DataType, rhs: &DataType) -> bool {
    use DataType::*;
    matches!(
        (lhs, rhs),
        (Int8, Int8)
            | (Int16, Int16)
            | (Int32, Int32)
            | (Int64, Int64)
            | (UInt8, UInt8)
            | (UInt16, UInt16)
            | (UInt32, UInt32)
            | (UInt64, UInt64)
            | (Float64, Float64)
            | (Float32, Float32)
    )
}

/// Defines basic addition operation for primitive arrays
pub trait ArrayAdd<Rhs>: Sized {
    /// Adds itself to `rhs`
    fn add(&self, rhs: &Rhs) -> Self;
}

/// Defines wrapping addition operation for primitive arrays
pub trait ArrayWrappingAdd<Rhs>: Sized {
    /// Adds itself to `rhs` using wrapping addition
    fn wrapping_add(&self, rhs: &Rhs) -> Self;
}

/// Defines checked addition operation for primitive arrays
pub trait ArrayCheckedAdd<Rhs>: Sized {
    /// Checked add
    fn checked_add(&self, rhs: &Rhs) -> Self;
}

/// Defines saturating addition operation for primitive arrays
pub trait ArraySaturatingAdd<Rhs>: Sized {
    /// Saturating add
    fn saturating_add(&self, rhs: &Rhs) -> Self;
}

/// Defines Overflowing addition operation for primitive arrays
pub trait ArrayOverflowingAdd<Rhs>: Sized {
    /// Overflowing add
    fn overflowing_add(&self, rhs: &Rhs) -> (Self, Bitmap);
}

/// Defines basic subtraction operation for primitive arrays
pub trait ArraySub<Rhs>: Sized {
    /// subtraction
    fn sub(&self, rhs: &Rhs) -> Self;
}

/// Defines wrapping subtraction operation for primitive arrays
pub trait ArrayWrappingSub<Rhs>: Sized {
    /// wrapping subtraction
    fn wrapping_sub(&self, rhs: &Rhs) -> Self;
}

/// Defines checked subtraction operation for primitive arrays
pub trait ArrayCheckedSub<Rhs>: Sized {
    /// checked subtraction
    fn checked_sub(&self, rhs: &Rhs) -> Self;
}

/// Defines saturating subtraction operation for primitive arrays
pub trait ArraySaturatingSub<Rhs>: Sized {
    /// saturarting subtraction
    fn saturating_sub(&self, rhs: &Rhs) -> Self;
}

/// Defines Overflowing subtraction operation for primitive arrays
pub trait ArrayOverflowingSub<Rhs>: Sized {
    /// overflowing subtraction
    fn overflowing_sub(&self, rhs: &Rhs) -> (Self, Bitmap);
}

/// Defines basic multiplication operation for primitive arrays
pub trait ArrayMul<Rhs>: Sized {
    /// multiplication
    fn mul(&self, rhs: &Rhs) -> Self;
}

/// Defines wrapping multiplication operation for primitive arrays
pub trait ArrayWrappingMul<Rhs>: Sized {
    /// wrapping multiplication
    fn wrapping_mul(&self, rhs: &Rhs) -> Self;
}

/// Defines checked multiplication operation for primitive arrays
pub trait ArrayCheckedMul<Rhs>: Sized {
    /// checked multiplication
    fn checked_mul(&self, rhs: &Rhs) -> Self;
}

/// Defines saturating multiplication operation for primitive arrays
pub trait ArraySaturatingMul<Rhs>: Sized {
    /// saturating multiplication
    fn saturating_mul(&self, rhs: &Rhs) -> Self;
}

/// Defines Overflowing multiplication operation for primitive arrays
pub trait ArrayOverflowingMul<Rhs>: Sized {
    /// overflowing multiplication
    fn overflowing_mul(&self, rhs: &Rhs) -> (Self, Bitmap);
}

/// Defines basic division operation for primitive arrays
pub trait ArrayDiv<Rhs>: Sized {
    /// division
    fn div(&self, rhs: &Rhs) -> Self;
}

/// Defines checked division operation for primitive arrays
pub trait ArrayCheckedDiv<Rhs>: Sized {
    /// checked division
    fn checked_div(&self, rhs: &Rhs) -> Self;
}

/// Defines basic reminder operation for primitive arrays
pub trait ArrayRem<Rhs>: Sized {
    /// remainder
    fn rem(&self, rhs: &Rhs) -> Self;
}

/// Defines checked reminder operation for primitive arrays
pub trait ArrayCheckedRem<Rhs>: Sized {
    /// checked remainder
    fn checked_rem(&self, rhs: &Rhs) -> Self;
}

/// Trait describing a [`NativeType`] whose semantics of arithmetic in Arrow equals
/// the semantics in Rust.
/// A counter example is `i128`, that in arrow represents a decimal while in rust represents
/// a signed integer.
pub trait NativeArithmetics: NativeType {}
impl NativeArithmetics for u8 {}
impl NativeArithmetics for u16 {}
impl NativeArithmetics for u32 {}
impl NativeArithmetics for u64 {}
impl NativeArithmetics for i8 {}
impl NativeArithmetics for i16 {}
impl NativeArithmetics for i32 {}
impl NativeArithmetics for i64 {}
impl NativeArithmetics for f32 {}
impl NativeArithmetics for f64 {}
