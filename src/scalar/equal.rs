use super::*;

impl PartialEq for dyn Scalar {
    fn eq(&self, other: &Self) -> bool {
        equal(self, other)
    }
}

macro_rules! dyn_eq {
    ($ty:ty, $lhs:expr, $rhs:expr) => {{
        let lhs = $lhs
            .as_any()
            .downcast_ref::<PrimitiveScalar<$ty>>()
            .unwrap();
        let rhs = $rhs
            .as_any()
            .downcast_ref::<PrimitiveScalar<$ty>>()
            .unwrap();
        lhs == rhs
    }};
}

fn equal(lhs: &dyn Scalar, rhs: &dyn Scalar) -> bool {
    if lhs.data_type() != rhs.data_type() {
        return false;
    }

    match lhs.data_type() {
        DataType::Null => {
            let lhs = lhs.as_any().downcast_ref::<NullScalar>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<NullScalar>().unwrap();
            lhs == rhs
        }
        DataType::Boolean => {
            let lhs = lhs.as_any().downcast_ref::<BooleanScalar>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<BooleanScalar>().unwrap();
            lhs == rhs
        }
        DataType::UInt8 => {
            dyn_eq!(u8, lhs, rhs)
        }
        DataType::UInt16 => {
            dyn_eq!(u16, lhs, rhs)
        }
        DataType::UInt32 => {
            dyn_eq!(u32, lhs, rhs)
        }
        DataType::UInt64 => {
            dyn_eq!(u64, lhs, rhs)
        }
        DataType::Int8 => {
            dyn_eq!(i8, lhs, rhs)
        }
        DataType::Int16 => {
            dyn_eq!(i16, lhs, rhs)
        }
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            dyn_eq!(i32, lhs, rhs)
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            dyn_eq!(i64, lhs, rhs)
        }
        DataType::Decimal(_, _) => {
            dyn_eq!(i128, lhs, rhs)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            dyn_eq!(days_ms, lhs, rhs)
        }
        DataType::Float16 => unreachable!(),
        DataType::Float32 => {
            dyn_eq!(f32, lhs, rhs)
        }
        DataType::Float64 => {
            dyn_eq!(f64, lhs, rhs)
        }
        DataType::Utf8 => {
            let lhs = lhs.as_any().downcast_ref::<Utf8Scalar<i32>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Utf8Scalar<i32>>().unwrap();
            lhs == rhs
        }
        DataType::LargeUtf8 => {
            let lhs = lhs.as_any().downcast_ref::<Utf8Scalar<i64>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<Utf8Scalar<i64>>().unwrap();
            lhs == rhs
        }
        DataType::Binary => {
            let lhs = lhs.as_any().downcast_ref::<BinaryScalar<i32>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<BinaryScalar<i32>>().unwrap();
            lhs == rhs
        }
        DataType::LargeBinary => {
            let lhs = lhs.as_any().downcast_ref::<BinaryScalar<i64>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<BinaryScalar<i64>>().unwrap();
            lhs == rhs
        }
        DataType::List(_) => {
            let lhs = lhs.as_any().downcast_ref::<ListScalar<i32>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<ListScalar<i32>>().unwrap();
            lhs == rhs
        }
        DataType::LargeList(_) => {
            let lhs = lhs.as_any().downcast_ref::<ListScalar<i64>>().unwrap();
            let rhs = rhs.as_any().downcast_ref::<ListScalar<i64>>().unwrap();
            lhs == rhs
        }
        _ => unimplemented!(),
    }
}
