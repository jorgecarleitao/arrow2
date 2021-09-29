//! contains a wide range of compute operations (e.g.
//! [`arithmetics`], [`aggregate`],
//! [`filter`], [`comparison`], and [`sort`])
//!
//! This module's general design is
//! that each operator has two interfaces, a statically-typed version and a dynamically-typed
//! version.
//! The statically-typed version expects concrete arrays (such as [`PrimitiveArray`](crate::array::PrimitiveArray));
//! the dynamically-typed version expects `&dyn Array` and errors if the the type is not
//! supported.
//! Some dynamically-typed operators have an auxiliary function, `can_*`, that returns
//! true if the operator can be applied to the particular `DataType`.

pub mod aggregate;
pub mod arithmetics;
pub mod arity;
pub mod boolean;
pub mod boolean_kleene;
pub mod cast;
pub mod comparison;
pub mod concat;
pub mod contains;
pub mod filter;
pub mod hash;
pub mod if_then_else;
pub mod length;
pub mod limit;
pub mod nullif;
pub mod partition;
pub mod sort;
pub mod substring;
pub mod take;
pub mod temporal;
mod utils;
pub mod window;

#[cfg(feature = "regex")]
#[cfg_attr(docsrs, doc(cfg(feature = "regex")))]
pub mod like;
#[cfg(feature = "regex")]
#[cfg_attr(docsrs, doc(cfg(feature = "regex")))]
pub mod regex_match;

#[cfg(feature = "merge_sort")]
#[cfg_attr(docsrs, doc(cfg(feature = "merge_sort")))]
pub mod merge_sort;
