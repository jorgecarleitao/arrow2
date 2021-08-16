use crate::types::NativeType;

use super::super::display::get_value_display;
use super::super::{display_fmt, Array};
use super::PrimitiveArray;

impl<T: NativeType> std::fmt::Display for PrimitiveArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = get_value_display(self);
        let new_lines = false;
        let head = &format!("{}", self.data_type());
        let iter = self.iter().enumerate().map(|(i, x)| x.map(|_| display(i)));
        display_fmt(iter, head, f, new_lines)
    }
}
