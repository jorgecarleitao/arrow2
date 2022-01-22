mod binary;
mod fixed_size_binary;
mod boolean;
mod fixed_size_list;
mod list;
mod null;
mod primitive;
mod struct_;
mod utf8;

// check that `PartialEq` can be derived
#[derive(PartialEq)]
struct A {
    array: std::sync::Arc<dyn arrow2::scalar::Scalar>,
}
