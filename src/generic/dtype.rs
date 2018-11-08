use std::fmt::{Debug, Display};
use std::hash::Hash;

/// The default data type for graph indices is `u32`.
#[cfg(not(feature = "usize_id"))]
pub type DefaultId = u32;

/// The default data type for graph indices can be set to `usize` by setting `feature="usize_id"`.
#[cfg(feature = "usize_id")]
pub type DefaultId = usize;

pub type DefaultTy = Directed;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub enum Void {}

pub trait GraphType: Debug + Eq + Clone {
    fn is_directed() -> bool;
}

/// Marker for directed graph
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub enum Directed {}

/// Marker for undirected graph
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub enum Undirected {}

impl GraphType for Directed {
    #[inline(always)]
    fn is_directed() -> bool {
        true
    }
}

impl GraphType for Undirected {
    #[inline(always)]
    fn is_directed() -> bool {
        false
    }
}

pub unsafe trait IdType: Copy + Clone + Default + Hash + Debug + Display + Ord {
    fn new(x: usize) -> Self;
    fn id(&self) -> usize;
    fn max_value() -> Self;
    fn increment(&self) -> Self;
}

//unsafe impl IdType for () {
//    #[inline(always)]
//    fn new(x: usize) -> Self {
//        ()
//    }
//    #[inline(always)]
//    fn id(&self) -> usize {
//        0
//    }
//    #[inline(always)]
//    fn max_value() -> Self {
//        ()
//    }
//    #[inline(always)]
//    fn increment(&self) -> Self {
//        ()
//    }
//}

unsafe impl IdType for u8 {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x as u8
    }
    #[inline(always)]
    fn id(&self) -> usize {
        *self as usize
    }
    #[inline(always)]
    fn max_value() -> Self {
        ::std::u8::MAX
    }
    #[inline(always)]
    fn increment(&self) -> Self {
        *self + 1
    }
}

unsafe impl IdType for u16 {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x as u16
    }
    #[inline(always)]
    fn id(&self) -> usize {
        *self as usize
    }
    #[inline(always)]
    fn max_value() -> Self {
        ::std::u16::MAX
    }
    #[inline(always)]
    fn increment(&self) -> Self {
        *self + 1
    }
}

unsafe impl IdType for u32 {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x as u32
    }
    #[inline(always)]
    fn id(&self) -> usize {
        *self as usize
    }
    #[inline(always)]
    fn max_value() -> Self {
        ::std::u32::MAX
    }
    #[inline(always)]
    fn increment(&self) -> Self {
        *self + 1
    }
}

unsafe impl IdType for usize {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x
    }
    #[inline(always)]
    fn id(&self) -> usize {
        *self
    }
    #[inline(always)]
    fn max_value() -> Self {
        ::std::usize::MAX
    }
    #[inline(always)]
    fn increment(&self) -> Self {
        *self + 1
    }
}
