use std::fmt::{Debug, Display};
use std::hash::Hash;

/// The default data type for graph indices is `u32`.
#[cfg(not(feature = "usize_id"))]
pub type DefaultId = u32;

/// The default data type for graph indices can be set to `usize` by setting `feature="usize_id"`.
#[cfg(feature = "usize_id")]
pub type DefaultId = usize;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub enum Void {}

pub trait GraphType: Debug + Eq + Clone {
    fn is_directed() -> bool;
}

/// Marker for directed graph
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Directed {}

/// Marker for undirected graph
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Undirected {}

impl GraphType for Directed {
    #[inline]
    fn is_directed() -> bool {
        true
    }
}

impl GraphType for Undirected {
    #[inline]
    fn is_directed() -> bool {
        false
    }
}

pub unsafe trait IdType: Copy + Clone + Default + Hash + Debug + Display + Ord {
    fn new(x: usize) -> Self;
    fn id(&self) -> usize;
    fn max_value() -> Self;
}

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
}

//impl<Id: IdType> From<usize> for Id {
//    fn from(id: usize) -> Self {
//        IdType::new(id)
//    }
//}
//
//impl<Id: IdType> From<Id> for usize {
//    fn from(id: Id) -> Self {
//        id.id()
//    }
//}
