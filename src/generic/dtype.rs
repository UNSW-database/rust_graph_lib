use std::fmt::Debug;
use std::hash::Hash;

/// The default data type for graph indices.
pub type DefaultId = u32;

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

pub unsafe trait IdType: Copy + Clone + Default + Hash + Debug + Ord {
    fn new(x: usize) -> Self;
    fn id(&self) -> usize;
    fn max_value() -> Self;
    fn max_usize() -> usize;
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
    #[inline(always)]
    fn max_usize() -> usize {
        ::std::u8::MAX as usize
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
    fn max_usize() -> usize {
        ::std::u16::MAX as usize
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
    fn max_usize() -> usize {
        ::std::u32::MAX as usize
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
    fn max_usize() -> usize {
        ::std::usize::MAX
    }
}
