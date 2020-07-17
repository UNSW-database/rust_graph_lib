/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

use std::fmt::Debug;
use std::hash::Hash;
use std::marker::{Send, Sync};

/// The default data type for graph indices can be set to `usize` by setting `feature="usize_id"`.
#[cfg(feature = "usize_id")]
pub type DefaultId = usize;

/// The default data type for graph indices is `u32`.
#[cfg(not(feature = "usize_id"))]
pub type DefaultId = u32;

pub type DefaultTy = Directed;

pub type Void = ();

pub trait GraphType: Debug + PartialEq + Eq + Copy + Clone + Hash {
    fn is_directed() -> bool;
}

/// Marker for directed graph
#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash, Serialize, Deserialize)]
pub enum Directed {}

/// Marker for undirected graph
#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash, Serialize, Deserialize)]
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

pub unsafe trait IdType:
    'static + Copy + Clone + Default + Hash + Debug + Ord + Eq + Send + Sync
{
    fn new(x: usize) -> Self;
    fn id(&self) -> usize;
    fn max_value() -> Self;
    fn max_usize() -> usize;
    #[inline(always)]
    fn increment(&mut self) {}
}

unsafe impl IdType for () {
    #[inline(always)]
    fn new(_: usize) -> Self {
        ()
    }
    #[inline(always)]
    fn id(&self) -> usize {
        0
    }
    #[inline(always)]
    fn max_value() -> Self {
        ()
    }
    #[inline(always)]
    fn max_usize() -> usize {
        0
    }
}

macro_rules! impl_id_type {
    ($($type:ident,)*) => (
        $(
            unsafe impl IdType for $type {
                #[inline(always)]
                fn new(x: usize) -> Self {
                    x as $type
                }
                #[inline(always)]
                fn id(&self) -> usize {
                    *self as usize
                }
                #[inline(always)]
                fn max_value() -> Self {
                    ::std::$type::MAX
                }
                #[inline(always)]
                fn max_usize() -> usize {
                    ::std::$type::MAX as usize
                }
                #[inline(always)]
                fn increment(&mut self) {
                    *self+=1;
                }
            }
        )*
    )
}

impl_id_type!(u8, u16, u32, usize,);
