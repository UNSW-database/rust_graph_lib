/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

/// Implementations of id-item mapping table that
/// maps arbitrary data to `usize` integer.
pub mod set_map;
pub mod vec_map;

pub use crate::map::set_map::SetMap;
pub use crate::map::vec_map::VecMap;
