/// Implementations of id-item mapping table that
/// maps arbitrary data to `usize` integer.

pub mod setmap;
pub mod vecmap;

pub use graph_impl::map::setmap::SetMap;
pub use graph_impl::map::vecmap::VecMap;
