/// Implementations of id-item mapping table that
/// maps arbitrary data to `usize` integer.

pub mod set_map;
pub mod vec_map;

pub use graph_impl::map::set_map::SetMap;
pub use graph_impl::map::vec_map::VecMap;
