pub mod csv;
pub mod mmap;
pub mod serde;

pub use io::csv::{read_from_csv, write_to_csv};

#[cfg(feature = "ldbc")]
pub mod ldbc;
#[cfg(feature = "ldbc")]
pub use io::ldbc::read_ldbc_from_path;
