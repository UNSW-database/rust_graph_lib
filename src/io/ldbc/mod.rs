pub mod node;
pub mod relation;
pub mod scheme;

pub use io::ldbc::scheme::Scheme;

use generic::{GraphType, IdType};
use graph_impl::TypedGraphMap;
use std::path::Path;

pub fn ldbc_from_path<Id: IdType, Ty: GraphType, P: AsRef<Path>>(
    path: P,
) -> TypedGraphMap<Id, String, String, Ty> {
    self::scheme::Scheme::init().from_path(path).unwrap()
}
