#[macro_use]
extern crate serde_derive;

extern crate serde;

extern crate ordermap;

pub mod generic;
pub mod prelude;
pub mod graph_impl;
pub mod pattern_matching;

pub use graph_impl::{DiGraphMap, GraphMap, UnGraphMap};
pub use graph_impl::{DiStaticGraph, EdgeVec, StaticGraph, UnStaticGraph};

//#[cfg(test)]
//mod tests {
//    #[test]
//    fn it_works() {
//        assert_eq!(2 + 2, 4);
//    }
//}
