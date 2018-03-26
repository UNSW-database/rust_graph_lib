#[macro_use]
extern crate serde_derive;

extern crate serde;

extern crate indexmap;

pub mod generic;
pub mod prelude;
pub mod graph_impl;
pub mod converter;
pub mod map;
pub mod io;
pub mod pattern_matching;

pub use graph_impl::{DiGraphMap, GraphMap, UnGraphMap};
pub use graph_impl::{DiStaticGraph, StaticGraph, UnStaticGraph};
