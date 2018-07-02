extern crate indexmap;
extern crate itertools;
extern crate rand;
extern crate serde;
extern crate bincode;

#[macro_use]
extern crate serde_derive;

pub mod converter;
pub mod generic;
pub mod graph_gen;
pub mod graph_impl;
pub mod io;
pub mod map;
pub mod pattern_matching;
pub mod prelude;

pub use graph_impl::{DiGraphMap, GraphMap, UnGraphMap};
pub use graph_impl::{DiStaticGraph, StaticGraph, UnStaticGraph};

pub use converter::{DiStaticGraphConverter, StaticGraphConverter, UnStaticGraphConverter};
