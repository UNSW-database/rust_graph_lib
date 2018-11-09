extern crate bincode;
extern crate counter;
extern crate csv;
extern crate indexmap;
extern crate itertools;
extern crate rand;
extern crate serde;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[cfg(feature = "ldbc")]
extern crate regex;

pub mod generic;
pub mod graph_gen;
pub mod graph_impl;
pub mod io;
pub mod map;
pub mod prelude;
pub mod algorithm;

pub use graph_impl::{
    DiGraphMap, DiStaticGraph, GraphMap, StaticGraph, StaticGraphMmap, UnGraphMap, UnStaticGraph,
};
