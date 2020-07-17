/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

pub mod generic;
pub mod graph_gen;
pub mod graph_impl;
pub mod io;
pub mod map;
pub mod prelude;

pub use crate::graph_impl::{
    DiGraphMap, DiStaticGraph, GraphMap, StaticGraph, UnGraphMap, UnStaticGraph,
};

pub static VERSION: &str = env!("CARGO_PKG_VERSION");
pub static NAME: &str = env!("CARGO_PKG_NAME");
