pub mod reader;
pub mod record;
pub mod writer;

use std::hash::Hash;
use std::io::Result;
use std::path::Path;

use serde::{Deserialize, Serialize};

use generic::IdType;
use generic::{GeneralGraph, MutGraphTrait};
use io::csv::reader::GraphReader;
use io::csv::writer::GraphWriter;

pub fn write_to_csv< Id, NL, EL, P>(
    g: &GeneralGraph<Id, NL, EL>,
    path_to_nodes: P,
    path_to_edges: P,
) -> Result<()>
where
    Id: IdType + Serialize,
    NL: Hash + Eq + Serialize,
    EL: Hash + Eq + Serialize,
    P: AsRef<Path>,
{
    GraphWriter::new(g, path_to_nodes, path_to_edges).write()
}

pub fn read_from_csv<Id, NL, EL, G, P>(g: &mut G, path_to_nodes: Option<P>, path_to_edges: P) -> Result<()>
where
    for<'de> Id: IdType + Serialize + Deserialize<'de>,
    for<'de> NL: Hash + Eq + Serialize + Deserialize<'de>,
    for<'de> EL: Hash + Eq + Serialize + Deserialize<'de>,
    G: MutGraphTrait<Id, NL, EL>,
    P: AsRef<Path>,
{
    GraphReader::new(path_to_nodes, path_to_edges).read(g)
}

//impl<Ty: GraphType, NL: Hash + Eq, EL: Hash + Eq> GraphReader<Ty, NL, EL> {
//    pub fn new<P: AsRef<Path>>(path_to_nodes: P, path_to_edges: P) -> Self {
