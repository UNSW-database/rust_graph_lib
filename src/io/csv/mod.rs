pub mod reader;
pub mod record;
pub mod writer;

use std::hash::Hash;
use std::io::Result;
use std::path::Path;

use serde::{Deserialize, Serialize};

use generic::GeneralGraph;
use generic::{GraphType, IdType};
use graph_impl::TypedGraphMap;
use io::csv::reader::TypedGraphReader;
use io::csv::writer::GraphWriter;

pub fn write_to_csv<'a, Id, NL, EL, P>(
    g: &'a GeneralGraph<Id, NL, EL>,
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

pub fn read_from_csv<'a, Id, NL, EL, Ty, P>(
    path_to_nodes: P,
    path_to_edges: P,
) -> Result<TypedGraphMap<Id, NL, EL, Ty>>
where
    for<'de> Id: IdType + Serialize + Deserialize<'de>,
    for<'de> NL: Hash + Eq + Serialize + Deserialize<'de>,
    for<'de> EL: Hash + Eq + Serialize + Deserialize<'de>,
    Ty: GraphType,
    P: AsRef<Path>,
{
    TypedGraphReader::new(path_to_nodes, path_to_edges).read()
}

//impl<Ty: GraphType, NL: Hash + Eq, EL: Hash + Eq> GraphReader<Ty, NL, EL> {
//    pub fn new<P: AsRef<Path>>(path_to_nodes: P, path_to_edges: P) -> Self {
