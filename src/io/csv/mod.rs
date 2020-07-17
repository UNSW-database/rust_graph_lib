/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

pub mod reader;
pub mod record;
pub mod writer;

use std::hash::Hash;
use std::io::Result;
use std::path::Path;

use serde::{Deserialize, Serialize};
pub use serde_json::Value as JsonValue;

use crate::generic::{GeneralGraph, IdType, MutGraphTrait};
pub use crate::io::csv::reader::CSVReader;
pub use crate::io::csv::writer::CSVWriter;

pub fn write_to_csv<Id, NL, EL, P, L>(
    g: &dyn GeneralGraph<Id, NL, EL, L>,
    path_to_nodes: P,
    path_to_edges: P,
) -> Result<()>
where
    Id: IdType + Serialize,
    NL: Hash + Eq + Serialize,
    EL: Hash + Eq + Serialize,
    L: IdType + Serialize,
    P: AsRef<Path>,
{
    CSVWriter::new(g, path_to_nodes, path_to_edges).write()
}

pub fn read_from_csv<Id, NL, EL, G, P>(
    g: &mut G,
    path_to_nodes: Vec<P>,
    path_to_edges: Vec<P>,
    separator: Option<&str>,
    has_headers: bool,
    is_flexible: bool,
) where
    for<'de> Id: IdType + Serialize + Deserialize<'de>,
    for<'de> NL: Hash + Eq + Serialize + Deserialize<'de>,
    for<'de> EL: Hash + Eq + Serialize + Deserialize<'de>,
    G: MutGraphTrait<Id, NL, EL>,
    P: AsRef<Path>,
{
    let mut reader = CSVReader::new(path_to_nodes, path_to_edges)
        .headers(has_headers)
        .flexible(is_flexible);

    if let Some(sep) = separator {
        reader = reader.with_separator(sep);
    }

    reader.read(g)
}
