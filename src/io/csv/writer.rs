/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

use std::hash::Hash;
use std::io::Result;
use std::path::{Path, PathBuf};

use csv::WriterBuilder;
use serde::Serialize;

use crate::generic::GeneralGraph;
use crate::generic::IdType;
use crate::io::csv::record::{EdgeRecord, NodeRecord};

pub struct CSVWriter<'a, Id, NL, EL, L>
where
    Id: 'a + IdType + Serialize,
    NL: 'a + Hash + Eq + Serialize,
    EL: 'a + Hash + Eq + Serialize,
    L: 'a + IdType + Serialize,
{
    g: &'a dyn GeneralGraph<Id, NL, EL, L>,
    path_to_nodes: PathBuf,
    path_to_edges: PathBuf,
    separator: u8,
}

impl<'a, Id, NL, EL, L> CSVWriter<'a, Id, NL, EL, L>
where
    Id: 'a + IdType + Serialize,
    NL: 'a + Hash + Eq + Serialize,
    EL: 'a + Hash + Eq + Serialize,

    L: 'a + IdType + Serialize,
{
    pub fn new<P: AsRef<Path>>(
        g: &'a dyn GeneralGraph<Id, NL, EL, L>,
        path_to_nodes: P,
        path_to_edges: P,
    ) -> Self {
        CSVWriter {
            g,
            path_to_nodes: path_to_nodes.as_ref().to_path_buf(),
            path_to_edges: path_to_edges.as_ref().to_path_buf(),
            separator: b',',
        }
    }

    pub fn with_separator<P: AsRef<Path>>(
        g: &'a dyn GeneralGraph<Id, NL, EL, L>,
        path_to_nodes: P,
        path_to_edges: P,
        separator: &str,
    ) -> Self {
        let sep_string = match separator {
            "comma" => ",",
            "space" => " ",
            "tab" => "\t",
            other => other,
        };

        if sep_string.len() != 1 {
            panic!("Invalid separator {}.", sep_string);
        }

        CSVWriter {
            g,
            path_to_nodes: path_to_nodes.as_ref().to_path_buf(),
            path_to_edges: path_to_edges.as_ref().to_path_buf(),
            separator: sep_string.chars().next().unwrap() as u8,
        }
    }
}

impl<'a, Id, NL, EL, L> CSVWriter<'a, Id, NL, EL, L>
where
    Id: 'a + IdType + Serialize,
    NL: 'a + Hash + Eq + Serialize,
    EL: 'a + Hash + Eq + Serialize,
    L: 'a + IdType + Serialize,
{
    pub fn write(&self) -> Result<()> {
        let g = self.g.as_labeled_graph();

        info!(
            "csv::Writer::write - Writing nodes to {}",
            self.path_to_nodes.as_path().to_str().unwrap()
        );

        let mut wtr = WriterBuilder::new()
            .delimiter(self.separator)
            .from_path(self.path_to_nodes.as_path())?;

        for id in self.g.node_indices() {
            wtr.serialize(NodeRecord::new(id, g.get_node_label(id)))?;
        }

        info!(
            "csv::Writer::write - Writing edges to {}",
            self.path_to_edges.as_path().to_str().unwrap()
        );

        let mut wtr = WriterBuilder::new()
            .delimiter(self.separator)
            .from_path(self.path_to_edges.as_path())?;

        for (start, target) in self.g.edge_indices() {
            wtr.serialize(EdgeRecord::new(
                start,
                target,
                g.get_edge_label(start, target),
            ))?;
        }

        Ok(())
    }
}
