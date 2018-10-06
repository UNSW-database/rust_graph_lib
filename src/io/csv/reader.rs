/// Nodes:
/// node_id <sep> node_label
///
/// Edges:
/// src <sep> dst <sep> edge_label
use std::hash::Hash;
use std::io::Result;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use csv::ReaderBuilder;
use serde::Deserialize;

use generic::IdType;
use generic::MutGraphTrait;
use io::csv::record::{EdgeRecord, NodeRecord};

pub struct GraphReader<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> {
    path_to_nodes: Option<PathBuf>,
    path_to_edges: PathBuf,
    separator: u8,
has_headers:bool,
    id_type: PhantomData<Id>,
    nl_type: PhantomData<NL>,
    el_type: PhantomData<EL>,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> GraphReader<Id, NL, EL> {
    pub fn new<P: AsRef<Path>>(path_to_nodes: Option<P>, path_to_edges: P,has_headers:bool) -> Self {
        GraphReader {
            path_to_nodes: path_to_nodes.map_or(None, |x| Some(x.as_ref().to_path_buf())),
            path_to_edges: path_to_edges.as_ref().to_path_buf(),
            separator: b',',
            has_headers,
            id_type: PhantomData,
            nl_type: PhantomData,
            el_type: PhantomData,
        }
    }

    pub fn with_separator<P: AsRef<Path>>(
        path_to_nodes: Option<P>,
        path_to_edges: P,
        separator: &str,
        has_headers:bool,
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

        GraphReader {
            path_to_nodes: path_to_nodes.map_or(None, |x| Some(x.as_ref().to_path_buf())),
            path_to_edges: path_to_edges.as_ref().to_path_buf(),
            separator: sep_string.chars().next().unwrap() as u8,
            has_headers,
            id_type: PhantomData,
            nl_type: PhantomData,
            el_type: PhantomData,
        }
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> GraphReader<Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
    pub fn read<G: MutGraphTrait<Id, NL, EL>>(&self, g: &mut G) -> Result<()> {
        if let Some(ref path_to_nodes) = self.path_to_nodes {
            info!(
                "csv::Reader::read - Adding nodes from {}",
                path_to_nodes.as_path().to_str().unwrap()
            );
            let mut rdr = ReaderBuilder::new()
                .has_headers(self.has_headers)
                .delimiter(self.separator)
                .from_path(path_to_nodes.as_path())?;

            for result in rdr.deserialize() {
                let record: NodeRecord<Id, NL> = result?;
                record.add_to_graph(g);
            }
        }

        info!(
            "csv::Reader::read - Adding edges from {}",
            self.path_to_edges.as_path().to_str().unwrap()
        );

        let mut rdr = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .delimiter(self.separator)
            .from_path(self.path_to_edges.as_path())?;

        for result in rdr.deserialize() {
            let record: EdgeRecord<Id, EL> = result?;
            record.add_to_graph(g);
        }

        Ok(())
    }
}
