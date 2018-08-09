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

use converter::graph::{DiStaticGraphConverter, UnStaticGraphConverter};
use generic::{DefaultId, IdType};
use generic::{DefaultTy, Directed, GraphType, Undirected};
use graph_impl::TypedGraphMap;
use io::csv::record::{EdgeRecord, NodeRecord};

pub struct TypedGraphReader<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> {
    path_to_nodes: PathBuf,
    path_to_edges: PathBuf,
    separator: u8,
    graph_type: PhantomData<Ty>,
    id: PhantomData<Id>,
    nl: PhantomData<NL>,
    el: PhantomData<EL>,
}

pub type GraphReader<NL = String, EL = String, Ty = DefaultTy> =
    TypedGraphReader<DefaultId, NL, EL, Ty>;
pub type DiGraphReader<NL = String, EL = String> = GraphReader<NL, EL, Directed>;
pub type UnGraphReader<NL = String, EL = String> = GraphReader<NL, EL, Undirected>;

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> TypedGraphReader<Id, NL, EL, Ty> {
    pub fn new<P: AsRef<Path>>(path_to_nodes: P, path_to_edges: P) -> Self {
        TypedGraphReader {
            path_to_nodes: path_to_nodes.as_ref().to_path_buf(),
            path_to_edges: path_to_edges.as_ref().to_path_buf(),
            separator: b',',
            graph_type: PhantomData,
            id: PhantomData,
            nl: PhantomData,
            el: PhantomData,
        }
    }

    pub fn with_separator<P: AsRef<Path>>(
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

        TypedGraphReader {
            path_to_nodes: path_to_nodes.as_ref().to_path_buf(),
            path_to_edges: path_to_edges.as_ref().to_path_buf(),
            separator: sep_string.chars().next().unwrap() as u8,
            graph_type: PhantomData,
            id: PhantomData,
            nl: PhantomData,
            el: PhantomData,
        }
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> TypedGraphReader<Id, NL, EL, Ty>
where
    for<'de> Id: IdType + Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
    pub fn read(&self) -> Result<TypedGraphMap<Id, NL, EL, Ty>> {
        let mut g = TypedGraphMap::new();

        info!(
            "csv::Reader::read - Adding nodes from {}",
            self.path_to_nodes.as_path().to_str().unwrap()
        );

        let mut rdr = ReaderBuilder::new()
            .delimiter(self.separator)
            .from_path(self.path_to_nodes.as_path())?;

        for result in rdr.deserialize() {
            let record: NodeRecord<Id, NL> = result?;
            record.add_to_graph(&mut g);
        }

        info!(
            "csv::Reader::read - Adding edges from {}",
            self.path_to_edges.as_path().to_str().unwrap()
        );

        let mut rdr = ReaderBuilder::new()
            .delimiter(self.separator)
            .from_path(self.path_to_edges.as_path())?;

        for result in rdr.deserialize() {
            let record: EdgeRecord<Id, EL> = result?;
            record.add_to_graph(&mut g);
        }

        Ok(g)
    }
}

//
//impl DiGraphReader {
//    pub fn read_to_static(&self) -> DiStaticGraphConverter<String, String> {
//        let g = self.read();
//
//        DiStaticGraphConverter::new(&g)
//    }
//}
//
//impl UnGraphReader {
//    pub fn read_to_static(&self) -> UnStaticGraphConverter<String, String> {
//        let g = self.read();
//
//        UnStaticGraphConverter::new(&g)
//    }
//}
