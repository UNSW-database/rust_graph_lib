/// Nodes:
/// node_id <sep> node_label(optional)
///
/// Edges:
/// src <sep> dst <sep> edge_label(optional)

use std::marker::PhantomData;

use std::fs::File;
use std::io::{BufRead, BufReader};

use generic::{Directed, GraphType, Undirected};
use generic::MutGraphTrait;
use graph_impl::GraphMap;
use converter::graph::{DiStaticGraphConverter, UnStaticGraphConverter};

pub struct GraphReader<Ty: GraphType> {
    path_to_nodes: String,
    path_to_edges: String,
    separator: String,
    graph_type: PhantomData<Ty>,
}

pub type DiGraphReader = GraphReader<Directed>;
pub type UnGraphReader = GraphReader<Undirected>;

impl<Ty: GraphType> GraphReader<Ty> {
    pub fn new(path_to_nodes: String, path_to_edges: String) -> Self {
        GraphReader {
            path_to_nodes,
            path_to_edges,
            separator: ",".to_owned(),
            graph_type: PhantomData,
        }
    }

    pub fn with_separator(path_to_nodes: String, path_to_edges: String, separator: String) -> Self {
        GraphReader {
            path_to_nodes,
            path_to_edges,
            separator,
            graph_type: PhantomData,
        }
    }
}

impl<Ty: GraphType> GraphReader<Ty> {
    pub fn read(&self) -> GraphMap<String, Ty> {
        let mut g = GraphMap::<String, Ty>::new();

        let file = File::open(&self.path_to_nodes)
            .expect(&format!("Error when reading {}", &self.path_to_nodes));
        for line in BufReader::new(file).lines() {
            let line = line.unwrap();
            let line_vec: Vec<&str> = line.split(&self.separator).collect();
            let length = line_vec.len();

            if length < 1 || length > 2 {
                panic!("Unknown format!")
            }

            let node_id: usize = line_vec[0].parse().unwrap();
            let node_label = if length > 1 {
                Some(line_vec[1].to_owned())
            } else {
                None
            };

            g.add_node(node_id, node_label);
        }

        let file = File::open(&self.path_to_edges)
            .expect(&format!("Error when reading {}", &self.path_to_edges));
        for line in BufReader::new(file).lines() {
            let line = line.unwrap();
            let line_vec: Vec<&str> = line.split(&self.separator).collect();
            let length = line_vec.len();

            if length < 2 || length > 3 {
                panic!("Unknown format!")
            }

            let src: usize = line_vec[0].parse().unwrap();
            let dst: usize = line_vec[1].parse().unwrap();
            let edge_label = if length > 2 {
                Some(line_vec[2].to_owned())
            } else {
                None
            };

            g.add_edge(src, dst, edge_label);
        }

        g
    }
}

impl DiGraphReader {
    pub fn read_to_static(&self) -> DiStaticGraphConverter<String> {
        let g = self.read();
        DiStaticGraphConverter::from(g)
    }
}

impl UnGraphReader {
    pub fn read_to_static(&self) -> UnStaticGraphConverter<String> {
        let g = self.read();
        UnStaticGraphConverter::from(g)
    }
}
