use std::borrow::Cow;
use std::hash::Hash;
use std::marker::PhantomData;

use cdrs::authenticators::{NoneAuthenticator, StaticPasswordAuthenticator};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::frame::IntoBytes;
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;
use cdrs::types::rows::Row;
use cdrs::types::{AsRust, AsRustType, ByIndex, ByName, IntoRustByIndex, IntoRustByName};

use generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType};
use graph_impl::GraphImpl;
use map::SetMap;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

/// Cassandra scheme:
///     CREATE TABLE graph_name.graph (
///         id bigint PRIMARY KEY,
///         adj list<bigint>
///     )

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RawNode {
    id: i64,
    adj: Vec<i64>,
}

pub struct CassandraGraph<Id, L = Id> {
    //    user:Option<String>,
    //    password:Option<String>,
    nodes_addr: Vec<String>,
    graph_name: String,
    session: Option<CurrentSession>,
    _ph: PhantomData<(Id, L)>,
}

impl<Id, L> CassandraGraph<Id, L> {
    pub fn new<S: ToString, SS: ToString>(nodes_addr: Vec<S>, graph_name: SS) -> Self {
        assert!(nodes_addr.len() > 0);

        let nodes_addr: Vec<String> = nodes_addr.into_iter().map(|s| s.to_string()).collect();

        let mut graph = CassandraGraph {
            nodes_addr,
            graph_name: graph_name.to_string(),
            session: None,
            _ph: PhantomData,
        };

        graph.create_session();

        graph
    }

    fn create_session(&mut self) {
        let auth = NoneAuthenticator;

        let mut nodes = Vec::new();
        for addr in self.nodes_addr.iter() {
            let node = NodeTcpConfigBuilder::new(addr, auth.clone()).build();
            nodes.push(node);
        }

        let cluster_config = ClusterTcpConfig(nodes);

        let no_compression: CurrentSession =
            new_session(&cluster_config, RoundRobin::new()).expect("session should be created");

        self.session = Some(no_compression);
    }

    pub fn get_session(&self) -> &CurrentSession {
        self.session.as_ref().unwrap()
    }

    pub fn run_query<S: ToString>(&self, query: S) -> Vec<Row> {
        let session = self.get_session();

        let query = query.to_string();

        trace!("Running '{}'", &query);

        let rows = session
            .query(query)
            .expect("query")
            .get_body()
            .expect("get body")
            .into_rows()
            .expect("into rows");

        rows
    }
}

impl<Id: IdType, L: IdType> GraphTrait<Id, L> for CassandraGraph<Id, L> {
    fn get_node(&self, id: Id) -> NodeType<Id, L> {
        unimplemented!()
    }

    fn get_edge(&self, start: Id, target: Id) -> EdgeType<Id, L> {
        unimplemented!()
    }

    fn has_node(&self, id: Id) -> bool {
        let cql = format!(
            "SELECT id FROM {}.graph WHERE id={};",
            self.graph_name,
            id.id()
        );
        let rows = self.run_query(cql);

        rows.len() > 0
    }

    fn has_edge(&self, start: Id, target: Id) -> bool {
        unimplemented!()
    }

    fn node_count(&self) -> usize {
        let cql = format!("SELECT COUNT(id) FROM {}.graph;", self.graph_name);
        let rows = self.run_query(cql);

        let first_row = rows.into_iter().next().unwrap();
        let count: i64 = first_row.get_by_index(0).expect("get by index").unwrap();

        count as usize
    }

    fn edge_count(&self) -> usize {
        unimplemented!()
    }

    fn is_directed(&self) -> bool {
        false
    }

    fn node_indices(&self) -> Iter<Id> {
        unimplemented!()
    }

    fn edge_indices(&self) -> Iter<(Id, Id)> {
        unimplemented!()
    }

    fn nodes(&self) -> Iter<NodeType<Id, L>> {
        unimplemented!()
    }

    fn edges(&self) -> Iter<EdgeType<Id, L>> {
        unimplemented!()
    }

    fn degree(&self, id: Id) -> usize {
        unimplemented!()
    }

    fn total_degree(&self, id: Id) -> usize {
        unimplemented!()
    }

    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        unimplemented!()
    }

    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        unimplemented!()
    }

    fn max_seen_id(&self) -> Option<Id> {
        unimplemented!()
    }

    fn implementation(&self) -> GraphImpl {
        unimplemented!()
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GraphLabelTrait<Id, NL, EL, L>
    for CassandraGraph<Id, L>
{
    fn get_node_label_map(&self) -> &SetMap<NL> {
        unimplemented!()
    }

    fn get_edge_label_map(&self) -> &SetMap<EL> {
        unimplemented!()
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GeneralGraph<Id, NL, EL, L>
    for CassandraGraph<Id, L>
{
    fn as_graph(&self) -> &GraphTrait<Id, L> {
        self
    }

    fn as_labeled_graph(&self) -> &GraphLabelTrait<Id, NL, EL, L> {
        unimplemented!()
    }

    fn as_general_graph(&self) -> &GeneralGraph<Id, NL, EL, L> {
        self
    }
}
