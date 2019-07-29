extern crate chashmap;

use std::borrow::Cow;
use std::hash::Hash;
use std::marker::PhantomData;

use self::chashmap::CHashMap;
use cdrs::authenticators::{NoneAuthenticator, StaticPasswordAuthenticator};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::frame::IntoBytes;
use cdrs::load_balancing::RoundRobinSync;
use cdrs::query::*;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;
use cdrs::types::rows::Row;
use cdrs::types::{AsRustType, IntoRustByIndex};

use generic::Undirected;
use generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType};
use graph_impl::{GraphImpl, TypedStaticGraph};
use map::SetMap;

type CurrentSession = Session<RoundRobinSync<TcpConnectionPool<NoneAuthenticator>>>;

/// Cassandra scheme:
///     CREATE TABLE graph_name.graph (
///         id bigint PRIMARY KEY,
///         adj list<bigint>
///     );
///
///     CREATE TABLE graph_name.stats (
///         key text PRIMARY KEY,
///         value bigint
///     );
//#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
//struct RawNode {
//    id: i64,
//    adj: Vec<i64>,
//}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RawAdj {
    adj: Vec<i64>,
}

pub struct CassandraGraph<Id: IdType, N: Hash + Eq, E: Hash + Eq, L: IdType = Id> {
    nodes_addr: Vec<String>,
    graph_name: String,
    session: Option<CurrentSession>,
    node_count: Option<usize>,
    max_node_id: Option<Id>,
    cache: CHashMap<Id, Vec<Id>>,

    workers: usize,
    machines: usize,
    processor: usize,
    static_graph: Option<TypedStaticGraph<Id, N, E, Undirected, L>>,

    _ph: PhantomData<L>,
}

impl<Id: IdType, N: Hash + Eq, E: Hash + Eq, L: IdType> CassandraGraph<Id, N, E, L> {
    pub fn new<S: ToString, SS: ToString>(
        nodes_addr: Vec<S>,
        graph_name: SS,
        cache_size: usize,
        workers: usize,
        machines: usize,
        processor: usize,
    ) -> Self {
        assert!(nodes_addr.len() > 0);

        let nodes_addr: Vec<String> = nodes_addr.into_iter().map(|s| s.to_string()).collect();

        let mut graph = CassandraGraph {
            nodes_addr,
            graph_name: graph_name.to_string(),
            session: None,
            node_count: None,
            max_node_id: None,
            cache: CHashMap::with_capacity(cache_size),
            workers,
            machines,
            processor,
            static_graph: None,
            _ph: PhantomData,
        };

        graph.create_session();
        graph.query_node_count();
        graph.query_max_node_id();

        graph
    }

    pub fn set_static_graph(&mut self, graph: TypedStaticGraph<Id, N, E, Undirected, L>) {
        self.static_graph = Some(graph);
    }

    pub fn cache_capacity(&self) -> usize {
        self.cache.capacity()
    }

    pub fn cache_length(&self) -> usize {
        self.cache.len()
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
            new_session(&cluster_config, RoundRobinSync::new()).expect("session should be created");

        self.session = Some(no_compression);

        info!("Cassandra session established");
    }

    #[inline(always)]
    fn get_session(&self) -> &CurrentSession {
        self.session.as_ref().unwrap()
    }

    #[inline]
    fn run_query<S: ToString>(&self, query: S) -> Vec<Row> {
        let session = self.get_session();

        let query = query.to_string();

        //        trace!("Running '{}'", &query);

        let rows = session
            .query(query)
            .expect("query")
            .get_body()
            .expect("get body")
            .into_rows()
            .expect("into rows");

        rows
    }

    #[inline]
    fn query_neighbors(&self, id: &Id) -> Vec<Id> {
        let cql = format!(
            "SELECT adj FROM {}.graph WHERE id={};",
            self.graph_name,
            id.id()
        );
        let rows = self.run_query(cql);

        if rows.len() == 0 {
            return vec![];
        }

        let first_row = rows.into_iter().next().unwrap();
        let result = RawAdj::try_from_row(first_row);

        match result {
            Ok(raw_adj) => {
                let neighbors = raw_adj
                    .adj
                    .into_iter()
                    .map(|n| Id::new(n as usize))
                    .collect();

                neighbors
            }
            Err(e) => {
                error!("Id {:?} into RawAdj: {:?}", id, e);

                vec![]
            }
        }
    }

    fn query_node_count(&mut self) {
        let cql = format!(
            "SELECT value FROM {}.stats WHERE key='node_count';",
            self.graph_name
        );
        let rows = self.run_query(cql);

        let first_row = rows.into_iter().next().unwrap();
        let count: i64 = first_row.get_by_index(0).expect("get by index").unwrap();

        self.node_count = Some(count as usize);
    }

    fn query_max_node_id(&mut self) {
        let cql = format!(
            "SELECT value FROM {}.stats WHERE key='max_node_id';",
            self.graph_name
        );
        let rows = self.run_query(cql);

        let first_row = rows.into_iter().next().unwrap();
        let id: i64 = first_row.get_by_index(0).expect("get by index").unwrap();

        self.max_node_id = Some(Id::new(id as usize));
    }

    #[inline(always)]
    fn get_static_graph(&self) -> &TypedStaticGraph<Id, N, E, Undirected, L> {
        self.static_graph.as_ref().unwrap()
    }

    #[inline(always)]
    fn is_local(&self, id: Id) -> bool {
        if self.static_graph.is_none() {
            return false;
        }

        id.id() % (self.machines * self.workers) / self.workers == self.processor
    }
}

impl<Id: IdType, L: IdType, N: Hash + Eq, E: Hash + Eq> GraphTrait<Id, L>
    for CassandraGraph<Id, N, E, L>
{
    fn get_node(&self, id: Id) -> NodeType<Id, L> {
        unimplemented!()
    }

    fn get_edge(&self, start: Id, target: Id) -> EdgeType<Id, L> {
        unimplemented!()
    }

    fn has_node(&self, id: Id) -> bool {
        id <= self.max_node_id.unwrap()
    }

    fn has_edge(&self, start: Id, target: Id) -> bool {
        if self.is_local(start) {
            return self.get_static_graph().has_edge(start, target);
        }

        if self.is_local(target) {
            return self.get_static_graph().has_edge(target, start);
        }

        let cache = &self.cache;

        let cached_result = cache.get(&start).map(|x| x.contains(&target));

        match cached_result {
            Some(result) => result,
            None => {
                let neighbors = self.query_neighbors(&start);
                let result = neighbors.contains(&target);

                cache.insert_new(start, neighbors);

                result
            }
        }
    }

    fn node_count(&self) -> usize {
        self.node_count.unwrap()
    }

    fn edge_count(&self) -> usize {
        unimplemented!()
    }

    fn is_directed(&self) -> bool {
        false
    }

    fn node_indices(&self) -> Iter<Id> {
        let max_node_id = self.max_seen_id().unwrap().id();

        Iter::new(Box::new((0..max_node_id).map(|n| Id::new(n))))
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
        if self.is_local(id) {
            return self.get_static_graph().degree(id);
        }

        let cache = &self.cache;

        let cached_result = cache.get(&id).map(|x| x.len());

        match cached_result {
            Some(result) => result,
            None => {
                let neighbors = self.query_neighbors(&id);
                let result = neighbors.len();

                cache.insert_new(id, neighbors);

                result
            }
        }
    }

    fn total_degree(&self, id: Id) -> usize {
        unimplemented!()
    }

    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        Iter::new(Box::new(self.neighbors(id).into_owned().into_iter()))
    }

    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        if self.is_local(id) {
            return self.get_static_graph().neighbors(id);
        }

        let cache = &self.cache;

        let cached_result = cache.get(&id).map(|x| x.clone());

        match cached_result {
            Some(result) => result.into(),
            None => {
                let neighbors = self.query_neighbors(&id);
                let result = neighbors.clone();

                cache.insert_new(id, neighbors);

                result.into()
            }
        }
    }

    fn max_seen_id(&self) -> Option<Id> {
        self.max_node_id
    }

    fn implementation(&self) -> GraphImpl {
        GraphImpl::CassandraGraph
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GraphLabelTrait<Id, NL, EL, L>
    for CassandraGraph<Id, NL, EL, L>
{
    fn get_node_label_map(&self) -> &SetMap<NL> {
        unimplemented!()
    }

    fn get_edge_label_map(&self) -> &SetMap<EL> {
        unimplemented!()
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GeneralGraph<Id, NL, EL, L>
    for CassandraGraph<Id, NL, EL, L>
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
