extern crate lru;

use std::borrow::Cow;
use std::cell::RefCell;
use std::hash::Hash;
use std::marker::PhantomData;

use self::lru::LruCache;
use cdrs::authenticators::{NoneAuthenticator, StaticPasswordAuthenticator};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::frame::IntoBytes;
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;
use cdrs::types::rows::Row;
use cdrs::types::{AsRustType, IntoRustByIndex};
use fxhash::FxBuildHasher;

use generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType};
use graph_impl::GraphImpl;
use map::SetMap;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;
type FxLruCache<K, V> = LruCache<K, V, FxBuildHasher>;

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

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RawAdj {
    adj: Vec<i64>,
}

pub struct CassandraGraph<Id: IdType, L = Id> {
    //    user:Option<String>,
    //    password:Option<String>,
    nodes_addr: Vec<String>,
    graph_name: String,
    session: Option<CurrentSession>,

    node_count: RefCell<Option<usize>>,
    //    edge_count: RefCell<Option<usize>>,
    max_node_id: RefCell<Option<Id>>,

    cache: RefCell<FxLruCache<Id, Vec<Id>>>,
    cache_hits: RefCell<usize>,
    num_of_opts: RefCell<usize>,

    _ph: PhantomData<(Id, L)>,
}

//impl<Id: IdType, L> Clone for CassandraGraph<Id, L> {
//    fn clone(&self) -> Self {
//        let mut new_cache =
//            FxLruCache::with_hasher(self.cache.borrow().cap(), FxBuildHasher::default());
//
//        for (k, v) in self.cache.borrow().iter() {
//            new_cache.put(*k, v.clone());
//        }
//
//        let mut cloned = Self {
//            nodes_addr: self.nodes_addr.clone(),
//            graph_name: self.graph_name.clone(),
//            session: None,
//            node_count: self.node_count.clone(),
////            edge_count: self.edge_count.clone(),
//            max_node_id: self.max_node_id.clone(),
//            cache: RefCell::new(new_cache),
//            cache_hits:0,
//            num_of_opts:0,
//            _ph: PhantomData,
//        };
//
//        cloned.create_session();
//
//        cloned
//    }
//}

impl<Id: IdType + Clone, L> CassandraGraph<Id, L> {
    pub fn new<S: ToString, SS: ToString>(
        nodes_addr: Vec<S>,
        graph_name: SS,
        cache_size: usize,
    ) -> Self {
        assert!(nodes_addr.len() > 0);

        let nodes_addr: Vec<String> = nodes_addr.into_iter().map(|s| s.to_string()).collect();

        let mut graph = CassandraGraph {
            nodes_addr,
            graph_name: graph_name.to_string(),
            session: None,
            node_count: RefCell::new(None),
            //            edge_count: RefCell::new(None),
            max_node_id: RefCell::new(None),
            cache: RefCell::new(FxLruCache::with_hasher(
                cache_size,
                FxBuildHasher::default(),
            )),
            cache_hits: RefCell::new(0),
            num_of_opts: RefCell::new(0),
            _ph: PhantomData,
        };

        graph.create_session();

        graph
    }

    pub fn hit_rate(&self) -> f64 {
        if *self.num_of_opts.borrow() == 0 {
            return 0.;
        }

        (*self.cache_hits.borrow() as f64) / (*self.num_of_opts.borrow() as f64)
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

        info!("Cassandra session established")
    }

    fn get_session(&self) -> &CurrentSession {
        self.session.as_ref().unwrap()
    }

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

    fn query_neighbors(&self, id: &Id) -> Vec<Id> {
        let cql = format!(
            "SELECT adj FROM {}.graph WHERE id={};",
            self.graph_name,
            id.id()
        );
        let rows = self.run_query(cql);

        let first_row_opt = rows.into_iter().next();

        match first_row_opt {
            Some(row) => {
                let raw_adj: RawAdj = RawAdj::try_from_row(row).expect("into RawAdj");
                let neighbors = raw_adj
                    .adj
                    .into_iter()
                    .map(|n| Id::new(n as usize))
                    .collect();

                neighbors
            }
            None => Vec::new(),
        }
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
        *self.num_of_opts.borrow_mut() += 1;

        if self.cache.borrow().contains(&start) {
            *self.cache_hits.borrow_mut() += 1;

            return self
                .cache
                .borrow_mut()
                .get(&start)
                .unwrap()
                .contains(&target);
        }

        let neighbors = self.query_neighbors(&start);
        let ans = neighbors.contains(&target);
        self.cache.borrow_mut().put(start, neighbors);

        ans
    }

    fn node_count(&self) -> usize {
        if self.node_count.borrow().is_none() {
            let cql = format!("SELECT COUNT(id) FROM {}.graph;", self.graph_name);
            let rows = self.run_query(cql);

            let first_row = rows.into_iter().next().unwrap();
            let count: i64 = first_row.get_by_index(0).expect("get by index").unwrap();

            self.node_count.replace(Some(count as usize));
        }

        self.node_count.borrow().unwrap()
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
        *self.num_of_opts.borrow_mut() += 1;

        if self.cache.borrow().contains(&id) {
            *self.cache_hits.borrow_mut() += 1;

            return self.cache.borrow_mut().get(&id).unwrap().len();
        }

        let neighbors = self.query_neighbors(&id);
        let len = neighbors.len();
        self.cache.borrow_mut().put(id, neighbors);

        len
    }

    fn total_degree(&self, id: Id) -> usize {
        unimplemented!()
    }

    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        Iter::new(Box::new(self.neighbors(id).into_owned().into_iter()))
    }

    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        *self.num_of_opts.borrow_mut() += 1;

        if self.cache.borrow().contains(&id) {
            *self.cache_hits.borrow_mut() += 1;

            let cached = self.cache.borrow_mut().get(&id).unwrap().clone();

            return cached.into();
        }

        let neighbors = self.query_neighbors(&id);
        self.cache.borrow_mut().put(id, neighbors.clone());

        neighbors.into()
    }

    fn max_seen_id(&self) -> Option<Id> {
        if self.max_node_id.borrow().is_none() {
            let cql = format!("SELECT MAX(id) FROM {}.graph;", self.graph_name);
            let rows = self.run_query(cql);

            let first_row = rows.into_iter().next().unwrap();
            let id: i64 = first_row.get_by_index(0).expect("get by index").unwrap();

            self.max_node_id.replace(Some(Id::new(id as usize)));
        }

        *self.max_node_id.borrow()
    }

    fn implementation(&self) -> GraphImpl {
        GraphImpl::CassandraGraph
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
