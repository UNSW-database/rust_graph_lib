pub mod concurrent_cache;

use std::borrow::Cow;
use std::cell::RefCell;
use std::hash::Hash;
use std::marker::PhantomData;

use cdrs::authenticators::NoneAuthenticator;
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
use lru::LruCache;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType};
use crate::graph_impl::cassandra_graph::concurrent_cache::ConcurrentCache;
use crate::graph_impl::GraphImpl;
use crate::map::SetMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;
type FxLruCache<K, V> = LruCache<K, V, FxBuildHasher>;

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
    adj: Vec<i32>,
}


pub struct CassandraGraph<Id: IdType, L = Id> {
    //    user:Option<String>,
    //    password:Option<String>,
    id: usize,
    nodes_addr: Vec<String>,
    graph_name: String,
    session: Option<CurrentSession>,

    node_count: AtomicUsize,
    max_node_id: AtomicUsize,
    cache: Arc<ConcurrentCache<Id>>,

    get_time: RefCell<Duration>,
    query_time: RefCell<Duration>,
    put_time: RefCell<Duration>,
    query_count: RefCell<usize>,

    _ph: PhantomData<(Id, L)>,
}

impl<Id: IdType + Clone, L: IdType> CassandraGraph<Id, L> {
    pub fn new<S: ToString, SS: ToString>(
        nodes_addr: Vec<S>,
        id: usize,
        graph_name: SS,
        shared_cache: Arc<ConcurrentCache<Id>>,
    ) -> Self {
        assert!(nodes_addr.len() > 0);

        let nodes_addr: Vec<String> = nodes_addr.into_iter().map(|s| s.to_string()).collect();

        let mut graph = CassandraGraph {
            nodes_addr,
            id,
            graph_name: graph_name.to_string(),
            session: None,
            node_count: AtomicUsize::new(std::usize::MAX),
            max_node_id: AtomicUsize::new(std::usize::MAX),
            cache: shared_cache,
            get_time: RefCell::new(Duration::new(0, 0)),
            query_time: RefCell::new(Duration::new(0, 0)),
            put_time: RefCell::new(Duration::new(0, 0)),
            query_count: RefCell::new(0usize),
            _ph: PhantomData,
        };

        graph.create_session();

        graph
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.cache.get_hits() as f64;
        let misses = self.cache.get_misses() as f64;
        hits / (hits + misses)
    }

    pub fn cache_capacity(&self) -> usize {
        self.cache.get_capacity()
    }

    pub fn cache_length(&self) -> usize {
        self.cache.get_len()
    }

    pub fn get_query_time(&self) -> Duration {
        self.query_time.clone().into_inner()
    }

    pub fn get_get_time(&self) -> Duration {
        self.get_time.clone().into_inner()
    }

    pub fn get_put_time(&self) -> Duration {
        self.put_time.clone().into_inner()
    }

    pub fn get_query_count(&self) -> usize {
        self.query_count.clone().into_inner()
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

        info!("Cassandra session established");
    }

    fn get_session(&self) -> &CurrentSession {
        self.session.as_ref().unwrap()
    }

    fn update_time(&self, get_time: Duration, put_time: Duration, query_time: Duration, query_count: usize) {
        *self.get_time.borrow_mut() += get_time;
        *self.put_time.borrow_mut() += put_time;
        *self.query_time.borrow_mut() += query_time;
        *self.query_count.borrow_mut() += query_count;
    }


    #[inline(always)]
    fn run_query<S: ToString>(&self, query: S) -> Vec<Row> {
        let session = self.get_session();
        let query = query.to_string();

        let rows = session
            .query(query)
            .expect("query")
            .get_body()
            .expect("get body")
            .into_rows()
            .expect("into rows");

        rows
    }

    #[inline(always)]
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
                debug!("Id {:?} into RawAdj: {:?}", id, e);

                vec![]
            }
        }
    }

}

impl<Id: IdType, L: IdType> GraphTrait<Id, L> for CassandraGraph<Id, L> {

    fn get_node(&self, _id: Id) -> NodeType<Id, L> {
        unimplemented!()
    }

    fn get_edge(&self, _start: Id, _target: Id) -> EdgeType<Id, L> {
        unimplemented!()
    }


    #[inline(always)]
    fn has_node(&self, id: Id) -> bool {
        id <= self.max_seen_id().unwrap()

        // let cql = format!(
        //     "SELECT id FROM {}.graph WHERE id={};",
        //     self.graph_name,
        //     id.id()
        // );
        // let rows = self.run_query(cql);
        //
        // rows.len() > 0
    }

    #[inline(always)]
    fn has_edge(&self, start: Id, target: Id) -> bool {
        let mut timer = Instant::now();

        if let Some(has_edge) = self.cache.has_edge(&start, &target) {
            self.update_time(timer.elapsed(),
                             Duration::new(0, 0),
                             Duration::new(0, 0),
                             0);

            return has_edge;
        }

        let get_time = timer.elapsed();

        timer = Instant::now();
        let neighbors = self.query_neighbors(&start);
        let query_time = timer.elapsed();

        let ans = neighbors.binary_search(&target).is_ok();

        timer = Instant::now();
        self.cache.put(start, neighbors);
        let put_time = timer.elapsed();

        self.update_time(get_time, put_time, query_time, 1usize);

        ans
    }

    #[inline(always)]
    fn node_count(&self) -> usize {
        if self.node_count.load(Ordering::SeqCst) == std::usize::MAX {
            let cql = format!(
                "SELECT value FROM {}.stats WHERE key = 'node_count'",
                self.graph_name
            );
            let rows = self.run_query(cql);

            let first_row = rows.into_iter().next().unwrap();
            let count: i32 = first_row.get_by_index(0).expect("get by index").unwrap();

            self.node_count.swap(count as usize, Ordering::Relaxed);
        }

        self.node_count.load(Ordering::SeqCst)
    }

    fn edge_count(&self) -> usize {
        unimplemented!()
    }

    fn is_directed(&self) -> bool {
        false
    }

    #[inline(always)]
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

    #[inline(always)]
    fn degree(&self, id: Id) -> usize {
        let mut timer = Instant::now();

        if let Some(degree) = self.cache.degree(&id) {
            self.update_time(timer.elapsed(),
                             Duration::new(0, 0),
                             Duration::new(0, 0),
                             0);

            return degree;
        }

        let get_time = timer.elapsed();

        timer = Instant::now();
        let neighbors = self.query_neighbors(&id);
        let query_time = timer.elapsed();

        let len = neighbors.len();

        let timer = Instant::now();
        self.cache.put(id, neighbors);
        let put_time = timer.elapsed();

        self.update_time(get_time, put_time, query_time, 1usize);

        len
    }

    fn total_degree(&self, _id: Id) -> usize {
        unimplemented!()
    }

    #[inline(always)]
    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        let neighbors= self.neighbors(id);
        Iter::new(Box::new(neighbors.into_owned().into_iter()))
    }

    #[inline(always)]
    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        let mut timer = Instant::now();

        if let Some(neighbours) = self.cache.get(&id) {
            self.update_time(timer.elapsed(),
                             Duration::new(0, 0),
                             Duration::new(0, 0),
                             0);

            return neighbours.into();
        }

        let get_time = timer.elapsed();

        timer = Instant::now();
        let neighbors = self.query_neighbors(&id);
        let query_time = timer.elapsed();

        timer = Instant::now();
        self.cache.put(id, neighbors.clone());
        let put_time = timer.elapsed();

        self.update_time(get_time, put_time, query_time, 1usize);

        neighbors.into()
    }

    #[inline(always)]
    fn max_seen_id(&self) -> Option<Id> {
        if self.max_node_id.load(Ordering::SeqCst) == std::usize::MAX {
            let cql = format!(
                "SELECT value FROM {}.stats WHERE key = 'node_count'",
                self.graph_name
            );
            let rows = self.run_query(cql);

            let first_row = rows.into_iter().next().unwrap();
            let id: i32 = first_row.get_by_index(0).expect("get by index").unwrap();

            self.max_node_id.swap(id as usize, Ordering::Relaxed);
        }

        Some(Id::new(self.max_node_id.load(Ordering::SeqCst)))
    }

    fn implementation(&self) -> GraphImpl {
        GraphImpl::CassandraGraph
    }
}


impl<Id: IdType, L> Drop for CassandraGraph<Id, L> {
    fn drop(&mut self) {
        info!(
            "CassandraGraph {}: cache length {}",
            self.id,
            self.cache.get_len()
        );
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
    fn as_graph(&self) -> &dyn GraphTrait<Id, L> {
        self
    }

    fn as_labeled_graph(&self) -> &dyn GraphLabelTrait<Id, NL, EL, L> {
        unimplemented!()
    }

    fn as_general_graph(&self) -> &dyn GeneralGraph<Id, NL, EL, L> {
        self
    }
}