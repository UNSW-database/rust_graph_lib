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
    id: usize,
    core: Arc<CassandraCore<Id, L>>,
    get_time: RefCell<Duration>,
    query_time: RefCell<Duration>,
    put_time: RefCell<Duration>,
    cache: RefCell<LruCache<Id, Vec<Id>>>,
    query_count: RefCell<usize>,
}

impl<Id: IdType + Clone, L: IdType> CassandraGraph<Id, L> {
    pub fn new(id: usize, core: Arc<CassandraCore<Id, L>>, cache_size: usize) -> Self {
        Self {
            id,
            core,
            get_time: RefCell::new(Duration::new(0, 0)),
            query_time: RefCell::new(Duration::new(0, 0)),
            put_time: RefCell::new(Duration::new(0, 0)),
            cache: RefCell::new(LruCache::new(cache_size)),
            query_count: RefCell::new(0usize),
        }
    }

    pub fn hit_rate(&self) -> f64 {
        self.core.hit_rate()
    }

    pub fn cache_capacity(&self) -> usize {
        self.core.cache_capacity()
    }

    pub fn cache_length(&self) -> usize {
        self.core.cache_length()
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
}

impl<Id: IdType, L: IdType> GraphTrait<Id, L> for CassandraGraph<Id, L> {
    fn get_node(&self, _id: Id) -> NodeType<Id, L> {
        unimplemented!()
    }

    fn get_edge(&self, _start: Id, _target: Id) -> EdgeType<Id, L> {
        unimplemented!()
    }

    fn has_node(&self, id: Id) -> bool {
        self.core.has_node(id)
    }

    fn has_edge(&self, start: Id, target: Id) -> bool {
        if let Some(neighbors) = self.cache.borrow_mut().get(&start) {
            return neighbors.binary_search(&target).is_ok();
        }

        let (ret, get_time, put_time, query_time, queried) = self.core.has_edge(start, target);
        *self.get_time.borrow_mut() += get_time;
        *self.put_time.borrow_mut() += put_time;
        *self.query_time.borrow_mut() += query_time;
        if queried {
            *self.query_count.borrow_mut() += 1;
        }

        ret
    }

    fn node_count(&self) -> usize {
        self.core.node_count()
    }

    fn edge_count(&self) -> usize {
        unimplemented!()
    }

    fn is_directed(&self) -> bool {
        self.core.is_directed()
    }

    fn node_indices(&self) -> Iter<Id> {
        self.core.node_indices()
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
        if let Some(neighbors) = self.cache.borrow_mut().get(&id) {
            return neighbors.len();
        }

        let (ret, get_time, put_time, query_time, queried) = self.core.degree(id);

        *self.get_time.borrow_mut() += get_time;
        *self.put_time.borrow_mut() += put_time;
        *self.query_time.borrow_mut() += query_time;
        if queried {
            *self.query_count.borrow_mut() += 1;
        }

        ret
    }

    fn total_degree(&self, _id: Id) -> usize {
        unimplemented!()
    }

    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        let iter = self.neighbors(id).to_vec().into_iter();

        Iter::new(Box::new(iter))
    }

    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        if let Some(neighbors) = self.cache.borrow_mut().get(&id) {
            return neighbors.clone().into();
        }

        let (ret, get_time, put_time, query_time, queried) = self.core.neighbors(id);
        self.cache.borrow_mut().put(id, ret.clone());

        *self.get_time.borrow_mut() += get_time;
        *self.put_time.borrow_mut() += put_time;
        *self.query_time.borrow_mut() += query_time;
        if queried {
            *self.query_count.borrow_mut() += 1;
        }

        ret.into()
    }

    fn max_seen_id(&self) -> Option<Id> {
        self.core.max_seen_id()
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

pub struct CassandraCore<Id: IdType, L = Id> {
    //    user:Option<String>,
    //    password:Option<String>,
    nodes_addr: Vec<String>,
    graph_name: String,
    session: Option<CurrentSession>,

    node_count: AtomicUsize,
    max_node_id: AtomicUsize,
    cache: ConcurrentCache<Id>,

    _ph: PhantomData<(Id, L)>,
}

impl<Id: IdType + Clone, L: IdType> CassandraCore<Id, L> {
    pub fn new<S: ToString, SS: ToString>(
        nodes_addr: Vec<S>,
        graph_name: SS,
        page_num: usize,
        page_size: usize,
    ) -> Self {
        assert!(nodes_addr.len() > 0);

        let nodes_addr: Vec<String> = nodes_addr.into_iter().map(|s| s.to_string()).collect();

        let mut graph = CassandraCore {
            nodes_addr,
            graph_name: graph_name.to_string(),
            session: None,
            node_count: AtomicUsize::new(std::usize::MAX),
            max_node_id: AtomicUsize::new(std::usize::MAX),
            cache: ConcurrentCache::new(page_num, page_size),
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
                error!("Id {:?} into RawAdj: {:?}", id, e);

                vec![]
            }
        }
    }

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
    fn has_edge(&self, start: Id, target: Id) -> (bool, Duration, Duration, Duration, bool) {
        let mut timer = Instant::now();

        if let Some(has_edge) = self.cache.has_edge(&start, &target) {
            return (
                has_edge,
                timer.elapsed(),
                Duration::new(0, 0),
                Duration::new(0, 0),
                false,
            );
        }
        let get_time = timer.elapsed();
        timer = Instant::now();
        let neighbors = self.query_neighbors(&start);
        let query_time = timer.elapsed();
        let ans = neighbors
            .binary_search_by(|v| v.partial_cmp(&target).expect("Couldn't compare values"))
            .is_ok();
        timer = Instant::now();
        self.cache.put(start, neighbors);
        let put_time = timer.elapsed();

        (ans, get_time, put_time, query_time, true)
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
    fn degree(&self, id: Id) -> (usize, Duration, Duration, Duration, bool) {
        let mut timer = Instant::now();
        if let Some(degree) = self.cache.degree(&id) {
            return (
                degree,
                timer.elapsed(),
                Duration::new(0, 0),
                Duration::new(0, 0),
                false,
            );
        }
        let get_time = timer.elapsed();
        timer = Instant::now();
        let neighbors = self.query_neighbors(&id);
        let query_time = timer.elapsed();
        let len = neighbors.len();
        let timer = Instant::now();
        self.cache.put(id, neighbors);
        let put_time = timer.elapsed();

        (len, get_time, put_time, query_time, true)
    }

    fn total_degree(&self, _id: Id) -> usize {
        unimplemented!()
    }

    // #[inline(always)]
    // fn neighbors_iter(&self, id: Id) -> (Iter<Id>, Duration, Duration, Duration) {
    //     let (neighbors, get_time, put_time, query_time) = self.neighbors(id);
    //     (
    //         Iter::new(Box::new(neighbors.into_owned().into_iter())),
    //         get_time,
    //         put_time,
    //         query_time,
    //     )
    // }

    #[inline(always)]
    fn neighbors(&self, id: Id) -> (Vec<Id>, Duration, Duration, Duration, bool) {
        let mut timer = Instant::now();

        if let Some(neighbours) = self.cache.get(&id) {
            return (
                neighbours.into(),
                timer.elapsed(),
                Duration::new(0, 0),
                Duration::new(0, 0),
                false,
            );
        }
        let get_time = timer.elapsed();
        timer = Instant::now();
        let neighbors = self.query_neighbors(&id);
        let query_time = timer.elapsed();
        timer = Instant::now();
        self.cache.put(id, neighbors.clone());
        let put_time = timer.elapsed();

        (neighbors, get_time, put_time, query_time, true)
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
