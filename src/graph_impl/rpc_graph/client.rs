use std::borrow::Cow;
use std::cell::RefCell;
use std::fs;
use std::hash::Hash;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use fxhash::FxBuildHasher;
//use lru::LruCache;
use tarpc::{
    client::{self, NewClient},
    context,
};
//use tokio::runtime::current_thread::Runtime as CurrentRuntime;

use crate::generic::{DefaultId, Void};
use crate::generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType};
use crate::graph_impl::rpc_graph::communication::Messenger;
use crate::graph_impl::rpc_graph::server::{GraphRPC, GraphRPCClient};
use crate::graph_impl::GraphImpl;
use crate::graph_impl::UnStaticGraph;
use crate::map::SetMap;
use std::path::Path;
use std::time::{Duration, Instant};

type DefaultGraph = UnStaticGraph<Void>;
//type FxLruCache<K, V> = LruCache<K, V, FxBuildHasher>;

pub struct GraphClient {
    graph: Arc<DefaultGraph>,
//    runtime: RefCell<CurrentRuntime>,
    messenger: Arc<Messenger>,
    rpc_time: RefCell<Duration>,
}

impl GraphClient {
    pub fn new(graph: Arc<DefaultGraph>, messenger: Arc<Messenger>) -> Self {
        let client = GraphClient {
            graph,
//            runtime: RefCell::new(
//                CurrentRuntime::new()
//                    .unwrap_or_else(|e| panic!("Fail to create a runtime {:?} ", e)),
//            ),
            messenger,
            rpc_time: RefCell::new(Duration::new(0, 0)),
        };

        client
    }

    #[inline(always)]
    fn is_local(&self, id: DefaultId) -> bool {
        self.messenger.is_local(id)
    }

    #[inline(always)]
    fn get_runtime(&self) -> &tokio::runtime::Runtime {
        self.messenger.get_runtime()
    }

    #[inline]
    fn query_neighbors_rpc(&self, id: DefaultId) -> Vec<DefaultId> {
        let messenger = &self.messenger;

        let start_time = Instant::now();

        let neighbors = self
            .get_runtime()
            .block_on(async move { messenger.query_neighbors_async(id).await });

        let duration = start_time.elapsed();
        *self.rpc_time.borrow_mut() += duration;

        neighbors
    }

    #[inline]
    fn query_degree_rpc(&self, id: DefaultId) -> usize {
        let messenger = &self.messenger;

        let start_time = Instant::now();

        let degree = self
            .get_runtime()
            .block_on(async move { messenger.query_degree_async(id).await });

        let duration = start_time.elapsed();
        *self.rpc_time.borrow_mut() += duration;

        degree
    }

    #[inline]
    fn has_edge_rpc(&self, start: DefaultId, target: DefaultId) -> bool {
        let messenger = &self.messenger;

        let start_time = Instant::now();

        let has_edge = self
            .get_runtime()
            .block_on(async move { messenger.has_edge_async(start, target).await });

        let duration = start_time.elapsed();
        *self.rpc_time.borrow_mut() += duration;

        has_edge
    }

    pub fn status(&self) -> String {
        format!("rpc time: {:?}", self.rpc_time.clone().into_inner()).to_string()
    }
}

impl GraphTrait<DefaultId, DefaultId> for GraphClient {
    fn get_node(&self, id: u32) -> NodeType<u32, u32> {
        unimplemented!()
    }

    fn get_edge(&self, start: u32, target: u32) -> EdgeType<u32, u32> {
        unimplemented!()
    }

    fn has_node(&self, id: u32) -> bool {
        self.graph.has_node(id)
    }

    fn has_edge(&self, start: u32, target: u32) -> bool {
        if self.is_local(start) {
            return self.graph.has_edge(start, target);
        }

        if self.is_local(target) {
            return self.graph.has_edge(target, start);
        }

        self.has_edge_rpc(start, target)
    }

    fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    fn edge_count(&self) -> usize {
        unimplemented!()
    }

    fn is_directed(&self) -> bool {
        false
    }

    fn node_indices(&self) -> Iter<u32> {
        self.graph.node_indices()
    }

    fn edge_indices(&self) -> Iter<(u32, u32)> {
        unimplemented!()
    }

    fn nodes(&self) -> Iter<NodeType<u32, u32>> {
        unimplemented!()
    }

    fn edges(&self) -> Iter<EdgeType<u32, u32>> {
        unimplemented!()
    }

    fn degree(&self, id: u32) -> usize {
        if self.is_local(id) {
            return self.graph.degree(id);
        }

       self.query_degree_rpc(id)
    }

    fn total_degree(&self, id: u32) -> usize {
        unimplemented!()
    }

    fn neighbors_iter(&self, id: u32) -> Iter<u32> {
        unimplemented!()
    }

    fn neighbors(&self, id: u32) -> Cow<[u32]> {
        if self.is_local(id) {
            return self.graph.neighbors(id);
        }

        self.query_neighbors_rpc(id).into()
    }

    fn max_seen_id(&self) -> Option<u32> {
        unimplemented!()
    }

    fn implementation(&self) -> GraphImpl {
        unimplemented!()
    }
}

impl<NL: Hash + Eq, EL: Hash + Eq> GraphLabelTrait<DefaultId, NL, EL, DefaultId> for GraphClient {
    fn get_node_label_map(&self) -> &SetMap<NL> {
        unimplemented!()
    }

    fn get_edge_label_map(&self) -> &SetMap<EL> {
        unimplemented!()
    }
}

impl<NL: Hash + Eq, EL: Hash + Eq> GeneralGraph<DefaultId, NL, EL, DefaultId> for GraphClient {
    fn as_graph(&self) -> &dyn GraphTrait<u32, u32> {
        self
    }

    fn as_labeled_graph(&self) -> &dyn GraphLabelTrait<u32, NL, EL, u32> {
        unimplemented!()
    }

    fn as_general_graph(&self) -> &dyn GeneralGraph<u32, NL, EL, u32> {
        self
    }
}
