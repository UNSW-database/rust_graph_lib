use std::borrow::Cow;
use std::cell::RefCell;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;

use crate::generic::{DefaultId, Void};
use crate::generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, Iter, NodeType};
use crate::graph_impl::rpc_graph::communication::Messenger;
use crate::graph_impl::GraphImpl;
use crate::graph_impl::UnStaticGraph;
use crate::map::SetMap;

type DefaultGraph = UnStaticGraph<Void>;

pub struct GraphClient {
    graph: Arc<DefaultGraph>,
    messenger: Arc<Messenger>,
    cache: RefCell<LruCache<DefaultId, Vec<DefaultId>>>,

    rpc_time: RefCell<Duration>,
    clone_time: RefCell<Duration>,
    put_time: RefCell<Duration>,
    get_time: RefCell<Duration>,

    cache_hits: RefCell<usize>,
    cache_misses: RefCell<usize>,
    local_hits: RefCell<usize>,
}

impl GraphClient {
    pub fn new(graph: Arc<DefaultGraph>, messenger: Arc<Messenger>, cache_size: usize) -> Self {
        let cache = RefCell::new(LruCache::new(cache_size));

        let client = GraphClient {
            graph,
            messenger,
            cache,

            rpc_time: RefCell::new(Duration::new(0, 0)),
            clone_time: RefCell::new(Duration::new(0, 0)),
            put_time: RefCell::new(Duration::new(0, 0)),
            get_time: RefCell::new(Duration::new(0, 0)),

            cache_hits: RefCell::new(0),
            cache_misses: RefCell::new(0),
            local_hits: RefCell::new(0),
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
    fn query_neighbors_rpc(&self, id: DefaultId, pre_fetch: bool) -> Vec<DefaultId> {
        let messenger = &self.messenger;

        let start_time = Instant::now();

        let neighbors = self
            .get_runtime()
            .block_on(async move { messenger.query_neighbors_async(id, pre_fetch).await });

        let duration = start_time.elapsed();
        *self.rpc_time.borrow_mut() += duration;

        neighbors
    }

    //    #[inline]
    //    fn query_degree_rpc(&self, id: DefaultId) -> usize {
    //        let messenger = &self.messenger;
    //
    //        let start_time = Instant::now();
    //
    //        let degree = self
    //            .get_runtime()
    //            .block_on(async move { messenger.query_degree_async(id).await });
    //
    //        let duration = start_time.elapsed();
    //        *self.rpc_time.borrow_mut() += duration;
    //
    //        degree
    //    }

    //    #[inline]
    //    fn has_edge_rpc(&self, start: DefaultId, target: DefaultId) -> bool {
    //        let messenger = &self.messenger;
    //
    //        let start_time = Instant::now();
    //
    //        let has_edge = self
    //            .get_runtime()
    //            .block_on(async move { messenger.has_edge_async(start, target).await });
    //
    //        let duration = start_time.elapsed();
    //        *self.rpc_time.borrow_mut() += duration;
    //
    //        has_edge
    //    }

    pub fn status(&self) -> String {
        let cache_hits = self.cache_hits.clone().into_inner();
        let cache_misses = self.cache_misses.clone().into_inner();
        let local_hits = self.local_hits.clone().into_inner();
        let hits_rate = cache_hits as f64 / (cache_hits + cache_misses) as f64;

        format!(
            "rpc time: {:?}, clone time {:?}, put time: {:?}, get time: {:?}, local cache length: {}, cache_hits: {}, cache_misses: {}, local_hits: {}, hits_rate: {}",
            self.rpc_time.clone().into_inner(),
            self.clone_time.clone().into_inner(),
            self.put_time.clone().into_inner(),
            self.get_time.clone().into_inner(),

            self.cache.borrow().len(),
            cache_hits,cache_misses,local_hits,hits_rate
        )
        .to_string()
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
            //            *self.local_hits.borrow_mut() += 1;
            return self.graph.has_edge(start, target);
        }

        if self.is_local(target) {
            //            *self.local_hits.borrow_mut() += 1;
            return self.graph.has_edge(target, start);
        }

        let start_time = Instant::now();
        if let Some(cached_result) = self.cache.borrow_mut().get(&start) {
            //            *self.cache_hits.borrow_mut() += 1;

            let duration = start_time.elapsed();
            *self.get_time.borrow_mut() += duration;

            return cached_result.contains(&target);
        }

        let start_time = Instant::now();
        if let Some(cached_result) = self.cache.borrow_mut().get(&target) {
            //            *self.cache_hits.borrow_mut() += 1;

            let duration = start_time.elapsed();
            *self.get_time.borrow_mut() += duration;

            return cached_result.contains(&start);
        }

        //        self.has_edge_rpc(start, target)

        //        *self.cache_misses.borrow_mut() += 1;
        let neighbors = self.query_neighbors_rpc(start, false);
        let has_edge = neighbors.contains(&target);

        let start_time = Instant::now();
        self.cache.borrow_mut().put(start, neighbors);
        let duration = start_time.elapsed();
        *self.put_time.borrow_mut() += duration;

        has_edge
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
        //         assuming a local degree cache
        //        self.graph.degree(id)

        if self.is_local(id) {
            //            *self.local_hits.borrow_mut() += 1;
            return self.graph.degree(id);
        }

        let start_time = Instant::now();
        if let Some(cached_result) = self.cache.borrow_mut().get(&id) {
            //            *self.cache_hits.borrow_mut() += 1;

            let duration = start_time.elapsed();
            *self.get_time.borrow_mut() += duration;

            return cached_result.len();
        }

        //        self.query_degree_rpc(id)

        //        *self.cache_misses.borrow_mut() += 1;
        let neighbors = self.query_neighbors_rpc(id, true);
        let degree = neighbors.len();

        let start_time = Instant::now();
        self.cache.borrow_mut().put(id, neighbors);
        let duration = start_time.elapsed();
        *self.put_time.borrow_mut() += duration;

        degree
    }

    fn total_degree(&self, id: u32) -> usize {
        unimplemented!()
    }

    fn neighbors_iter(&self, id: u32) -> Iter<u32> {
        unimplemented!()
    }

    fn neighbors(&self, id: u32) -> Cow<[u32]> {
        if self.is_local(id) {
            //            *self.local_hits.borrow_mut() += 1;
            return self.graph.neighbors(id);
        }

        let start_time = Instant::now();
        if let Some(cached_result) = self.cache.borrow_mut().get(&id) {
            //            *self.cache_hits.borrow_mut() += 1;

            let duration = start_time.elapsed();
            *self.get_time.borrow_mut() += duration;

            let start = Instant::now();
            let cloned = cached_result.clone();
            let duration = start.elapsed();
            *self.clone_time.borrow_mut() += duration;

            return cloned.into();
        }

        //        *self.cache_misses.borrow_mut() += 1;
        let neighbors = self.query_neighbors_rpc(id, true);

        let start = Instant::now();
        let cloned = neighbors.clone();
        let duration = start.elapsed();
        *self.clone_time.borrow_mut() += duration;

        let start_time = Instant::now();
        self.cache.borrow_mut().put(id, cloned);
        let duration = start_time.elapsed();
        *self.put_time.borrow_mut() += duration;

        neighbors.into()
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
