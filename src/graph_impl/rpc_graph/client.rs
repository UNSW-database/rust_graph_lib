use std::borrow::Cow;
use std::cell::{RefCell, RefMut};
use std::fs;
use std::hash::Hash;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use fxhash::FxBuildHasher;
use lru::LruCache;
use tarpc::{
    client::{self, NewClient},
    context,
};
use tarpc_bincode_transport as bincode_transport;
use tokio::runtime::current_thread::Runtime as CurrentRuntime;

use crate::generic::{DefaultId, Void};
use crate::generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType};
use crate::graph_impl::rpc_graph::server::{GraphRPC, GraphRPCClient};
use crate::graph_impl::GraphImpl;
use crate::graph_impl::UnStaticGraph;
use crate::map::SetMap;
use std::path::Path;
use std::time::{Duration, Instant};

type DefaultGraph = UnStaticGraph<Void>;
type FxLruCache<K, V> = LruCache<K, V, FxBuildHasher>;

pub struct GraphClient {
    graph: Arc<DefaultGraph>,
    server_addrs: Vec<SocketAddr>,
    runtime: RefCell<CurrentRuntime>,
    clients: Vec<Option<RefCell<GraphRPCClient>>>,
    cache: RefCell<FxLruCache<DefaultId, Vec<DefaultId>>>,
    workers: usize,
    peers: usize,
    processor: usize,
    cache_hits: RefCell<usize>,
    rpc_queries: RefCell<usize>,
    requests: RefCell<usize>,
    rpc_time: RefCell<Duration>,
}

impl GraphClient {
    pub fn new<P: AsRef<Path>>(
        graph: Arc<DefaultGraph>,
        cache_size: usize,
        port: u16,
        workers: usize,
        machines: usize,
        processor: usize,
        path_to_hosts: P,
    ) -> Self {
        let hosts_str = fs::read_to_string(path_to_hosts).unwrap();
        let hosts = parse_hosts(hosts_str, machines);
        let server_addrs = init_address(hosts, port);

        let cache = RefCell::new(FxLruCache::with_hasher(
            cache_size,
            FxBuildHasher::default(),
        ));

        let mut client = GraphClient {
            graph,
            server_addrs,
            runtime: RefCell::new(
                CurrentRuntime::new()
                    .unwrap_or_else(|e| panic!("Fail to create a runtime {:?} ", e)),
            ),
            clients: vec![],
            cache,
            workers,
            peers: workers * machines,
            processor,
            cache_hits: RefCell::new(0),
            rpc_queries: RefCell::new(0),
            requests: RefCell::new(0),
            rpc_time: RefCell::new(Duration::new(0, 0)),
        };
        client.create_clients();

        client
    }

    fn create_clients(&mut self) {
        for (i, addr) in self.server_addrs.iter().enumerate() {
            let client = if i == self.processor {
                None
            } else {
                let transport = self
                    .runtime
                    .borrow_mut()
                    .block_on(async move {
                        info!("Connecting to {:?}", addr);
                        bincode_transport::connect(&addr).await
                    })
                    .unwrap_or_else(|e| panic!("Fail to connect to {:}: {:}", addr, e));

                let NewClient { client, dispatch } =
                    GraphRPCClient::new(client::Config::default(), transport);

                self.runtime.borrow_mut().spawn(async move {
                    if let Err(e) = dispatch.await {
                        panic!("Error while running client dispatch: {:?}", e)
                    }
                });

                Some(RefCell::new(client))
            };

            self.clients.push(client);
        }
    }

    #[inline(always)]
    fn is_local(&self, id: DefaultId) -> bool {
        id.id() % self.peers / self.workers == self.processor
    }

    #[inline(always)]
    fn get_client_id(&self, id: DefaultId) -> usize {
        id.id() % self.peers / self.workers
    }

    #[inline(always)]
    fn get_client(&self, id: DefaultId) -> RefMut<GraphRPCClient> {
        let idx = self.get_client_id(id);
        let client = &self.clients[idx];

        client.as_ref().unwrap().borrow_mut()
    }

    #[inline]
    async fn query_neighbors_async(&self, id: DefaultId) -> Vec<DefaultId> {
        let mut client = self.get_client(id);
        let vec = client
            .neighbors(context::current(), id)
            .await
            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));

        vec
    }

    #[inline]
    fn query_neighbors(&self, id: DefaultId) -> Vec<DefaultId> {
        let start = Instant::now();
        let neighbors = self
            .runtime
            .borrow_mut()
            .block_on(async move { self.query_neighbors_async(id).await });
        let duration = start.elapsed();
        *self.rpc_time.borrow_mut() += duration;

        neighbors
    }

    //    #[inline]
    //    async fn query_neighbors_batch_async(&self, client_id: usize, ids:Vec<DefaultId>) -> Vec<Vec<DefaultId>> {
    //
    //
    //
    //
    //        let mut client = self.get_client(client_id);
    //        let vec = client
    //            .neighbors_batch(context::current(), ids)
    //            .await
    //            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));
    //
    //        vec
    //    }

    //    #[inline]
    //    async fn query_degree_async(&self, id: DefaultId) -> usize {
    //        let mut client = self.get_client(id);
    //        let degree = client
    //            .degree(context::current(), id)
    //            .await
    //            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));
    //
    //        degree
    //    }
    //
    //    #[inline]
    //    fn query_degree(&self, id: DefaultId) -> usize {
    //        self.runtime
    //            .borrow_mut()
    //            .block_on(async move { self.query_degree_async(id).await })
    //    }

    #[inline]
    fn request(&self) {
        *self.requests.borrow_mut() += 1;
    }

    pub fn cache_length(&self) -> usize {
        self.cache.borrow().len()
    }

    pub fn status(&self) -> String {
        format!(
            "#requests: {}, #rpc:{}, #cache hits: {}, #cache length: {}, rpc time: {:?}",
            *self.requests.borrow(),
            *self.rpc_queries.borrow(),
            *self.cache_hits.borrow(),
            self.cache_length(),
            self.rpc_time.clone().into_inner()
        )
        .to_string()
    }
}

fn parse_hosts<S: ToString>(s: S, n: usize) -> Vec<SocketAddr> {
    s.to_string()
        .lines()
        .take(n)
        .map(|line| line.trim().to_socket_addrs().unwrap().next().unwrap())
        .collect()
}

fn init_address(addrs: Vec<SocketAddr>, port: u16) -> Vec<SocketAddr> {
    let mut addrs = addrs;
    for addr in addrs.iter_mut() {
        addr.set_port(port);
    }

    addrs
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
        self.request();

        if self.is_local(start) {
            return self.graph.has_edge(start, target);
        }

        if self.is_local(target) {
            return self.graph.has_edge(target, start);
        }

        if let Some(cached_result) = self
            .cache
            .borrow_mut()
            .get(&start)
            .map(|x| x.contains(&target))
        {
            *self.cache_hits.borrow_mut() += 1;
            return cached_result;
        }

        if let Some(cached_result) = self
            .cache
            .borrow_mut()
            .get(&target)
            .map(|x| x.contains(&start))
        {
            *self.cache_hits.borrow_mut() += 1;
            return cached_result;
        }

        *self.rpc_queries.borrow_mut() += 1;

        let neighbors = self.query_neighbors(start);
        let result = neighbors.contains(&target);

        self.cache.borrow_mut().put(start, neighbors);

        result
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
        self.request();

        if self.is_local(id) {
            return self.graph.degree(id);
        }

        if self.cache.borrow().contains(&id) {
            *self.cache_hits.borrow_mut() += 1;
            return self.cache.borrow_mut().get(&id).unwrap().len();
        }

        *self.rpc_queries.borrow_mut() += 1;
        let neighbors = self.query_neighbors(id);
        let degree = neighbors.len();
        self.cache.borrow_mut().put(id, neighbors);

        degree
    }

    fn total_degree(&self, id: u32) -> usize {
        unimplemented!()
    }

    fn neighbors_iter(&self, id: u32) -> Iter<u32> {
        unimplemented!()
    }

    fn neighbors(&self, id: u32) -> Cow<[u32]> {
        self.request();

        if self.is_local(id) {
            return self.graph.neighbors(id);
        }

        if self.cache.borrow().contains(&id) {
            *self.cache_hits.borrow_mut() += 1;
            let cached = self.cache.borrow_mut().get(&id).unwrap().clone();
            return cached.into();
        }

        *self.rpc_queries.borrow_mut() += 1;
        let neighbors = self.query_neighbors(id);
        self.cache.borrow_mut().put(id, neighbors.clone());

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
