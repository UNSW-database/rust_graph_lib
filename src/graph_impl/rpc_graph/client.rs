use std::borrow::Cow;
use std::cell::{RefCell, RefMut};
use std::fs;
use std::hash::Hash;
use std::net::SocketAddr;
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
    //    hits: RefCell<usize>,
    //    requests: RefCell<usize>,
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
                        println!("Connecting tp {:?}", addr);
                        bincode_transport::connect(&addr).await
                    })
                    .unwrap_or_else(|e| panic!("Fail to connect to {:}: {:}", addr, e));
                println!("Connected");

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
    fn get_client(&self, id: DefaultId) -> RefMut<GraphRPCClient> {
        let idx = id.id() % self.peers / self.workers;
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
        self.runtime
            .borrow_mut()
            .block_on(async move { self.query_neighbors_async(id).await })
    }
}

fn parse_hosts<S: ToString>(s: S, n: usize) -> Vec<SocketAddr> {
    s.to_string()
        .lines()
        .take(n)
        .map(|line| line.parse().unwrap())
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
        unimplemented!()
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

        if self.cache.borrow().contains(&id) {
            //            *self.hits.borrow_mut() += 1;

            return self.cache.borrow_mut().get(&id).unwrap().len();
        }

        let neighbors = self.query_neighbors(id);
        let len = neighbors.len();
        self.cache.borrow_mut().put(id, neighbors);

        len
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

        if self.cache.borrow().contains(&id) {
            //            *self.hits.borrow_mut() += 1;

            let cached = self.cache.borrow_mut().get(&id).unwrap().clone();

            return cached.into();
        }

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

impl<NL: Hash + Eq, EL: Hash + Eq> GraphLabelTrait<DefaultId, NL, EL, DefaultId>
    for GraphClient
{
    fn get_node_label_map(&self) -> &SetMap<NL> {
        unimplemented!()
    }

    fn get_edge_label_map(&self) -> &SetMap<EL> {
        unimplemented!()
    }
}

impl<NL: Hash + Eq, EL: Hash + Eq> GeneralGraph<DefaultId, NL, EL, DefaultId> for GraphClient {
    fn as_graph(&self) -> &GraphTrait<u32, u32> {
        self
    }

    fn as_labeled_graph(&self) -> &GraphLabelTrait<u32, NL, EL, u32> {
        unimplemented!()
    }

    fn as_general_graph(&self) -> &GeneralGraph<u32, NL, EL, u32> {
        self
    }
}
