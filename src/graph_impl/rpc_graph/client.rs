use std::borrow::{Borrow, Cow};
use std::cell::{RefCell, RefMut};
use std::fs;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::future::Future;
use futures::stream::Stream;
use fxhash::FxBuildHasher;
use lru::LruCache;
use tokio::io::AsyncRead;
use tokio::runtime::current_thread::Runtime as CurrentRuntime;

use crate::generic::{DefaultId, Void};
use crate::generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType};
use crate::graph_capnp;
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
    clients: Vec<Option<graph_capnp::graph::Client>>,
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

        Self::new_from_addrs(
            graph,
            cache_size,
            workers,
            machines,
            processor,
            server_addrs,
        )
    }

    pub fn new_from_addrs(
        graph: Arc<DefaultGraph>,
        cache_size: usize,
        workers: usize,
        machines: usize,
        processor: usize,
        server_addrs: Vec<SocketAddr>,
    ) -> Self {
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
        let mut runtime = self.runtime.borrow_mut();

        for (i, addr) in self.server_addrs.iter().enumerate() {
            let client = if i == self.processor {
                None
            } else {
                let stream = runtime
                    .block_on(tokio::net::TcpStream::connect(&addr))
                    .unwrap();
                stream.set_nodelay(true).unwrap();
                let (reader, writer) = stream.split();

                let network = Box::new(twoparty::VatNetwork::new(
                    reader,
                    std::io::BufWriter::new(writer),
                    rpc_twoparty_capnp::Side::Client,
                    Default::default(),
                ));
                let mut rpc_system = RpcSystem::new(network, None);
                let client: graph_capnp::graph::Client =
                    rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
                runtime.spawn(rpc_system.map_err(|_e| ()));

                Some(client)
            };

            self.clients.push(client);
        }
    }

    #[inline(always)]
    fn is_local(&self, id: DefaultId) -> bool {
        id.id() % self.peers / self.workers == self.processor
    }

    #[inline(always)]
    fn get_client(&self, id: DefaultId) -> &graph_capnp::graph::Client {
        let idx = id.id() % self.peers / self.workers;
        let client = &self.clients[idx];

        client.as_ref().unwrap()
    }

    #[inline]
    pub fn query_neighbors(&self, id: DefaultId) -> Vec<DefaultId> {
        let mut runtime = self.runtime.borrow_mut();

        let client = self.get_client(id);
        let mut request = client.neighbors_request();
        request.get().set_x(id);

        let promise = request.send().promise.and_then(|response| {
            let reader = response.get()?.get_y()?;
            let vec = reader.iter().collect::<Vec<_>>();

            Ok(vec)
        });

        runtime.block_on(promise).unwrap()
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

impl<NL: Hash + Eq, EL: Hash + Eq> GraphLabelTrait<DefaultId, NL, EL, DefaultId> for GraphClient {
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
