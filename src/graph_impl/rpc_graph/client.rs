use std::borrow::Cow;
use std::cell::{RefCell, RefMut};
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;

use fxhash::FxBuildHasher;
use lru::LruCache;
use tarpc::{client, context};

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
    clients: Vec<Option<RefCell<GraphRPCClient>>>,
    cache: RefCell<FxLruCache<DefaultId, Vec<DefaultId>>>,
    workers: usize,
    machines: usize,
    peers: usize,
    processor: usize,
    //    hits: RefCell<usize>,
    //    requests: RefCell<usize>,
}
//
//impl GraphRPCClient{
//    pub fn new<P:AsRef<Path>>(graph:Arc<DefaultGraph>,
//               cache_size: usize,
//               workers: usize,
//               machines: usize,
//               processor: usize, path_to_hosts:P)->Self{
//        unimplemented!()
//    }
//}

impl GraphClient {
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
    async fn query_neighbors(&self, id: DefaultId) -> Vec<DefaultId> {
        let mut client = self.get_client(id);
        let vec = client
            .neighbors(context::current(), 0)
            .await
            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));

        vec
    }
}

fn parse_hosts<S: ToString>(s: S) -> Vec<SocketAddr> {
    s.to_string()
        .lines()
        .map(|line| line.trim().parse().unwrap())
        .collect()
}

fn init_address(addrs: Vec<SocketAddr>, port: u16) -> Vec<SocketAddr> {
    let mut addrs = addrs;
    for addr in addrs.iter_mut() {
        addr.set_port(port);
    }

    addrs
}
