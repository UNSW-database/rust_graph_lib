use std::borrow::Cow;
use std::cell::{RefCell, RefMut};
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;

use fxhash::FxBuildHasher;
use lru::LruCache;
use tarpc::{client::{self, NewClient}, context};
use tokio::runtime::current_thread::{Runtime as CurrentRuntime,self};
use tarpc_bincode_transport as bincode_transport;

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
    runtime : RefCell<CurrentRuntime>,
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
    pub fn new()->Self{
        let cache=RefCell::new(FxLruCache::with_hasher(
            1,
            FxBuildHasher::default(),
        ));

        let mut client = GraphClient{
            graph:Arc::new(DefaultGraph::empty()),
            server_addrs:vec![([127,0,0,1],18888).into()],
            runtime:RefCell::new(CurrentRuntime::new().unwrap_or_else(|e| panic!("Fail to create a runtime {:?} ",e))),
            clients:vec![],
            cache,
            workers:1,
            machines:1,
            peers:1,
            processor:100,
        };
        client.create_clients();


        client
    }

    fn create_clients(&mut self){
        for (i,addr) in self.server_addrs.iter().enumerate(){
            let client =if i==self.processor{
                None
            }else {
                let transport = self.runtime.borrow_mut()
                    .block_on(async move {
                        println!("Connecting tp {:?}",addr);
                        bincode_transport::connect(&addr).await })
                    .unwrap_or_else(|e| panic!("Fail to connect to {:}: {:}",addr,e));
                println!("Connected");

                let NewClient {
                    client,
                    dispatch,
                } = GraphRPCClient::new(client::Config::default(), transport);

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
    pub fn query_neighbors(&self, id: DefaultId) -> Vec<DefaultId> {
        self.runtime.borrow_mut().block_on(async move {
            self.query_neighbors_async(id).await
        })
    }
}

fn parse_hosts<S: ToString>(s: S) -> Vec<SocketAddr> {
    s.to_string()
        .lines()
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
