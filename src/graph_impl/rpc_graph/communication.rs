use std::borrow::Cow;
use std::fs;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;

use lru::LruCache;
use parking_lot::Mutex;
use tarpc::{
    client::{self, NewClient},
    context,
};
use tarpc_bincode_transport as bincode_transport;

use crate::generic::{DefaultId, IdType};
use crate::graph_impl::rpc_graph::server::{GraphRPC, GraphRPCClient};

pub struct Messenger {
    server_addrs: Vec<SocketAddr>,
    clients: Vec<Option<GraphRPCClient>>,
    cache: Mutex<LruCache<DefaultId, Vec<DefaultId>>>,
    workers: usize,
    peers: usize,
    processor: usize,

    runtime:tokio::runtime::Runtime,
}

impl Messenger {
    pub fn new<P: AsRef<Path>>(
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

        let cache = Mutex::new(LruCache::new(cache_size));

        let mut messenger = Self {
            server_addrs,
            clients: vec![],
            cache,
            workers,
            processor,
            peers: workers * machines,
            runtime:tokio::runtime::Runtime::new()
                .unwrap_or_else(|e| panic!("Fail to initialize the runtime: {:?}", e))
        };

        messenger.create_clients();

        messenger
    }

    fn create_clients(&mut self) {
        let runtime = &self.runtime;

        for (i, addr) in self.server_addrs.iter().enumerate() {
            let client = if i == self.processor {
                None
            } else {
                let transport = runtime
                    .block_on(async move {
                        info!("Connecting to {:?}", addr);
                        bincode_transport::connect(&addr).await
                    })
                    .unwrap_or_else(|e| panic!("Fail to connect to {:}: {:}", addr, e));

                let NewClient { client, dispatch } =
                    GraphRPCClient::new(client::Config::default(), transport);

                runtime.spawn(async move {
                    if let Err(e) = dispatch.await {
                        panic!("Error while running client dispatch: {:?}", e)
                    }
                });

                Some(client)
            };

            self.clients.push(client);
        }
    }

    #[inline(always)]
    pub fn is_local(&self, id: DefaultId) -> bool {
        id.id() % self.peers / self.workers == self.processor
    }

    #[inline(always)]
    pub fn get_runtime(&self) -> &tokio::runtime::Runtime {
        &self.runtime
    }

    pub fn cache_length(&self) -> usize {
        self.cache.lock().len()
    }

    #[inline(always)]
    fn get_client_id(&self, id: DefaultId) -> usize {
        id.id() % self.peers / self.workers
    }

    #[inline(always)]
    fn get_client(&self, id: DefaultId) -> GraphRPCClient {
        let idx = self.get_client_id(id);
        let client = &self.clients[idx];

        client.clone().unwrap()
    }

    #[inline]
    pub async fn query_neighbors_async(&self, id: DefaultId) -> Vec<DefaultId> {
        {
            let mut cache = self.cache.lock();

            if let Some(cached) = cache.get(&id) {
                return cached.clone();
            }
        }

        let mut client = self.get_client(id);
        let vec = client
            .neighbors(context::current(), id)
            .await
            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));

        {
            let mut cache = self.cache.lock();
            cache.put(id, vec.clone());
        }

        vec
    }

    #[inline]
    pub async fn query_degree_async(&self, id: DefaultId) -> usize {
        {
            let mut cache = self.cache.lock();

            if let Some(cached) = cache.get(&id) {
                return cached.len();
            }
        }

        let mut client = self.get_client(id);
        let vec = client
            .neighbors(context::current(), id)
            .await
            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));
        let degree = vec.len();

        {
            let mut cache = self.cache.lock();
            cache.put(id, vec);
        }

        degree
    }

    #[inline]
    pub async fn has_edge_async(&self, start: DefaultId, target: DefaultId) -> bool {
        {
            let mut cache = self.cache.lock();

            if let Some(cached) = cache.get(&start) {
                return cached.contains(&target);
            }
        }

        let mut client = self.get_client(start);
        let vec = client
            .neighbors(context::current(), start)
            .await
            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));
        let has_edge = vec.contains(&target);

        {
            let mut cache = self.cache.lock();
            cache.put(start, vec);
        }

        has_edge
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
