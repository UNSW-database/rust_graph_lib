use std::fs;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use tarpc::{
    client::{self, NewClient},
    context,
};
use tarpc_bincode_transport as bincode_transport;
#[cfg(feature = "pre_fetch")]
use threadpool::ThreadPool;

use crate::generic::{DefaultId, IdType};
use crate::graph_impl::rpc_graph::server::{GraphRPC, GraphRPCClient};

#[cfg(feature = "pre_fetch")]
const PRE_FETCH_QUEUE_LENGTH: usize = 1_000;
#[cfg(feature = "pre_fetch")]
const PRE_FETCH_SKIP_LENGTH: usize = 0;

pub struct Messenger {
    server_addrs: Vec<SocketAddr>,
    clients: Vec<Option<GraphRPCClient>>,
    caches: Vec<Option<Arc<RwLock<LruCache<DefaultId, Vec<DefaultId>>>>>>,
    workers: usize,
    peers: usize,
    processor: usize,
    cache_size: usize,

    #[cfg(feature = "pre_fetch")]
    pool: Mutex<ThreadPool>,

    runtime: tokio::runtime::Runtime,
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

        let mut messenger = Self {
            server_addrs,
            clients: vec![],
            caches: vec![],
            workers,
            processor,
            peers: workers * machines,
            cache_size,

            #[cfg(feature = "pre_fetch")]
            pool: Mutex::new(ThreadPool::with_name(
                "pre-fetching thread pool".to_owned(),
                num_cpus::get() - workers + 1,
            )),

            runtime: tokio::runtime::Builder::new()
                .core_threads(workers)
                .build()
                .unwrap_or_else(|e| panic!("Fail to initialize the runtime: {:?}", e)),
        };

        messenger.create_clients();

        messenger
    }

    fn create_clients(&mut self) {
        let runtime = &self.runtime;

        let cache_size = self.cache_size / self.peers;

        info!("The size of each cache is {}", cache_size);

        for (i, addr) in self.server_addrs.iter().enumerate() {
            for _ in 0..self.workers {
                let (client, cache) = if i == self.processor {
                    (None, None)
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

                    let cache = RwLock::new(LruCache::new(cache_size));

                    (Some(client), Some(Arc::new(cache)))
                };

                self.clients.push(client);
                self.caches.push(cache);
            }
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
        self.caches
            .iter()
            .map(|x| x.as_ref())
            .filter_map(|x| x)
            .map(|x| x.read().len())
            .sum()
    }

    #[inline(always)]
    fn get_client_id(&self, id: DefaultId) -> usize {
        id.id() % self.peers
    }

    #[inline(always)]
    fn get_client(&self, id: DefaultId) -> GraphRPCClient {
        let idx = self.get_client_id(id);
        let client = self.clients[idx].clone();

        client.unwrap()
    }

    #[inline(always)]
    fn get_cache(&self, id: DefaultId) -> Arc<RwLock<LruCache<DefaultId, Vec<DefaultId>>>> {
        let idx = self.get_client_id(id);
        let cache = self.caches[idx].clone();

        cache.unwrap()
    }

    #[cfg(feature = "pre_fetch")]
    #[inline(always)]
    fn get_pool(&self) -> ThreadPool {
        self.pool.lock().clone()
    }

    #[inline]
    pub async fn query_neighbors_async(&self, id: DefaultId) -> Vec<DefaultId> {
        let cache = self.get_cache(id);

        if cache.read().contains(&id) {
            let mut cache = cache.write();

            let cached = cache.get(&id).unwrap();

            return cached.clone();
        }

        let mut client = self.get_client(id);
        let vec = client
            .neighbors(context::current(), id)
            .await
            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));

        if !cache.read().contains(&id) {
            let mut cache = cache.write();
            cache.put(id, vec.clone());
        }

        #[cfg(feature = "pre_fetch")]
        self.pre_fetch(&vec[..]);

        vec
    }

    #[inline]
    pub async fn query_degree_async(&self, id: DefaultId) -> usize {
        let cache = self.get_cache(id);

        {
            let cache = cache.read();
            if let Some(cached) = cache.peek(&id) {
                return cached.len();
            }
        }

        let mut client = self.get_client(id);
        let vec = client
            .neighbors(context::current(), id)
            .await
            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));
        let degree = vec.len();

//        #[cfg(feature = "pre_fetch")]
//        self.pre_fetch(&vec[..]);

        if !cache.read().contains(&id) {
            let mut cache = cache.write();
            cache.put(id, vec);
        }

        degree
    }

    #[inline]
    pub async fn has_edge_async(&self, start: DefaultId, target: DefaultId) -> bool {
        let cache = self.get_cache(start);

        {
            let cache = cache.read();
            if let Some(cached) = cache.peek(&start) {
                return cached.contains(&target);
            }
        }

        let mut client = self.get_client(start);
        let vec = client
            .neighbors(context::current(), start)
            .await
            .unwrap_or_else(|e| panic!("RPC error:{:?}", e));
        let has_edge = vec.contains(&target);

//        #[cfg(feature = "pre_fetch")]
//        self.pre_fetch(&vec[..]);

        if !cache.read().contains(&start) {
            let mut cache = cache.write();
            cache.put(start, vec);
        }

        has_edge
    }

    #[cfg(feature = "pre_fetch")]
    #[inline]
    pub fn pre_fetch(&self, nodes: &[DefaultId]) {
        let pool = self.get_pool();

        if pool.queued_count() >= PRE_FETCH_QUEUE_LENGTH {
            return;
        }

        let workers = self.workers;

        for n in nodes
            .iter()
            .cloned()
            .filter(|x| !self.is_local(*x))
            .skip(PRE_FETCH_SKIP_LENGTH)
            .take(PRE_FETCH_QUEUE_LENGTH / workers)
        {
            let cache = self.get_cache(n);
            let mut client = self.get_client(n);

            let pre_fetch = async move {
                let cached = {
                    let cache = cache.read();
                    cache.contains(&n)
                };

                if !cached {
                    let vec = client
                        .neighbors(context::current(), n)
                        .await
                        .unwrap_or_else(|e| panic!("RPC error:{:?}", e));

                    if !cache.read().contains(&n) {
                        let mut cache = cache.write();
                        cache.put(n, vec);
                    }
                }
            };

            pool.execute(move || {
                let mut runtime = tokio::runtime::current_thread::Runtime::new().unwrap();

                runtime.block_on(pre_fetch);
            });
        }
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
