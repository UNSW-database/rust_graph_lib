use std::fs;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Sender};
use lru::LruCache;
use parking_lot::RwLock;
use rand::{thread_rng, Rng};
use tarpc::{
    client::{self, NewClient},
    context,
};
use tarpc_bincode_transport as bincode_transport;

use crate::generic::{DefaultId, IdType};
use crate::graph_impl::rpc_graph::server::GraphRPCClient;

const MAX_RETRY: usize = 5;
const MIN_RETRY_SLEEP_MILLIS: u64 = 500;
const MAX_RETRY_SLEEP_MILLIS: u64 = 2500;

const PRE_FETCH_SKIP_LENGTH: usize = 0;

pub struct Messenger {
    server_addrs: Vec<SocketAddr>,
    clients: Vec<Option<GraphRPCClient>>,
    caches: Vec<Option<Arc<RwLock<LruCache<DefaultId, Vec<DefaultId>>>>>>,
    workers: usize,
    peers: usize,
    processor: usize,
    cache_size: usize,
    runtime: tokio::runtime::Runtime,
    sender: Sender<DefaultId>,
    pre_fetch_queue_len: usize,
}

impl Messenger {
    pub fn new<P: AsRef<Path>>(
        cache_size: usize,
        port: u16,
        workers: usize,
        machines: usize,
        processor: usize,
        pre_fetch_wokers: usize,
        pre_fetch_length: usize,
        path_to_hosts: P,
    ) -> Self {
        let hosts_str = fs::read_to_string(path_to_hosts).unwrap();
        let hosts = parse_hosts(hosts_str, machines);
        let server_addrs = init_address(hosts, port);

        let (sender, receiver) = bounded(pre_fetch_length);
        let runtime = tokio::runtime::Builder::new()
            .name_prefix("graph-clients-")
            .core_threads(workers)
            .build()
            .unwrap_or_else(|e| panic!("Fail to initialize the runtime: {:?}", e));
        let peers = workers * machines;

        let mut messenger = Self {
            server_addrs,
            clients: vec![],
            caches: vec![],
            workers,
            processor,
            peers,
            cache_size,
            runtime,
            sender,
            pre_fetch_queue_len: pre_fetch_length,
        };

        messenger.create_clients();

        let pool = tokio::runtime::Builder::new()
            .name_prefix("graph-pre-fetch-")
            .core_threads(pre_fetch_wokers)
            .build()
            .unwrap_or_else(|e| panic!("Fail to initialize the runtime: {:?}", e));
        let clients = messenger.clients.clone();
        let caches = messenger.caches.clone();

        thread::spawn(move || {
            while let Ok(n) = receiver.recv() {
                debug!("Pre-fetching: recv {}", n);

                let client_id = n.id() % peers;
                let cache = caches[client_id].clone().unwrap();

                if cache.read().contains(&n) {
                    continue;
                }

                let mut client = clients[client_id].clone().unwrap();

                let pre_fetch = async move {
                    let vec = client
                        .neighbors(context::current(), n)
                        .await
                        .unwrap_or_else(|e| panic!("RPC error:{:?}", e));

                    if !cache.read().contains(&n) {
                        let mut cache = cache.write();
                        cache.put(n, vec);
                    }
                };

                pool.spawn(pre_fetch);
            }
        });

        messenger
    }

    fn create_clients(&mut self) {
        let runtime = &self.runtime;
        let cache_size = self.cache_size / (self.peers - self.workers);
        let mut rng = thread_rng();

        info!("The size of each cache is {}", cache_size);

        for (i, addr) in self.server_addrs.iter().enumerate() {
            for _ in 0..self.workers {
                let (client, cache) = if i == self.processor {
                    (None, None)
                } else {
                    let mut retry = 0;

                    let transport = loop {
                        let transport = runtime.block_on(async move {
                            if retry == 0 {
                                info!("Connecting to {:?}", addr);
                            } else {
                                info!("Retry {}: connecting to {:?}", retry, addr);
                            }

                            bincode_transport::connect(&addr).await
                        });

                        match transport {
                            Ok(channel) => break channel,
                            Err(e) => {
                                warn!("Fail to connect to {:}: {:}", addr, e);
                                retry += 1;

                                if retry > MAX_RETRY {
                                    panic!("Connection failed: exceeded maximum number of retries");
                                }

                                let sleep_time = Duration::from_millis(
                                    rng.gen_range(MIN_RETRY_SLEEP_MILLIS, MAX_RETRY_SLEEP_MILLIS),
                                );
                                thread::sleep(sleep_time);
                            }
                        }
                    };

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
            .enumerate()
            .map(|(i, x)| {
                x.map(move |cache| {
                    let len = cache.read().len();
                    info!("Worker {} cache length: {}", i, len);

                    len
                })
            })
            .filter_map(|x| x)
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

    #[inline]
    pub async fn query_neighbors_async(&self, id: DefaultId, pre_fetch: bool) -> Vec<DefaultId> {
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

        if pre_fetch {
            self.pre_fetch(&vec[..]);
        }

        vec
    }

    #[inline]
    pub fn pre_fetch(&self, nodes: &[DefaultId]) {
        for n in nodes
            .iter()
            .copied()
            .filter(|x| !self.is_local(*x))
            .skip(PRE_FETCH_SKIP_LENGTH)
            .take(self.pre_fetch_queue_len / self.workers)
        {
            if !self.sender.is_full() {
                self.sender.send(n).unwrap();
            } else {
                break;
            }
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
