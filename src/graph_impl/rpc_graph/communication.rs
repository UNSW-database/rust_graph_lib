use std::fs;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use rand::{thread_rng, Rng};
use tarpc::{client, context};
use tokio::time;
use tokio_serde::formats::Bincode;
use vec_map::VecMap;

use crate::generic::{DefaultId, IdType};
use crate::graph_impl::rpc_graph::server::GraphRPCClient;

const MAX_RETRY: usize = 5;
const MIN_RETRY_SLEEP_MILLIS: u64 = 500;
const MAX_RETRY_SLEEP_MILLIS: u64 = 2500;

#[derive(Debug, Clone)]
pub struct ClientCore {
    server_addrs: Vec<SocketAddr>,
    clients: Arc<RwLock<VecMap<GraphRPCClient>>>,
    pub workers: usize,
    pub peers: usize,
    pub machines: usize,
    pub processor: usize,
}

impl ClientCore {
    pub fn new<P: AsRef<Path>>(
        port: u16,
        workers: usize,
        machines: usize,
        processor: usize,
        path_to_hosts: P,
    ) -> Self {
        let hosts_str = fs::read_to_string(path_to_hosts).unwrap();
        let hosts = parse_hosts(hosts_str, machines);
        let server_addrs = init_address(hosts, port);
        let peers = workers * machines;

        let client = Self {
            server_addrs,
            clients: Arc::new(RwLock::new(VecMap::new())),
            workers,
            processor,
            machines,
            peers,
        };

        client
    }

    // pub async fn start(&self){
    //     self.create_clients().await
    // }

    pub async fn start(&self) {
        let mut rng = thread_rng();

        for (i, addr) in self.server_addrs.iter().enumerate() {
            if i == self.processor {
                continue;
            }

            for w in 0..self.workers {
                let mut retry = 0;

                let transport = loop {
                    if retry == 0 {
                        info!("Connecting to {:?}", addr);
                    } else {
                        info!("Retry {}: connecting to {:?}", retry, addr);
                    }

                    let transport =
                        tarpc::serde_transport::tcp::connect(&addr, Bincode::default()).await;

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
                            time::delay_for(sleep_time).await;
                        }
                    }
                };

                let client = GraphRPCClient::new(client::Config::default(), transport)
                    .spawn()
                    .unwrap_or_else(|e| panic!("Unable to start the RPC client: {:?}", e));

                self.clients.write().insert(i * self.workers + w, client);
            }
        }
    }

    #[inline(always)]
    pub fn is_local(&self, id: DefaultId) -> bool {
        id.id() % self.peers / self.workers == self.processor
    }

    #[inline(always)]
    pub fn get_client_id(&self, id: DefaultId) -> usize {
        id.id() % self.peers
    }

    #[inline(always)]
    pub fn get_client(&self, idx: usize) -> GraphRPCClient {
        self.clients
            .read()
            .get(idx)
            .cloned()
            .unwrap_or_else(|| panic!("Error on getting client {}", idx))
    }

    #[inline(always)]
    pub fn get_client_for_node(&self, id: DefaultId) -> GraphRPCClient {
        let idx = self.get_client_id(id);

        self.get_client(idx)
    }

    #[inline]
    pub async fn query_neighbors_async(&self, id: DefaultId) -> Vec<DefaultId> {
        let mut client = self.get_client_for_node(id);
        let vec = client
            .neighbors(context::current(), id)
            .await
            .unwrap_or_else(|e| panic!("RPC error on getting node {}: {:?}", id, e));

        vec
    }

    #[inline]
    pub async fn query_neighbors_async_batch(
        &self,
        client_id: usize,
        ids: Vec<DefaultId>,
    ) -> Vec<Vec<DefaultId>> {
        let mut client = self.get_client(client_id);

        let vec = client
            .neighbors_batch(context::current(), ids)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "RPC error on getting node batch from machine {}: {:?}",
                    client_id, e
                )
            });

        vec
    }

    pub async fn stop_connections(&self) {
        for (_, mut client) in self.clients.read().clone() {
            client
                .add_stop(context::current())
                .await
                .unwrap_or_else(|e| panic!("RPC error on stopping: {:?}", e));
        }
    }

    #[inline]
    pub async fn send_count(&self, count: usize) {
        let mut client = self.get_client(0);
        client
            .send_count(context::current(), count)
            .await
            .unwrap_or_else(|e| panic!("RPC error on sending count: {:?}", e));
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
