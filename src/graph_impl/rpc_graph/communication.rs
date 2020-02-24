use std::fs;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::thread;
use std::time::Duration;

use rand::{thread_rng, Rng};
use tarpc::{client, context};
use tokio_serde::formats::Bincode;

use crate::generic::{DefaultId, IdType};
use crate::graph_impl::rpc_graph::server::GraphRPCClient;

const MAX_RETRY: usize = 5;
const MIN_RETRY_SLEEP_MILLIS: u64 = 500;
const MAX_RETRY_SLEEP_MILLIS: u64 = 2500;

//const PRE_FETCH_SKIP_LENGTH: usize = 0;

#[derive(Debug, Clone)]
pub struct ClientCore {
    server_addrs: Vec<SocketAddr>,
    clients: Vec<Option<GraphRPCClient>>,
    workers: usize,
    peers: usize,
    processor: usize,
}

impl ClientCore {
    pub async fn new<P: AsRef<Path>>(
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

        let mut client = Self {
            server_addrs,
            clients: vec![],
            workers,
            processor,
            peers,
        };

        client.create_clients().await;

        client
    }

    async fn create_clients(&mut self) {
        let mut rng = thread_rng();

        for (i, addr) in self.server_addrs.iter().enumerate() {
            for _ in 0..self.workers {
                if i == self.processor {
                    self.clients.push(None);
                } else {
                    let mut retry = 0;

                    let transport = loop {
                        if retry == 0 {
                            println!("Connecting to {:?}", addr);
                        } else {
                            println!("Retry {}: connecting to {:?}", retry, addr);
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
                                thread::sleep(sleep_time);
                            }
                        }
                    };

                    let client = GraphRPCClient::new(client::Config::default(), transport)
                        .spawn()
                        .unwrap_or_else(|e| panic!("Unable to start the RPC client: {:?}", e));

                    self.clients.push(Some(client));
                };
            }
        }
    }

    #[inline(always)]
    pub fn is_local(&self, id: DefaultId) -> bool {
        id.id() % self.peers / self.workers == self.processor
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

    #[inline]
    pub async fn query_neighbors_async(&self, id: DefaultId) -> Vec<DefaultId> {
        let mut client = self.get_client(id);
        let vec = client
            .neighbors(context::current(), id)
            .await
            .unwrap_or_else(|e| panic!("RPC error on getting node {}:{:?}", id, e));

        vec
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
