use futures::{
    channel::oneshot,
    future::{self, Ready},
    prelude::*,
};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tarpc::{
    context,
    server::{self, Channel, Handler},
};
use tokio_serde::formats::Bincode;

use crate::generic::GraphTrait;
use crate::generic::{DefaultId, Void};
use crate::graph_impl::UnStaticGraph;

type DefaultGraph = UnStaticGraph<Void>;

#[tarpc::service]
pub trait GraphRPC {
    async fn neighbors(id: DefaultId) -> Vec<DefaultId>;
    async fn neighbors_batch(ids: Vec<DefaultId>) -> Vec<Vec<DefaultId>>;
    async fn add_stop() -> ();
    async fn send_count(count: usize) -> ();
}

#[derive(Clone)]
pub struct GraphServer {
    graph: Arc<DefaultGraph>,
    stopped_count: Arc<AtomicUsize>,
    count: Arc<AtomicUsize>,
}

impl GraphRPC for GraphServer {
    type NeighborsFut = Ready<Vec<DefaultId>>;
    fn neighbors(self, _: context::Context, id: DefaultId) -> Self::NeighborsFut {
        future::ready(self.graph.neighbors(id).into())
    }

    type NeighborsBatchFut = Ready<Vec<Vec<DefaultId>>>;
    fn neighbors_batch(self, _: context::Context, ids: Vec<DefaultId>) -> Self::NeighborsBatchFut {
        let mut batch = Vec::with_capacity(ids.len());

        for id in ids {
            batch.push(self.graph.neighbors(id).into());
        }

        future::ready(batch)
    }

    type AddStopFut = Ready<()>;
    fn add_stop(self, _: context::Context) -> Self::AddStopFut {
        self.stopped_count.fetch_add(1, Ordering::SeqCst);

        future::ready(())
    }

    type SendCountFut = Ready<()>;
    fn send_count(self, _: context::Context, count: usize) -> Self::AddStopFut {
        self.count.fetch_add(count, Ordering::SeqCst);

        future::ready(())
    }
}

impl GraphServer {
    pub fn new(graph: Arc<DefaultGraph>) -> Self {
        GraphServer {
            graph,
            stopped_count: Arc::new(AtomicUsize::new(0)),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn run(
        &self,
        port: u16,
        max_channels_per_key: u32,
        max_concurrent_requests_per_channel: usize,
        buffer_unordered: usize,
    ) -> io::Result<()> {
        let server_addr = ("0.0.0.0", port);

        let transport = tarpc::serde_transport::tcp::listen(&server_addr, Bincode::default).await?;
        info!("Listening RPC requests on {:?}", transport.local_addr());

        let incoming = transport
            .filter_map(|r| future::ready(r.ok()))
            .map(|t| {
                let mut config = server::Config::default();
                config.pending_response_buffer = 256;
                server::BaseChannel::new(config, t)
            })
            .max_channels_per_key(max_channels_per_key, |t| {
                t.as_ref().peer_addr().unwrap().ip()
            })
            .max_concurrent_requests_per_channel(max_concurrent_requests_per_channel);

        incoming
            .map(|channel| {
                let server = self.clone();
                let (tx, rx) = oneshot::channel();

                tokio::spawn(async move {
                    channel.respond_with(server.serve()).execute().await;
                    tx.send(()).unwrap();
                });

                rx
            })
            .buffer_unordered(buffer_unordered) //(num_of_channels * (machines - 1))
            .for_each(|_| async {})
            .await;

        Ok(())
    }

    pub fn get_stopped_count(&self) -> usize {
        self.stopped_count.load(Ordering::SeqCst)
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}
