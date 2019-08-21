use futures::{
    channel::oneshot,
    future::{self, Ready},
    prelude::*,
};
use std::io;
use std::sync::Arc;
use std::thread;
use tarpc::{
    context,
    server::{self, Channel, Handler},
};
use tarpc_bincode_transport as bincode_transport;
use tokio::runtime::current_thread;

use crate::generic::GraphTrait;
use crate::generic::{DefaultId, Void};
use crate::graph_impl::UnStaticGraph;

type DefaultGraph = UnStaticGraph<Void>;

#[tarpc::service]
pub trait GraphRPC {
    async fn neighbors(id: DefaultId) -> Vec<DefaultId>;
    //    async fn neighbors_batch(ids: Vec<DefaultId>) -> Vec<Vec<DefaultId>>;
    //    async fn degree(id: DefaultId) -> usize;
}

#[derive(Clone)]
pub struct GraphServer {
    graph: Arc<DefaultGraph>,
}

impl GraphServer {
    pub fn new(graph: Arc<DefaultGraph>) -> Self {
        GraphServer { graph }
    }

    pub async fn run(self, port: u16, machines: usize, workers: usize, runtime:&tokio::runtime::Runtime) -> io::Result<()> {
        let server_addr = ([0, 0, 0, 0], port).into();

        let transport = bincode_transport::listen(&server_addr)?;
        //            .unwrap_or_else(|e| panic!("RPC server cannot be started: {:?}", e));

        info!("Running RPC server on {:?}", transport.local_addr());

        transport
            // Ignore accept errors.
            .filter_map(|r| future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            // Limit channels to 1 per IP.
            .max_channels_per_key(workers as u32, |t| t.as_ref().peer_addr().unwrap().ip())
            .max_concurrent_requests_per_channel(workers)
            // serve is generated by the service attribute. It takes as input any type implementing
            // the generated RPC trait.
            .map(|channel| {
                let server = self.clone();
                let (tx, rx) = oneshot::channel();

                runtime.spawn(async move {
                    channel.respond_with(server.serve()).execute().await;
                    tx.send(()).unwrap();
                });

                rx
            })
            .buffer_unordered(workers * machines)
            .for_each(|_| async {})
            .await;

        Ok(())
    }

    pub fn run_thread(self, port: u16, machines: usize, workers: usize) {
        let _ = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new()
                .unwrap_or_else(|e| panic!("Unable to start the runtime: {:?}", e));
            let run = self.run(port, machines, workers, &runtime);
            runtime.block_on(async move {
                if let Err(e) = run.await {
                    panic!("Error while running server: {}", e);
                }
            });
        });
    }
}

impl GraphRPC for GraphServer {
    type NeighborsFut = Ready<Vec<DefaultId>>;

    fn neighbors(self, _: context::Context, id: DefaultId) -> Self::NeighborsFut {
        let neighbors = self.graph.neighbors(id).into();

        future::ready(neighbors)
    }

    //    type NeighborsBatchFut = Ready<Vec<Vec<DefaultId>>>;

    //    fn neighbors_batch(self, _: context::Context, ids: Vec<DefaultId>) -> Self::NeighborsBatchFut {
    //        let mut batch = Vec::with_capacity(ids.len());
    //
    //        for id in ids {
    //            batch.push(self.graph.neighbors(id).into());
    //        }
    //
    //        future::ready(batch)
    //    }

    //    type DegreeFut = Ready<usize>;
    //
    //    fn degree(self, _: context::Context, id: DefaultId) -> Self::DegreeFut {
    //        let degree = self.graph.degree(id);
    //
    //        future::ready(degree)
    //    }
}
