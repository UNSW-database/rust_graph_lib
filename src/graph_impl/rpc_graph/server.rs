use futures::{
    channel::oneshot,
    future::{self, Ready},
    prelude::*,
};
use std::io;
use std::sync::Arc;

use tarpc::{
    context,
    server::{self, Channel, Handler},
};
use tokio::runtime::{Builder, Handle};
use tokio_serde::formats::Bincode;

use crate::generic::GraphTrait;
use crate::generic::{DefaultId, Void};
use crate::graph_impl::UnStaticGraph;

type DefaultGraph = UnStaticGraph<Void>;

#[tarpc::service]
pub trait GraphRPC {
    async fn neighbors(id: DefaultId) -> Vec<DefaultId>;
    async fn verify(src: DefaultId, dst: DefaultId) -> bool;
}

#[derive(Clone)]
pub struct GraphServer {
    graph: Arc<DefaultGraph>,
}

impl GraphRPC for GraphServer {
    type NeighborsFut = Ready<Vec<DefaultId>>;
    //    type NeighborsFut = Pin<Box<dyn Future<Output=Vec<DefaultId>>>>;

    fn neighbors(self, _: context::Context, id: DefaultId) -> Self::NeighborsFut {
        future::ready(self.graph.neighbors(id).into())
    }

    type VerifyFut = Ready<bool>;

    fn verify(self, _: context::Context, src: DefaultId, dst: DefaultId) -> Self::VerifyFut {
        future::ready(self.graph.has_edge(src, dst).into())
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

impl GraphServer {
    pub fn new(graph: Arc<DefaultGraph>) -> Self {
        GraphServer { graph }
    }

    pub async fn run(
        self,
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

    //    pub fn run_blocking(self, port: u16) -> io::Result<()> {
    //        let mut runtime = Builder::new()
    //            .thread_name("rpc-server")
    //            .threaded_scheduler()
    //            .enable_all()
    //            .on_thread_start(|| {
    //                info!("RPC server started");
    //            })
    //            .on_thread_stop(|| {
    //                info!("RPC server stopped");
    //            })
    //            .build()
    //            .unwrap_or_else(|e| panic!("Unable to start the runtime: {:?}", e));
    //
    //        let handle = runtime.handle().clone();
    //
    //        let run = self.run(port, handle);
    //
    //        runtime.block_on(run)
    //    }
}
