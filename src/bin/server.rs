#![feature(async_await)]

use rust_graph::graph_gen::random_gnp_graph_unlabeled;
use rust_graph::graph_impl::rpc_graph::server::*;
use std::io;
use std::sync::Arc;
use tarpc::server::{BaseChannel, Channel};

#[runtime::main(runtime_tokio::Tokio)]
async fn main() -> io::Result<()> {
    let _graph = random_gnp_graph_unlabeled(100, 0.5);
    let graph = Arc::new(_graph.into_static());

    let server = GraphRPCServer::new(graph);
    server.run(18888, 10).await;

    Ok(())
}
