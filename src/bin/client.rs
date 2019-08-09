use rust_graph::graph_impl::rpc_graph::server::*;
use std::io;
use tarpc::client::NewClient;
use tarpc::{client, context};
use tarpc_bincode_transport as bincode_transport;
use tokio;

fn main() -> io::Result<()> {
    let client = rust_graph::graph_impl::rpc_graph::client::GraphClient::new();

    for i in 0..10{
        let hello = client.query_neighbors(i);
        println!("{}-{:?}", i, hello);
    }

    Ok(())
}