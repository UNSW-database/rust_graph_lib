#![feature(async_await)]

use rust_graph::graph_impl::rpc_graph::server::*;
use std::io;
use tarpc::client::NewClient;
use tarpc::{client, context};
use tarpc_bincode_transport as bincode_transport;
use tokio;

fn main() -> io::Result<()> {
    let server_addr = ([127, 0, 0, 1], 18888).into();
    let mut runtime = tokio::runtime::current_thread::Runtime::new()?;

    let transport = runtime
        .block_on(async move { bincode_transport::connect(&server_addr).await })
        .unwrap();

    let NewClient {
        mut client,
        dispatch,
    } = GraphRPCClient::new(client::Config::default(), transport);
    runtime.spawn(async move {
        if let Err(e) = dispatch.await {
            println!("Error while running client dispatch: {}", e)
        }
    });

    let hello = runtime.block_on(async move { client.neighbors(context::current(), 0).await });

    println!("{:?}", hello);

    Ok(())
}
