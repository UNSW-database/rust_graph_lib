use rust_graph::graph_impl::rpc_graph::server::GraphRPCClient;
use std::io;
use std::time::Instant;
use tarpc::{client, context};
use tokio::sync::mpsc;
use tokio_serde::formats::Bincode;

#[tokio::main]
async fn main() -> io::Result<()> {
    let client =
        rust_graph::graph_impl::rpc_graph::ClientCore::new(18888, 1, 1, 1, "hosts.txt").await;

    let mut len = 0;
    for i in 0u32..5 {
        let neighbors = client.query_neighbors_async(i).await;
        len += neighbors.len();
        println!("Len: {}", len);
    }

    //    {
    //        let transport =
    //            tarpc::serde_transport::tcp::connect(("localhost", 18888), Bincode::default()).await?;
    //        let mut client = GraphRPCClient::new(client::Config::default(), transport).spawn()?;
    //
    //        let mut len = 0;
    //
    //        let start = Instant::now();
    //
    //        for i in 0u32..5 {
    //            let neighbors = client.neighbors(context::current(), i).await?;
    //            len += neighbors.len();
    //            println!("Len: {}", len);
    //        }
    //
    //        let duration = start.elapsed();
    //        println!("Sync - Time: {:?}, length: {}", duration, len);
    //    }
    //
    //    std::thread::sleep(std::time::Duration::new(5, 0));
    //
    //    {
    //        let start = Instant::now();
    //
    //        let mut rx = {
    //            let (tx, rx) = mpsc::unbounded_channel();
    //
    //            for i in 0u32..5 {
    //                let tx_clone = tx.clone();
    //                let transport =
    //                    tarpc::serde_transport::tcp::connect(("localhost", 18888), Bincode::default())
    //                        .await?;
    //                let mut client =
    //                    GraphRPCClient::new(client::Config::default(), transport).spawn()?;
    //                tokio::spawn(async move {
    //                    let neighbors = client.neighbors(context::current(), i).await.unwrap();
    //                    tx_clone.send(neighbors.len()).unwrap();
    //                });
    //            }
    //
    //            rx
    //        };
    //
    //        let mut len = 0;
    //        while let Some(i) = rx.recv().await {
    //            len += i;
    //            println!("Len: {}", len);
    //        }
    //
    //        let duration = start.elapsed();
    //        println!("Async - Time: {:?}, length: {}", duration, len);
    //    }

    Ok(())
}
