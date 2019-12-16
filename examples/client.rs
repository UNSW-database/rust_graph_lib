use std::io;
use std::time::Instant;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> io::Result<()> {
    {
        let client =
            rust_graph::graph_impl::rpc_graph::ClientCore::new(18888, 1, 1, 1, "hosts.txt").await;

        let mut len = 0;

        let start = Instant::now();

        for i in 0u32..5 {
            let neighbors = client.query_neighbors_async(i).await.len();
            len += neighbors;
            println!("{} - Len: {}", i, neighbors);
        }

        let duration = start.elapsed();
        println!("Sync - Time: {:?}, length: {}", duration, len);
    }

    {
        let client =
            rust_graph::graph_impl::rpc_graph::ClientCore::new(18888, 1, 1, 1, "hosts.txt").await;

        let start = Instant::now();

        let mut rx = {
            let (tx, rx) = mpsc::unbounded_channel();

            for i in 0u32..5 {
                let tx_clone = tx.clone();
                let client_clone = client.clone();

                tokio::spawn(async move {
                    let neighbors = client_clone.query_neighbors_async(i).await;
                    tx_clone.send((i, neighbors.len())).unwrap();
                });
            }

            rx
        };

        let mut len: usize = 0;
        while let Some((i, l)) = rx.recv().await {
            len += l;
            println!("{} - Len: {}", i, l);
        }

        let duration = start.elapsed();
        println!("Async - Time: {:?}, length: {}", duration, len);
    }

    Ok(())
}
