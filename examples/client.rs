use rust_graph::UnStaticGraph;
use std::io;
use std::sync::Arc;

fn main() -> io::Result<()> {
    let graph = Arc::new(UnStaticGraph::empty());

    let client = rust_graph::graph_impl::rpc_graph::client::GraphClient::new(
        graph, 10, 18888, 1, 1, 100, "host.txt",
    );

    for i in 0..10 {
        let hello = client.query_neighbors(i);
        println!("{}-{:?}", i, hello);
    }

    Ok(())
}
