use rust_graph::graph_impl::UnStaticGraph;
use std::io;
use std::sync::Arc;

fn main() -> io::Result<()> {
    let graph = Arc::new(UnStaticGraph::empty());

    let client = rust_graph::graph_impl::rpc_graph::client::GraphClient::new_from_addrs(
        graph,
        10,
        1,
        1,
        100,
        vec![([127, 0, 0, 1], 18888).into()],
    );

    for i in 0..10 {
        let hello = client.query_neighbors(i);
        println!("{}-{:?}", i, hello);
    }

    Ok(())
}
