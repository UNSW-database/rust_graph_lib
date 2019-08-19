use rust_graph::graph_gen::random_gnp_graph_unlabeled;
use rust_graph::graph_impl::rpc_graph::server::*;
use std::io;
use std::io::prelude::*;
use std::sync::Arc;

fn main() -> io::Result<()> {
    let _graph = random_gnp_graph_unlabeled(100, 0.5);
    let graph = Arc::new(_graph.into_static());

    let server = GraphServer::new(graph);
    server.run_thread(18888, 1, 1);

    pause();

    Ok(())
}

fn pause() {
    let mut stdin = io::stdin();
    let mut stdout = io::stdout();

    // We want the cursor to stay at the end of the line, so we print without a newline and flush manually.
    write!(stdout, "Press any key to continue...").unwrap();
    stdout.flush().unwrap();

    // Read a single byte and discard
    let _ = stdin.read(&mut [0u8]).unwrap();
}
