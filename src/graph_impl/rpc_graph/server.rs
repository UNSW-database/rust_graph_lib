use std::io;
use std::sync::Arc;
use std::thread;

use capnp::capability::Promise;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::future::Future;
use futures::stream::Stream;
use tokio::io::AsyncRead;
use tokio::runtime::current_thread;

use crate::generic::GraphTrait;
use crate::generic::Void;
use crate::graph_capnp;
use crate::graph_impl::UnStaticGraph;

type DefaultGraph = UnStaticGraph<Void>;

#[derive(Clone)]
pub struct GraphServer {
    graph: Arc<DefaultGraph>,
}

impl GraphServer {
    pub fn new(graph: Arc<DefaultGraph>) -> Self {
        GraphServer { graph }
    }

    pub fn run(self, port: u16) {
        let addr = ([0, 0, 0, 0], port).into();
        let socket = tokio::net::TcpListener::bind(&addr).unwrap();

        let graph = graph_capnp::graph::ToClient::new(self).into_client::<capnp_rpc::Server>();

        let done = socket.incoming().for_each(move |socket| {
            socket.set_nodelay(true)?;
            let (reader, writer) = socket.split();

            let network = twoparty::VatNetwork::new(
                reader,
                std::io::BufWriter::new(writer),
                rpc_twoparty_capnp::Side::Server,
                Default::default(),
            );

            let rpc_system = RpcSystem::new(Box::new(network), Some(graph.clone().client));
            current_thread::spawn(rpc_system.map_err(|e| println!("error: {:?}", e)));
            Ok(())
        });

        current_thread::block_on_all(done).unwrap();
    }
}

impl graph_capnp::graph::Server for GraphServer {
    fn neighbors(
        &mut self,
        params: graph_capnp::graph::NeighborsParams,
        mut results: graph_capnp::graph::NeighborsResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let x = pry!(params.get()).get_x();
        let neighbors = self.graph.neighbors(x);
        let len = neighbors.len() as u32;

        let mut builder = results.get().init_y(len);

        for i in 0..len {
            builder.set(i, neighbors[i as usize]);
        }

        capnp::capability::Promise::ok(())
    }
}
