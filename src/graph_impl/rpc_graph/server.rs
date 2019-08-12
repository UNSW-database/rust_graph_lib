use std::io;
use std::sync::Arc;
use std::thread;

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
