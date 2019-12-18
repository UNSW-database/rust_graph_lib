#![feature(async_await)]
extern crate rust_graph;
extern crate serde_json;
extern crate tikv_client;

use rust_graph::property::tikv_property::*;
use rust_graph::property::PropertyGraph;
use serde_json::{json, to_vec};
use tikv_client::{Config, Key, KvPair, RawClient as Client, Result, ToOwnedRange, Value};

/// The pd-server that is responsible to store node properties in its managed tikv-servers
const NODE_PD_SERVER_ADDR: &str = "192.168.2.2:2379";

/// The pd-server that is responsible to store edge properties in its managed tikv-servers
const EDGE_PD_SERVER_ADDR: &str = "192.168.2.7:2379";

fn main() {
    let args = parse_args("raw");
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
        .unwrap();
    // insert node property
    let new_prop = json!({"name":"jack"});
    let raw_prop = to_vec(&new_prop).unwrap();

    graph.insert_node_raw(0u32, raw_prop).unwrap();
    let node_property = graph.get_node_property_all(0u32).unwrap();

    assert_eq!(Some(json!({"name":"jack"})), node_property);

    // insert edge property
    let edge_prop = json!({"length":"15"});
    let raw_edge_prop = to_vec(&edge_prop).unwrap();

    graph.insert_edge_raw(0u32, 1u32, raw_edge_prop).unwrap();
    let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

    assert_eq!(Some(json!({"length":"15"})), edge_property);
}
