#![feature(async_await)]
extern crate rust_graph;
extern crate tempfile;

use rust_graph::graph_impl::DiGraphMap;
use rust_graph::io::{write_to_csv, GraphLoader};
use rust_graph::prelude::*;

use rust_graph::io::csv::CSVReader;
use rust_graph::io::tikv::tikv_loader::TikvLoader;
use serde_cbor::{to_vec, ObjectKey, Value};
use std::collections::BTreeMap;
use tempfile::TempDir;
use tikv_client::raw::Client;
use tikv_client::Config;

const NODE_PD_SERVER_ADDR: &str = "192.168.2.2:2379";
const EDGE_PD_SERVER_ADDR: &str = "192.168.2.7:2379";

fn main() {
    //Construct test csv files
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path();

    let mut g = DiGraphMap::<&str>::new();

    g.add_node(0, Some("n0"));
    g.add_node(1, Some("n1"));
    g.add_node(2, Some("n2"));

    g.add_edge(0, 1, Some("e0"));
    g.add_edge(0, 2, Some("e1"));
    g.add_edge(1, 0, Some("e2"));
    let path_to_nodes = tmp_dir_path.join("nodes_1.csv");
    let path_to_edges = tmp_dir_path.join("edges_1.csv");
    assert!(write_to_csv(&g, &path_to_nodes, &path_to_edges).is_ok());

    //Construct csvReader
    let reader = CSVReader::<u32, String, String>::new(vec![path_to_nodes], vec![path_to_edges])
        .headers(true)
        .flexible(true);

    TikvLoader::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .load(&reader, 1, 10);

    //Verifying nodes and edges storing in tikv
    futures::executor::block_on(async {
        let client = Client::new(Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()])).unwrap();
        let _node = client
            .get(bincode::serialize(&0).unwrap())
            .await
            .expect("Get node info failed!");
        println!("Node0 from tikv: {:?}", _node);
        match _node {
            Some(value_bytes) => {
                let bytes: Vec<u8> = value_bytes.into();
                let empty_map: BTreeMap<ObjectKey, Value> = BTreeMap::new();
                assert_eq!(bytes, to_vec(&(Some("n0"), empty_map)).unwrap());
            }
            None => assert!(false),
        }
        let client = Client::new(Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()])).unwrap();
        let _edge = client
            .get(bincode::serialize(&(0, 1)).unwrap())
            .await
            .expect("Get node info failed!");
        println!("Edge(0,1) from tikv: {:?}", _edge);
        match _edge {
            Some(value_bytes) => {
                let bytes: Vec<u8> = value_bytes.into();
                let empty_map: BTreeMap<ObjectKey, Value> = BTreeMap::new();
                assert_eq!(bytes, to_vec(&(Some("e0"), empty_map)).unwrap());
            }
            None => assert!(false),
        }
    });
}