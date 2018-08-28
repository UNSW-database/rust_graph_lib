#[macro_use]
extern crate rust_graph;
extern crate time;

use std::path::Path;

use time::PreciseTime;

use rust_graph::UnGraphMap;
use rust_graph::converter::UnStaticGraphConverter;
use rust_graph::io::serde::{Deserialize, Deserializer, Serialize, Serializer};
use rust_graph::map::SetMap;
use rust_graph::prelude::*;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let in_file = Path::new(&args[1]);
    let out_file = Path::new(&args[2]);

    let start = PreciseTime::now();

    println!("Loading {:?}", &in_file);
    let g: UnGraphMap<String> = Deserializer::import(in_file).unwrap();

    println!("{:?}", g.get_node_label_map());
    println!("{:?}", g.get_edge_label_map());

    let static_graph = UnStaticGraphConverter::with_label_map(
        &g,
        setmap!(
            "continent".to_owned(),
            "tagclass".to_owned(),
            "country".to_owned(),
            "person".to_owned(),
            "city".to_owned(),
            "company".to_owned(),
            "forum".to_owned(),
            "university".to_owned(),
            "tag".to_owned(),
            "comment".to_owned(),
            "post".to_owned()
        ),
        g.get_edge_label_map().clone(),
    ).to_graph()
        .to_int_label();

    println!("Exporting to {:?}...", &out_file);

    println!("{:?}", static_graph.get_node_label_map());
    println!("{:?}", static_graph.get_edge_label_map());

    Serializer::export(&static_graph, out_file).unwrap();

    let end = PreciseTime::now();

    println!("Finished in {} seconds.", start.to(end));
}
