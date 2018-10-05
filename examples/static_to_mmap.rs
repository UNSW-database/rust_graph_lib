extern crate clap;
extern crate rust_graph;

use clap::{App, Arg};
use rust_graph::graph_impl::{DiStaticGraph, UnStaticGraph};
use rust_graph::io::serde::{Deserialize, Deserializer};

fn main() {
    let matches = App::new("StaticGraph to MMap")
        .arg(
            Arg::with_name("graph")
                .short("g")
                .long("graph")
                .required(true)
                .takes_value(true),
        ).arg(Arg::with_name("directed").short("d").long("directed"))
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .required(true)
                .takes_value(true),
        ).get_matches();

    let graph = matches.value_of("graph").unwrap();
    let output = matches.value_of("output").unwrap();
    let is_directed = matches.is_present("directed");

    if !is_directed {
        let graph: UnStaticGraph<u32> = Deserializer::import(graph).unwrap();
        graph.dump_mmap(output).expect("Dump graph error");
    } else {
        let graph: DiStaticGraph<u32> = Deserializer::import(graph).unwrap();
        graph.dump_mmap(output).expect("Dump graph error");
    }
}
