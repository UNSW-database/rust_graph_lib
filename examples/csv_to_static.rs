extern crate clap;
extern crate rust_graph;
extern crate time;

use std::path::Path;

use clap::{App, Arg};
use time::PreciseTime;

use rust_graph::converter::{DiStaticGraphConverter, UnStaticGraphConverter};
use rust_graph::io::read_from_csv;
use rust_graph::io::serde::{Serialize, Serializer};
use rust_graph::prelude::*;
use rust_graph::{DiGraphMap, UnGraphMap};

fn main() {
    let matches = App::new("CSV to StaticGraph Converter")
        .arg(
            Arg::with_name("node_file")
                .short("n")
                .long("node")
                .takes_value(true),
        ).arg(
            Arg::with_name("edge_file")
                .short("e")
                .long("edge")
                .required(true)
                .takes_value(true),
        ).arg(
            Arg::with_name("out_file")
                .short("o")
                .long("out")
                .takes_value(true),
        ).arg(
            Arg::with_name("separator")
                .short("s")
                .long("separator")
                .long_help("allowed separator: [comma|space|tab]")
                .takes_value(true),
        ).arg(
            Arg::with_name("has_headers")
                .short("h")
                .long("headers")
                .multiple(true),
        ).arg(
            Arg::with_name("is_flexible")
                .short("f")
                .long("flexible")
                .multiple(true),
        ).arg(
            Arg::with_name("is_directed")
                .short("d")
                .long("directed")
                .multiple(true),
        ).arg(
            Arg::with_name("reorder_node_id")
                .short("i")
                .long("reorder_nodes")
                .multiple(true),
        ).arg(
            Arg::with_name("reorded_label_id")
                .short("l")
                .long("reorder_labels")
                .multiple(true),
        ).get_matches();

    let node_file = matches.value_of("node_file").map(Path::new);
    let edge_file = Path::new(matches.value_of("edge_file").unwrap());
    let out_file = Path::new(matches.value_of("out_file").unwrap_or("graph.static"));
    let separator = matches.value_of("separator");
    let is_directed = matches.is_present("is_directed");
    let has_headers = matches.is_present("has_headers");
    let is_flexible = matches.is_present("is_flexible");
    let reorder_node_id = matches.is_present("reorder_node_id");
    let reorder_label_id = matches.is_present("reorder_label_id");

    let start = PreciseTime::now();

    if is_directed {
        let mut g = DiGraphMap::<DefaultId>::new();
        read_from_csv(
            &mut g,
            node_file,
            edge_file,
            separator,
            has_headers,
            is_flexible,
        ).expect("Error when loading csv");

        //        println!("{:?}", g);

        let static_graph =
            DiStaticGraphConverter::new(g, reorder_node_id, reorder_label_id).convert();

        println!("{:?}", &static_graph);

        Serializer::export(&static_graph, out_file).unwrap();
    } else {
        let mut g = UnGraphMap::<DefaultId>::new();
        read_from_csv(
            &mut g,
            node_file,
            edge_file,
            separator,
            has_headers,
            is_flexible,
        ).expect("Error when exporting");

        //        println!("{:?}", g);

        let static_graph =
            UnStaticGraphConverter::new(g, reorder_node_id, reorder_label_id).convert();

        Serializer::export(&static_graph, out_file).expect("Error when exporting");
    }

    let end = PreciseTime::now();
    println!("Finished in {} seconds.", start.to(end));
}
