use generic::{GraphTrait, GraphType, IdType, Void};
use graph_impl::{EdgeVec, TypedStaticGraph};
use itertools::Itertools;
use rayon::prelude::{IndexedParallelIterator, ParallelBridge, ParallelIterator};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hash::Hash;
use std::io::{BufRead, BufReader, Read};
use UnStaticGraph;

pub fn read_partitions(
    graph_path: &str,
    partition_table_path: &str,
    separator: &str,
    num_partitions: usize,
) -> Vec<UnStaticGraph<Void>> {
    let mut graph_file = File::open(graph_path).expect("graph file not found.");
    let mut table_file = File::open(partition_table_path).expect("partition table file not found.");
    let graph: BufReader<File> = BufReader::new(graph_file);
    let table: BufReader<File> = BufReader::new(table_file);

    // cache origin graph neighbors with map
    let mut neighbors = HashMap::new();
    graph
        .lines()
        .map(|x| x.unwrap())
        .map(|x| {
            let nodes: Vec<&str> = x.split(separator).collect();
            let from: u32 = nodes[0].parse().expect("Error node id encoding.");
            let to: u32 = nodes[1].parse().expect("Error node id encoding.");
            (from, to)
        })
        .for_each(|(from, to)| {
            neighbors.entry(from).or_insert(HashSet::new()).insert(to);
        });

    // read partition table
    let mut partition_table = vec![HashSet::new(); num_partitions];
    table
        .lines()
        .map(|x| x.unwrap())
        .map(|x| x.parse::<usize>().unwrap())
        .enumerate()
        .for_each(|(cur_id, partition_id)| {
            (&mut partition_table[partition_id]).insert(cur_id);
        });

    // Generating partitions
    let mut partitions = vec![];
    for partition in partition_table {
        let mut nodes = partition.into_iter().collect_vec();
        nodes.sort();
        nodes.reverse();
        let mut offsets = vec![0];
        let mut edges = vec![];
        let mut cur_id = 0;
        while nodes.len() > 0 {
            let top = nodes.pop().unwrap();
            while cur_id < top {
                offsets.push(offsets.last().unwrap().clone());
                cur_id += 1;
            }
            let default = HashSet::new();
            let neig = neighbors.get(&(cur_id as u32)).unwrap_or(&default);
            offsets.push(offsets.last().unwrap() + neig.len());
            neig.iter()
                .sorted()
                .into_iter()
                .for_each(|item| edges.push(item.clone()));
            cur_id += 1;
        }
        let edge_vec = EdgeVec::new(offsets, edges);
        partitions.push(UnStaticGraph::<Void>::new(edge_vec, None, None, None));
    }

    partitions
}

#[test]
fn test_partitioner() {
    let graph_path = "C:\\Users\\cy\\Desktop\\lj.txt";
    let partition_table_path = "C:\\Users\\cy\\Desktop\\partition_table.part.10";
    println!("{}", graph_path);
    let result: Vec<UnStaticGraph<Void>> =
        read_partitions(graph_path, partition_table_path, "\t", 10);
    for graph in &result {
        println!(
            "num_node:{};num_edge:{}",
            graph.node_count(),
            graph.edge_count()
        )
    }
    assert_eq!(result.len(), 10);
    assert!(false); // for print
}
