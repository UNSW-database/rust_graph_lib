use generic::{GraphTrait, GraphType, Void};
use graph_impl::{EdgeVec, TypedStaticGraph};
use itertools::fold;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::prelude::ParallelBridge;
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
    let graph_file = File::open(graph_path).expect("graph file not found.");
    let table_file = File::open(partition_table_path).expect("partition table file not found.");
    let graph: BufReader<File> = BufReader::new(graph_file);
    let table: BufReader<File> = BufReader::new(table_file);

    // cache origin graph neighbors with map
    let neighbors_tmp: Vec<HashMap<u32, HashSet<u32>>> = graph
        .lines()
        .par_bridge()
        .map(|x| x.unwrap())
        .map(|x| {
            let nodes: Vec<&str> = x.split(separator).collect();
            let from: u32 = nodes[0].parse().expect("Error node id encoding.");
            let to: u32 = nodes[1].parse().expect("Error node id encoding.");
            (from, to)
        })
        .fold(
            || HashMap::<u32, HashSet<u32>>::new(),
            |mut map: HashMap<u32, HashSet<u32>>, (k, v): (u32, u32)| {
                map.entry(k).or_insert(HashSet::<u32>::new()).insert(v);
                map
            },
        )
        .collect();
    let neighbors = neighbors_tmp[0].clone();

    // read partition table
    let partition_table_tmp: Vec<Vec<HashSet<u32>>> = table
        .lines()
        .enumerate()
        .par_bridge()
        .map(|(idx, x)| (idx, x.unwrap()))
        .map(|(idx, x)| (idx, x.parse::<usize>().unwrap()))
        .fold(
            || vec![HashSet::<u32>::new(); num_partitions],
            |mut vec: Vec<HashSet<u32>>, (cur_id, partition_id)| {
                vec[partition_id].insert(cur_id as u32);
                vec
            },
        )
        .collect();
    let partition_table = partition_table_tmp[0].clone();

    // Generating partitions
    let mut partitions = vec![];
    for partition in partition_table {
        let mut nodes: Vec<u32> = partition.into_iter().collect();
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
            let mut neig_vec: Vec<u32> = neig.iter().map(|x| x.clone()).collect();
            neig_vec.sort();
            neig_vec.into_iter().for_each(|item| edges.push(item));
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
