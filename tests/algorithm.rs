/*
 * Copyright (c) 2018 UNSW Sydney, Data and Knowledge Group.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
extern crate rust_graph;

#[cfg(test)]
mod test {
    use rust_graph::algorithm::bfs::Bfs;
    use rust_graph::algorithm::conn_comp::ConnComp;
    use rust_graph::algorithm::dfs::Dfs;
    use rust_graph::algorithm::conn_subgraphs::ConnSubgraph;
    use rust_graph::algorithm::cano_label::CanoLabel;
    use rust_graph::algorithm::graph_minus::GraphMinus;
    use rust_graph::algorithm::graph_union::GraphUnion;
    use rust_graph::graph_impl::{UnGraphMap, DiGraphMap};
    use rust_graph::prelude::*;
    use rust_graph::generic::GeneralGraph;


    #[test]
    fn test_cc_undirected_one_component() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);

        let cc = ConnComp::new(&graph);

        assert_eq!(cc.get_count(), 1);

        assert_eq!(cc.is_connected(1, 2), true);
        assert_eq!(cc.is_connected(2, 3), true);
        assert_eq!(cc.is_connected(1, 3), true);

        assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 3);
        assert_eq!(cc.get_connected_nodes(2).unwrap().len(), 3);
        assert_eq!(cc.get_connected_nodes(3).unwrap().len(), 3);
    }

    #[test]
    fn test_cc_undirected_seperate_components() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(3, 4, None);

        let cc = ConnComp::new(&graph);

        assert_eq!(cc.get_count(), 2);

        assert_eq!(cc.is_connected(1, 2), true);
        assert_eq!(cc.is_connected(2, 3), false);
        assert_eq!(cc.is_connected(1, 3), false);
        assert_eq!(cc.is_connected(1, 4), false);
        assert_eq!(cc.is_connected(2, 4), false);
        assert_eq!(cc.is_connected(3, 4), true);

        assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 2);
        assert_eq!(cc.get_connected_nodes(2).unwrap().len(), 2);
        assert_eq!(cc.get_connected_nodes(3).unwrap().len(), 2);
        assert_eq!(cc.get_connected_nodes(4).unwrap().len(), 2);
    }

    #[test]
    fn test_cc_directed_one_component() {
        let mut graph = DiGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);

        let cc = ConnComp::new(&graph);

        assert_eq!(cc.get_count(), 1);

        assert_eq!(cc.is_connected(1, 2), true);
        assert_eq!(cc.is_connected(2, 3), true);
        assert_eq!(cc.is_connected(1, 3), true);

        assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 3);
        assert_eq!(cc.get_connected_nodes(2).unwrap().len(), 3);
        assert_eq!(cc.get_connected_nodes(3).unwrap().len(), 3);
    }

    #[test]
    fn test_cc_directed_seperate_components() {
        let mut graph = DiGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(3, 4, None);

        let cc = ConnComp::new(&graph);

        assert_eq!(cc.get_count(), 2);

        assert_eq!(cc.is_connected(1, 2), true);
        assert_eq!(cc.is_connected(2, 3), false);
        assert_eq!(cc.is_connected(1, 3), false);
        assert_eq!(cc.is_connected(1, 4), false);
        assert_eq!(cc.is_connected(2, 4), false);
        assert_eq!(cc.is_connected(3, 4), true);

        assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 2);
        assert_eq!(cc.get_connected_nodes(2).unwrap().len(), 2);
        assert_eq!(cc.get_connected_nodes(3).unwrap().len(), 2);
        assert_eq!(cc.get_connected_nodes(4).unwrap().len(), 2);
    }

    #[test]
    fn test_bfs_undirected_one_component() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);

        let mut bfs = Bfs::new(&graph, Some(1));
        let x = bfs.next();
        assert_eq!(x, Some(1));
        let x = bfs.next();
        assert_eq!(x, Some(2));
        let x = bfs.next();
        assert_eq!(x, Some(3));
        let x = bfs.next();
        assert_eq!(x, None);
    }

    #[test]
    fn test_bfs_undirected_radomly_chosen_start() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);

        let mut bfs = Bfs::new(&graph, None);
        let x = bfs.next();
        let result = x == Some(1) || x == Some(2);
        assert_eq!(result, true);
    }

    #[test]
    fn test_bfs_undirected_seperate_components() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(3, 4, None);

        let mut bfs = Bfs::new(&graph, Some(1));
        let x = bfs.next();
        assert_eq!(x, Some(1));
        let x = bfs.next();
        assert_eq!(x, Some(2));
        let x = bfs.next();
        let result = x == Some(3) || x == Some(4);
        assert_eq!(result, true);
    }

    #[test]
    fn test_bfs_directed_one_component() {
        let mut graph = DiGraphMap::<u32>::new();
        graph.add_edge(2, 1, None);
        graph.add_edge(3, 1, None);

        let mut bfs = Bfs::new(&graph, Some(1));
        let x = bfs.next();
        assert_eq!(x, Some(1));
        let x = bfs.next();
        let result = x == Some(3) || x == Some(2);
        assert_eq!(result, true);
    }

    #[test]
    fn test_bfs_directed_radomly_chosen_start() {
        let mut graph = DiGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);

        let mut bfs = Bfs::new(&graph, None);
        let x = bfs.next();
        let result = x == Some(1) || x == Some(2);
        assert_eq!(result, true);
    }

    #[test]
    fn test_bfs_directed_seperate_components() {
        let mut graph = DiGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(3, 4, None);

        let mut bfs = Bfs::new(&graph, Some(1));
        let x = bfs.next();
        assert_eq!(x, Some(1));
        let x = bfs.next();
        assert_eq!(x, Some(2));
        let x = bfs.next();
        let result = x == Some(3) || x == Some(4);
        assert_eq!(result, true);
    }

    #[test]
    fn test_dfs_undirected_one_component() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);

        let mut dfs = Dfs::new(&graph, Some(1));
        let x = dfs.next();
        assert_eq!(x, Some(1));
        let x = dfs.next();
        assert_eq!(x, Some(2));
        let x = dfs.next();
        assert_eq!(x, Some(3));
        let x = dfs.next();
        assert_eq!(x, None);
    }

    #[test]
    fn test_dfs_undirected_radomly_chosen_start() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);

        let mut dfs = Dfs::new(&graph, None);
        let x = dfs.next();
        let result = x == Some(1) || x == Some(2);
        assert_eq!(result, true);
    }

    #[test]
    fn test_dfs_undirected_seperate_components() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(3, 4, None);

        let mut dfs = Dfs::new(&graph, Some(1));
        let x = dfs.next();
        assert_eq!(x, Some(1));
        let x = dfs.next();
        assert_eq!(x, Some(2));
        let x = dfs.next();
        let result = x == Some(3) || x == Some(4);
        assert_eq!(result, true);
    }

    #[test]
    fn test_dfs_directed_one_component() {
        let mut graph = DiGraphMap::<u32>::new();
        graph.add_edge(2, 1, None);
        graph.add_edge(3, 1, None);

        let mut dfs = Dfs::new(&graph, Some(1));
        let x = dfs.next();
        assert_eq!(x, Some(1));
        let x = dfs.next();
        let result = x == Some(3) || x == Some(2);
        assert_eq!(result, true);
    }

    #[test]
    fn test_dfs_directed_radomly_chosen_start() {
        let mut graph = DiGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);

        let mut dfs = Dfs::new(&graph, None);
        let x = dfs.next();
        let result = x == Some(1) || x == Some(2);
        assert_eq!(result, true);
    }

    #[test]
    fn test_dfs_directed_seperate_components() {
        let mut graph = DiGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(3, 4, None);

        let mut dfs = Dfs::new(&graph, Some(1));
        let x = dfs.next();
        assert_eq!(x, Some(1));
        let x = dfs.next();
        assert_eq!(x, Some(2));
        let x = dfs.next();
        let result = x == Some(3) || x == Some(4);
        assert_eq!(result, true);
    }

    #[test]
    fn test_cano_label_two_graphs_seperate_components() {
        let mut graph0 = DiGraphMap::<u32>::new();
        graph0.add_edge(1, 2, None);
        graph0.add_edge(3, 4, None);

        let mut graph1 = DiGraphMap::<u32>::new();
        graph1.add_edge(1, 2, None);
        graph1.add_edge(3, 4, None);

        let cl0 = CanoLabel::new(&graph0);
        let cl1 = CanoLabel::new(&graph1);

        assert_eq!(cl0.get_label(), cl1.get_label());
    }

    #[test]
    fn test_conn_subgraphs_undirected_seperate_components() {
        let mut graph = UnGraphMap::<u32, u32, u32>::new();
        graph.add_node(1, Some(0));
        graph.add_node(2, Some(1));
        graph.add_node(3, Some(2));
        graph.add_node(4, Some(3));


        graph.add_edge(1, 2, Some(10));
        graph.add_edge(3, 4, Some(20));


        let cs = ConnSubgraph::new(&graph);
        let subgraphs = cs.get_subgraphs();
        assert_eq!(subgraphs.len(), 2);

        assert_eq!(subgraphs[0].has_node(1), true);
        assert_eq!(subgraphs[0].has_node(2), true);
        assert_eq!(subgraphs[0].has_node(3), false);
        assert_eq!(subgraphs[0].has_node(4), false);
        assert_eq!(subgraphs[1].has_node(1), false);
        assert_eq!(subgraphs[1].has_node(2), false);
        assert_eq!(subgraphs[1].has_node(3), true);
        assert_eq!(subgraphs[1].has_node(4), true);

        assert_eq!(subgraphs[0].has_edge(1, 2), true);
        assert_eq!(subgraphs[0].has_edge(3, 4), false);
        assert_eq!(subgraphs[1].has_edge(1, 2), false);
        assert_eq!(subgraphs[1].has_edge(3, 4), true);
        assert_eq!(subgraphs[0].has_edge(2, 1), true);
        assert_eq!(subgraphs[0].has_edge(4, 3), false);
        assert_eq!(subgraphs[1].has_edge(2, 1), false);
        assert_eq!(subgraphs[1].has_edge(4, 3), true);

        assert_eq!(subgraphs[0].get_node_label(1), Some(&0));
        assert_eq!(subgraphs[0].get_node_label(2), Some(&1));
        assert_eq!(subgraphs[1].get_node_label(3), Some(&2));
        assert_eq!(subgraphs[1].get_node_label(4), Some(&3));

        assert_eq!(graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(graph.get_edge_label(3, 4), Some(&20));
        assert_eq!(graph.get_edge_label(2, 1), Some(&10));
        assert_eq!(graph.get_edge_label(4, 3), Some(&20));
    }

    #[test]
    fn test_conn_subgraphs_directed_seperate_components() {
        let mut graph = DiGraphMap::<u32, u32, u32>::new();
        graph.add_node(1, Some(0));
        graph.add_node(2, Some(1));
        graph.add_node(3, Some(2));
        graph.add_node(4, Some(3));


        graph.add_edge(1, 2, Some(10));
        graph.add_edge(3, 4, Some(20));


        let cs = ConnSubgraph::new(&graph);
        let subgraphs = cs.get_subgraphs();
        assert_eq!(subgraphs.len(), 2);

        assert_eq!(subgraphs[0].has_node(1), true);
        assert_eq!(subgraphs[0].has_node(2), true);
        assert_eq!(subgraphs[0].has_node(3), false);
        assert_eq!(subgraphs[0].has_node(4), false);
        assert_eq!(subgraphs[1].has_node(1), false);
        assert_eq!(subgraphs[1].has_node(2), false);
        assert_eq!(subgraphs[1].has_node(3), true);
        assert_eq!(subgraphs[1].has_node(4), true);

        assert_eq!(subgraphs[0].has_edge(1, 2), true);
        assert_eq!(subgraphs[0].has_edge(3, 4), false);
        assert_eq!(subgraphs[1].has_edge(1, 2), false);
        assert_eq!(subgraphs[1].has_edge(3, 4), true);
        assert_eq!(subgraphs[0].has_edge(2, 1), false);
        assert_eq!(subgraphs[0].has_edge(4, 3), false);
        assert_eq!(subgraphs[1].has_edge(2, 1), false);
        assert_eq!(subgraphs[1].has_edge(4, 3), false);

        assert_eq!(subgraphs[0].get_node_label(1), Some(&0));
        assert_eq!(subgraphs[0].get_node_label(2), Some(&1));
        assert_eq!(subgraphs[1].get_node_label(3), Some(&2));
        assert_eq!(subgraphs[1].get_node_label(4), Some(&3));

        assert_eq!(graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(graph.get_edge_label(3, 4), Some(&20));
        assert_eq!(graph.get_edge_label(2, 1), None);
        assert_eq!(graph.get_edge_label(4, 3), None);
    }

    #[test]
    fn test_graph_union_directed_graphs() {
        let mut graph0 = DiGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_edge(1, 2, Some(10));


        let mut graph1 = DiGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let gu = GraphUnion::new(&graph0, &graph1);
        let result_graph = gu.get_result_graph();

        assert_eq!(result_graph.node_count(), 4);
        assert_eq!(result_graph.edge_count(), 2);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), true);
        assert_eq!(result_graph.has_node(4), true);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), true);
        assert_eq!(result_graph.has_edge(2, 1), false);
        assert_eq!(result_graph.has_edge(4, 3), false);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), Some(&2));
        assert_eq!(result_graph.get_node_label(4), Some(&3));

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), None);
        assert_eq!(result_graph.get_edge_label(4, 3), None);
    }

    #[test]
    fn test_graph_union_undirected_graphs() {
        let mut graph0 = UnGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_edge(1, 2, Some(10));


        let mut graph1 = UnGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let gu = GraphUnion::new(&graph0, &graph1);
        let result_graph = gu.get_result_graph();

        assert_eq!(result_graph.node_count(), 4);
        assert_eq!(result_graph.edge_count(), 2);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), true);
        assert_eq!(result_graph.has_node(4), true);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), true);
        assert_eq!(result_graph.has_edge(2, 1), true);
        assert_eq!(result_graph.has_edge(4, 3), true);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), Some(&2));
        assert_eq!(result_graph.get_node_label(4), Some(&3));

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
        assert_eq!(result_graph.get_edge_label(4, 3), Some(&20));
    }

    #[test]
    fn test_graph_add_directed_graphs() {
        let mut graph0 = DiGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_edge(1, 2, Some(10));


        let mut graph1 = DiGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let box0: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph0);
        let box1: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph1);
        let result_graph = box0 + box1;

        assert_eq!(result_graph.node_count(), 4);
        assert_eq!(result_graph.edge_count(), 2);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), true);
        assert_eq!(result_graph.has_node(4), true);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), true);
        assert_eq!(result_graph.has_edge(2, 1), false);
        assert_eq!(result_graph.has_edge(4, 3), false);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), Some(&2));
        assert_eq!(result_graph.get_node_label(4), Some(&3));

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), None);
        assert_eq!(result_graph.get_edge_label(4, 3), None);
    }

    #[test]
    fn test_graph_add_undirected_graphs() {
        let mut graph0 = UnGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_edge(1, 2, Some(10));


        let mut graph1 = UnGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let box0: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph0);
        let box1: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph1);
        let result_graph = box0 + box1;

        assert_eq!(result_graph.node_count(), 4);
        assert_eq!(result_graph.edge_count(), 2);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), true);
        assert_eq!(result_graph.has_node(4), true);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), true);
        assert_eq!(result_graph.has_edge(2, 1), true);
        assert_eq!(result_graph.has_edge(4, 3), true);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), Some(&2));
        assert_eq!(result_graph.get_node_label(4), Some(&3));

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
        assert_eq!(result_graph.get_edge_label(4, 3), Some(&20));
    }

    #[test]
    fn test_graph_add_boxed_directed_generalgraphs() {
        let mut graph0 = DiGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_edge(1, 2, Some(10));


        let mut graph1 = DiGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));


        let box0: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph0);
        let box1: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph1);
        let result_graph = box0 + box1;

        assert_eq!(result_graph.node_count(), 4);
        assert_eq!(result_graph.edge_count(), 2);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), true);
        assert_eq!(result_graph.has_node(4), true);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), true);
        assert_eq!(result_graph.has_edge(2, 1), false);
        assert_eq!(result_graph.has_edge(4, 3), false);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), Some(&2));
        assert_eq!(result_graph.get_node_label(4), Some(&3));

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), None);
        assert_eq!(result_graph.get_edge_label(4, 3), None);
    }

    #[test]
    fn test_graph_add_boxed_undirected_generalgraphs() {
        let mut graph0 = UnGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_edge(1, 2, Some(10));


        let mut graph1 = UnGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let box0: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph0);
        let box1: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph1);
        let result_graph = box0 + box1;

        assert_eq!(result_graph.node_count(), 4);
        assert_eq!(result_graph.edge_count(), 2);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), true);
        assert_eq!(result_graph.has_node(4), true);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), true);
        assert_eq!(result_graph.has_edge(2, 1), true);
        assert_eq!(result_graph.has_edge(4, 3), true);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), Some(&2));
        assert_eq!(result_graph.get_node_label(4), Some(&3));

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
        assert_eq!(result_graph.get_edge_label(4, 3), Some(&20));
    }

    #[test]
    fn test_graph_minus_directed_graphs() {
        let mut graph0 = DiGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_node(3, Some(2));
        graph0.add_node(4, Some(3));
        graph0.add_edge(1, 2, Some(10));
        graph0.add_edge(3, 4, Some(20));


        let mut graph1 = DiGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let gm = GraphMinus::new(&graph0, &graph1);
        let result_graph = gm.get_result_graph();
        assert_eq!(result_graph.node_count(), 2);
        assert_eq!(result_graph.edge_count(), 1);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), false);
        assert_eq!(result_graph.has_node(4), false);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), false);
        assert_eq!(result_graph.has_edge(2, 1), false);
        assert_eq!(result_graph.has_edge(4, 3), false);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), None);
        assert_eq!(result_graph.get_node_label(4), None);

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), None);
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), None);
        assert_eq!(result_graph.get_edge_label(4, 3), None);
    }

    #[test]
    fn test_graph_minus_undirected_graphs() {
        let mut graph0 = UnGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_node(3, Some(2));
        graph0.add_node(4, Some(3));
        graph0.add_edge(1, 2, Some(10));
        graph0.add_edge(3, 4, Some(20));


        let mut graph1 = UnGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let gm = GraphMinus::new(&graph0, &graph1);
        let result_graph = gm.get_result_graph();
        assert_eq!(result_graph.node_count(), 2);
        assert_eq!(result_graph.edge_count(), 1);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), false);
        assert_eq!(result_graph.has_node(4), false);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), false);
        assert_eq!(result_graph.has_edge(2, 1), true);
        assert_eq!(result_graph.has_edge(4, 3), false);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), None);
        assert_eq!(result_graph.get_node_label(4), None);

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), None);
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
        assert_eq!(result_graph.get_edge_label(4, 3), None);
    }

    #[test]
    fn test_graph_sub_directed_graphs() {
        let mut graph0 = DiGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_node(3, Some(2));
        graph0.add_node(4, Some(3));
        graph0.add_edge(1, 2, Some(10));
        graph0.add_edge(3, 4, Some(20));


        let mut graph1 = DiGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let result_graph = graph0 - graph1;

        assert_eq!(result_graph.node_count(), 2);
        assert_eq!(result_graph.edge_count(), 1);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), false);
        assert_eq!(result_graph.has_node(4), false);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), false);
        assert_eq!(result_graph.has_edge(2, 1), false);
        assert_eq!(result_graph.has_edge(4, 3), false);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), None);
        assert_eq!(result_graph.get_node_label(4), None);

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), None);
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), None);
        assert_eq!(result_graph.get_edge_label(4, 3), None);
    }

    #[test]
    fn test_graph_sub_undirected_graphs() {
        let mut graph0 = UnGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_node(3, Some(2));
        graph0.add_node(4, Some(3));
        graph0.add_edge(1, 2, Some(10));
        graph0.add_edge(3, 4, Some(20));


        let mut graph1 = UnGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let result_graph = graph0 - graph1;

        assert_eq!(result_graph.node_count(), 2);
        assert_eq!(result_graph.edge_count(), 1);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), false);
        assert_eq!(result_graph.has_node(4), false);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), false);
        assert_eq!(result_graph.has_edge(2, 1), true);
        assert_eq!(result_graph.has_edge(4, 3), false);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), None);
        assert_eq!(result_graph.get_node_label(4), None);

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), None);
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
        assert_eq!(result_graph.get_edge_label(4, 3), None);
    }

    #[test]
    fn test_graph_sub_boxed_directed_generalgraphs() {
        let mut graph0 = DiGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_node(3, Some(2));
        graph0.add_node(4, Some(3));
        graph0.add_edge(1, 2, Some(10));
        graph0.add_edge(3, 4, Some(20));


        let mut graph1 = DiGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));


        let box0: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph0);
        let box1: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph1);
        let result_graph = box0 - box1;

        assert_eq!(result_graph.node_count(), 2);
        assert_eq!(result_graph.edge_count(), 1);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), false);
        assert_eq!(result_graph.has_node(4), false);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), false);
        assert_eq!(result_graph.has_edge(2, 1), false);
        assert_eq!(result_graph.has_edge(4, 3), false);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), None);
        assert_eq!(result_graph.get_node_label(4), None);

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), None);
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), None);
        assert_eq!(result_graph.get_edge_label(4, 3), None);
    }

    #[test]
    fn test_graph_sub_boxed_undirected_generalgraphs() {
        let mut graph0 = UnGraphMap::<u32, u32, u32>::new();
        graph0.add_node(1, Some(0));
        graph0.add_node(2, Some(1));
        graph0.add_node(3, Some(2));
        graph0.add_node(4, Some(3));
        graph0.add_edge(1, 2, Some(10));
        graph0.add_edge(3, 4, Some(20));


        let mut graph1 = UnGraphMap::<u32, u32, u32>::new();
        graph1.add_node(3, Some(2));
        graph1.add_node(4, Some(3));
        graph1.add_edge(3, 4, Some(20));

        let box0: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph0);
        let box1: Box<GeneralGraph<u32, u32, u32, u32>> = Box::new(graph1);
        let result_graph = box0 - box1;

        assert_eq!(result_graph.node_count(), 2);
        assert_eq!(result_graph.edge_count(), 1);

        assert_eq!(result_graph.has_node(1), true);
        assert_eq!(result_graph.has_node(2), true);
        assert_eq!(result_graph.has_node(3), false);
        assert_eq!(result_graph.has_node(4), false);

        assert_eq!(result_graph.has_edge(1, 2), true);
        assert_eq!(result_graph.has_edge(3, 4), false);
        assert_eq!(result_graph.has_edge(2, 1), true);
        assert_eq!(result_graph.has_edge(4, 3), false);
        assert_eq!(result_graph.has_edge(2, 3), false);
        assert_eq!(result_graph.has_edge(1, 4), false);

        assert_eq!(result_graph.get_node_label(1), Some(&0));
        assert_eq!(result_graph.get_node_label(2), Some(&1));
        assert_eq!(result_graph.get_node_label(3), None);
        assert_eq!(result_graph.get_node_label(4), None);

        assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
        assert_eq!(result_graph.get_edge_label(3, 4), None);
        assert_eq!(result_graph.get_edge_label(1, 4), None);
        assert_eq!(result_graph.get_edge_label(2, 3), None);
        assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
        assert_eq!(result_graph.get_edge_label(4, 3), None);
    }


}

