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
    use rust_graph::prelude::*;
    use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
    use rust_graph::algorithm::conn_comp::ConnComp;
    use rust_graph::algorithm::dfs::Dfs;
    use rust_graph::algorithm::bfs::Bfs;

    #[test]
    fn test_cc_undirected_one_component() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);

        let mut cc = ConnComp::new(&graph);

        assert_eq!(cc.count, 1);

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

        let mut cc = ConnComp::new(&graph);

        assert_eq!(cc.count, 2);

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

        let mut cc = ConnComp::new(&graph);

        assert_eq!(cc.count, 1);

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

        let mut cc = ConnComp::new(&graph);

        assert_eq!(cc.count, 2);

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
}