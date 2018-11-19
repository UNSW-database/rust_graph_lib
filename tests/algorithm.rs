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
    use super::*;
    use generic::MutGraphTrait;
    use UnGraphMap;
    #[test]
    fn dfs_test() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);
        graph.add_edge(3, 1, None);
        graph.add_edge(3, 4, None);
        let res = dfs(&graph, Some(1));
        assert_eq!(vec![1, 2, 3, 4], res);
    }
    #[test]
    fn dfs_stack_test() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);
        graph.add_edge(3, 1, None);
        graph.add_edge(3, 4, None);
        let res = dfs_stack(&graph, Some(1));
        assert_eq!(vec![1, 2, 3, 4], res);
    }
    #[test]
    fn dfs_iter_test() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);
        graph.add_edge(3, 1, None);
        graph.add_edge(3, 4, None);
        let res = DFS::new(&graph, Some(1));
        assert_eq!(vec![1, 2, 3, 4], res.unwrap().collect_vec());
    }

}