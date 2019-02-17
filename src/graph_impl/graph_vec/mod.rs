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

use std::hash::Hash;

use generic::{DefaultId, GraphType, IdType, MutMapTrait};
use graph_impl::static_graph::edge_vec::EdgeVecTrait;
use graph_impl::{EdgeVec, TypedStaticGraph};
use map::SetMap;

pub type GraphVec<NL, EL = NL, L = DefaultId> = TypedGraphVec<DefaultId, NL, EL, L>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedGraphVec<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType = Id> {
    nodes: Vec<(Id, L)>,
    edges: Vec<(Id, Id, L)>,
    in_edges: Vec<(Id, Id)>,
    node_label_map: SetMap<NL>,
    edge_label_map: SetMap<EL>,

    max_id: Option<Id>,
    has_node_label: bool,
    has_edge_label: bool,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> TypedGraphVec<Id, NL, EL, L> {
    pub fn new() -> Self {
        TypedGraphVec {
            nodes: Vec::new(),
            edges: Vec::new(),
            in_edges: Vec::new(),
            node_label_map: SetMap::new(),
            edge_label_map: SetMap::new(),

            max_id: None,
            has_node_label: false,
            has_edge_label: false,
        }
    }

    pub fn with_label_map(node_label_map: SetMap<NL>, edge_label_map: SetMap<EL>) -> Self {
        TypedGraphVec {
            nodes: Vec::new(),
            edges: Vec::new(),
            in_edges: Vec::new(),
            node_label_map,
            edge_label_map,

            max_id: None,
            has_node_label: false,
            has_edge_label: false,
        }
    }

    #[inline]
    pub fn add_node(&mut self, id: Id, label: Option<NL>) {
        let label_id = match label {
            Some(l) => {
                if !self.has_node_label {
                    self.has_node_label = true;
                }
                L::new(self.node_label_map.add_item(l))
            }
            None => L::max_value(),
        };

        if self.max_id.map_or(false, |m| id > m) {
            self.max_id = Some(id);
        }

        self.nodes.push((id, label_id));
    }

    #[inline]
    pub fn add_edge(&mut self, src: Id, dst: Id, label: Option<EL>) {
        let label_id = match label {
            Some(l) => {
                if !self.has_edge_label {
                    self.has_edge_label = true;
                }
                L::new(self.edge_label_map.add_item(l))
            }
            None => L::max_value(),
        };

        if self.max_id.map_or(false, |m| src > m) {
            self.max_id = Some(src);
        }

        if self.max_id.map_or(false, |m| dst > m) {
            self.max_id = Some(dst);
        }

        self.edges.push((src, dst, label_id));
    }

    #[inline]
    pub fn add_in_edge(&mut self, src: Id, dst: Id) {
        if self.max_id.map_or(false, |m| src > m) {
            self.max_id = Some(src);
        }

        if self.max_id.map_or(false, |m| dst > m) {
            self.max_id = Some(dst);
        }

        self.in_edges.push((src, dst));
    }

    #[inline(always)]
    pub fn is_directed(&self) -> bool {
        !self.in_edges.is_empty()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    #[inline(always)]
    pub fn node_count(&self) -> usize {
        self.edges.len()
    }

    #[inline(always)]
    pub fn edge_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn into_static<Ty: GraphType>(self) -> TypedStaticGraph<Id, NL, EL, Ty, L> {
        if self.is_empty() {
            return TypedStaticGraph::empty();
        }

        let (node_labels, num_of_nodes) =
            Self::get_node_labels(self.nodes, self.max_id.unwrap(), self.has_node_label);
        let edge_vec = Self::get_edge_vec(self.edges, num_of_nodes, self.has_edge_label);
        let in_edge_vec = if Ty::is_directed() {
            Some(Self::get_in_edge_vec(self.in_edges, num_of_nodes))
        } else {
            None
        };

        TypedStaticGraph::from_raw(
            num_of_nodes,
            edge_vec.num_edges(),
            edge_vec,
            in_edge_vec,
            node_labels,
            self.node_label_map,
            self.edge_label_map,
        )
    }

    fn get_node_labels(
        mut nodes: Vec<(Id, L)>,
        max_node_id: Id,
        has_node_label: bool,
    ) -> (Option<Vec<L>>, usize) {
        nodes.sort_unstable_by_key(|&(i, _)| i);
        nodes.dedup_by_key(|&mut (i, _)| i);

        if !has_node_label {
            return (None, max_node_id.id() + 1);
        }

        let mut labels = Vec::new();
        let mut current = Id::new(0);

        for (i, l) in nodes.into_iter() {
            while i > current {
                labels.push(L::max_value());
                current.increment();
            }
            labels.push(l);
            current.increment();
        }

        (Some(labels), max_node_id.id() + 1)
    }

    fn get_edge_vec(
        mut graph: Vec<(Id, Id, L)>,
        num_of_nodes: usize,
        has_edge_label: bool,
    ) -> EdgeVec<Id, L> {
        let mut offsets = Vec::new();
        let mut edges = Vec::new();
        let mut labels = if has_edge_label {
            Some(Vec::new())
        } else {
            None
        };

        let mut offset = 0usize;
        offsets.push(offset);

        graph.sort_unstable_by_key(|&(s, d, _)| (s, d));
        graph.dedup_by_key(|&mut (s, d, _)| (s, d));

        let mut current = Id::new(0);
        let last = graph.last().map_or(0, |&(i, _, _)| i.id());

        for (s, d, l) in graph.into_iter() {
            while s > current {
                offsets.push(offset);
                current.increment();
            }

            edges.push(d);
            if let Some(_labels) = labels.as_mut() {
                _labels.push(l);
            }

            offset += 1;
        }

        offset = edges.len();
        offsets.push(offset);

        if last + 1 < num_of_nodes {
            for _ in 0..num_of_nodes - last {
                offsets.push(offset);
            }
        }

        EdgeVec::from_raw(offsets, edges, labels)
    }

    fn get_in_edge_vec(mut graph: Vec<(Id, Id)>, num_of_nodes: usize) -> EdgeVec<Id, L> {
        let mut offsets = Vec::new();
        let mut edges = Vec::new();

        let mut offset = 0usize;
        offsets.push(offset);

        graph.sort_unstable();
        graph.dedup();

        let mut current = Id::new(0);
        let last = graph.last().map_or(0, |&(i, _)| i.id());

        for (s, d) in graph.into_iter() {
            while s > current {
                offsets.push(offset);
                current.increment();
            }

            edges.push(d);
            offset += 1;
        }

        offset = edges.len();
        offsets.push(offset);

        if last + 1 < num_of_nodes {
            for _ in 0..num_of_nodes - last {
                offsets.push(offset);
            }
        }

        EdgeVec::new(offsets, edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_vec() {
        let mut g = GraphVec::<&str>::new();
        g.add_node(0, Some("node0"));
        g.add_node(2, Some("node2"));
        g.add_node(2, Some("node2"));
        g.add_edge(0, 1, Some("(0,1)"));
        g.add_edge(1, 0, Some("(0,2)"));
    }
}
