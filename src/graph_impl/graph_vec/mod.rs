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

use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;

use generic::{DefaultId, GraphType, IdType, MutMapTrait};
use graph_impl::static_graph::edge_vec::EdgeVecTrait;
use graph_impl::{EdgeVec, TypedStaticGraph};
use map::SetMap;

pub type GraphVec<NL, EL = NL, L = DefaultId> = TypedGraphVec<DefaultId, NL, EL, L>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedGraphVec<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType = Id> {
    nodes: BTreeMap<Id, L>,
    edges: BTreeMap<(Id, Id), L>,
    in_edges: BTreeSet<(Id, Id)>,
    node_label_map: SetMap<NL>,
    edge_label_map: SetMap<EL>,

    max_id: Option<Id>,
    has_node_label: bool,
    has_edge_label: bool,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> TypedGraphVec<Id, NL, EL, L> {
    pub fn new() -> Self {
        TypedGraphVec {
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
            in_edges: BTreeSet::new(),
            node_label_map: SetMap::new(),
            edge_label_map: SetMap::new(),

            max_id: None,
            has_node_label: false,
            has_edge_label: false,
        }
    }

    pub fn with_label_map(node_label_map: SetMap<NL>, edge_label_map: SetMap<EL>) -> Self {
        TypedGraphVec {
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
            in_edges: BTreeSet::new(),
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

        if self.max_id.map_or(true, |m| id > m) {
            self.max_id = Some(id);
        }

        self.nodes.insert(id, label_id);
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

        if self.max_id.map_or(true, |m| src > m) {
            self.max_id = Some(src);
        }

        if self.max_id.map_or(true, |m| dst > m) {
            self.max_id = Some(dst);
        }

        self.edges.insert((src, dst), label_id);
    }

    #[inline]
    pub fn add_in_edge(&mut self, src: Id, dst: Id) {
        if self.max_id.map_or(true, |m| src > m) {
            self.max_id = Some(src);
        }

        if self.max_id.map_or(true, |m| dst > m) {
            self.max_id = Some(dst);
        }

        self.in_edges.insert((src, dst));
    }

    #[inline(always)]
    pub fn is_directed(&self) -> bool {
        !self.in_edges.is_empty()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty() && self.in_edges.is_empty()
    }

    #[inline(always)]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[inline(always)]
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn into_static<Ty: GraphType>(self) -> TypedStaticGraph<Id, NL, EL, Ty, L> {
        if self.is_empty() {
            return TypedStaticGraph::empty();
        }

        let max_id = self.max_id.unwrap();

        let node_labels = Self::get_node_labels(self.nodes, max_id, self.has_node_label);
        let edge_vec = Self::get_edge_vec(self.edges, max_id, self.has_edge_label);
        let in_edge_vec = if Ty::is_directed() {
            Some(Self::get_in_edge_vec(self.in_edges, max_id))
        } else {
            None
        };

        TypedStaticGraph::from_raw(
            max_id.id() + 1,
            if Ty::is_directed() {
                edge_vec.num_edges()
            } else {
                edge_vec.num_edges() >> 1
            },
            edge_vec,
            in_edge_vec,
            node_labels,
            self.node_label_map,
            self.edge_label_map,
        )
    }

    fn get_node_labels(
        nodes: BTreeMap<Id, L>,
        max_node_id: Id,
        has_node_label: bool,
    ) -> Option<Vec<L>> {
        if !has_node_label {
            return None;
        }

        let mut labels = Vec::new();
        let mut current = Id::new(0);

        let mut last = Id::new(0);

        for (i, l) in nodes.into_iter() {
            while i > current {
                labels.push(L::max_value());
                current.increment();
            }
            labels.push(l);
            current.increment();

            last = i;
        }

        let last = last.id();
        if last < max_node_id.id() {
            for _ in 0..max_node_id.id() - last {
                labels.push(L::max_value());
            }
        }

        Some(labels)
    }

    fn get_edge_vec(
        graph: BTreeMap<(Id, Id), L>,
        max_node_id: Id,
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

        let mut current = Id::new(0);

        let mut last = Id::new(0);

        for ((s, d), l) in graph.into_iter() {
            while s > current {
                offsets.push(offset);
                current.increment();
            }

            edges.push(d);
            if let Some(_labels) = labels.as_mut() {
                _labels.push(l);
            }

            offset += 1;

            last = s;
        }

        offset = edges.len();
        offsets.push(offset);

        let last = last.id();
        if last < max_node_id.id() {
            for _ in 0..max_node_id.id() - last {
                offsets.push(offset);
            }
        }

        EdgeVec::from_raw(offsets, edges, labels)
    }

    fn get_in_edge_vec(graph: BTreeSet<(Id, Id)>, max_node_id: Id) -> EdgeVec<Id, L> {
        let mut offsets = Vec::new();
        let mut edges = Vec::new();

        let mut offset = 0usize;
        offsets.push(offset);

        let mut current = Id::new(0);

        let mut last = Id::new(0);

        for (s, d) in graph.into_iter() {
            while s > current {
                offsets.push(offset);
                current.increment();
            }

            edges.push(d);
            offset += 1;

            last = s;
        }

        offset = edges.len();
        offsets.push(offset);

        let last = last.id();
        if last < max_node_id.id() {
            for _ in 0..max_node_id.id() - last {
                offsets.push(offset);
            }
        }

        EdgeVec::new(offsets, edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_impl::{DiStaticGraph, UnStaticGraph};
    use prelude::*;

    #[test]
    fn test_undirected() {
        let mut g = GraphVec::<&str>::new();
        g.add_node(0, Some("node0"));
        g.add_node(2, Some("node2"));
        g.add_node(2, Some("node2"));
        g.add_edge(0, 1, Some("(0,1)"));
        g.add_edge(1, 0, Some("(0,1)"));
        g.add_edge(0, 3, Some("(0,3)"));

        let un_graph = g.clone().into_static::<Undirected>();

        let un_graph_true = UnStaticGraph::<&str>::from_raw(
            4,
            1,
            EdgeVec::with_labels(vec![0, 2, 3, 3, 3], vec![1, 3, 0], vec![0, 1, 0]),
            None,
            Some(vec![0, u32::max_value(), 1, u32::max_value()]),
            vec!["node0", "node2"].into(),
            vec!["(0,1)", "(0,3)"].into(),
        );
        assert_eq!(format!("{:?}", un_graph), format!("{:?}", un_graph_true));
    }

    #[test]
    fn test_directed() {
        let mut g = GraphVec::<&str>::new();
        g.add_node(0, Some("node0"));
        g.add_node(2, Some("node2"));
        g.add_node(2, Some("node2"));
        g.add_edge(0, 1, Some("(0,1)"));
        g.add_in_edge(1, 0);
        g.add_edge(0, 3, Some("(0,3)"));

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 2);

        let di_graph = g.clone().into_static::<Directed>();

        let di_graph_true = DiStaticGraph::<&str>::from_raw(
            4,
            2,
            EdgeVec::with_labels(vec![0, 2, 2, 2, 2], vec![1, 3], vec![0, 1]),
            Some(EdgeVec::new(vec![0, 0, 1, 1, 1], vec![0])),
            Some(vec![0, u32::max_value(), 1, u32::max_value()]),
            vec!["node0", "node2"].into(),
            vec!["(0,1)", "(0,3)"].into(),
        );

        assert_eq!(format!("{:?}", di_graph), format!("{:?}", di_graph_true));
    }

    #[test]
    fn test_no_node() {
        let mut g = GraphVec::<&str>::new();
        g.add_edge(0, 1, None);
        g.add_edge(0, 2, None);
        g.add_edge(1, 0, None);
        g.add_edge(2, 0, None);

        let un_graph = g.clone().into_static::<Undirected>();

        let un_graph_true = UnStaticGraph::<()>::from_raw(
            3,
            2,
            EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]),
            None,
            None,
            SetMap::new(),
            SetMap::new(),
        );

        assert_eq!(format!("{:?}", un_graph), format!("{:?}", un_graph_true));
    }
}