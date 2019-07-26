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

use hashbrown::HashMap;
use rayon::prelude::*;

use crate::generic::{DefaultId, GraphType, IdType, MutMapTrait};
use crate::graph_impl::static_graph::edge_vec::EdgeVecTrait;
use crate::graph_impl::static_graph::edge_vec::OffsetIndex;
use crate::graph_impl::{EdgeVec, TypedStaticGraph};
use itertools::Itertools;
use crate::map::SetMap;

pub type GraphVec<NL, EL = NL, L = DefaultId> = TypedGraphVec<DefaultId, NL, EL, L>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedGraphVec<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType = Id> {
    nodes: HashMap<Id, L>,
    edges: Vec<((Id, Id), L)>,
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
            nodes: HashMap::new(),
            edges: Vec::new(),
            in_edges: Vec::new(),
            node_label_map: SetMap::new(),
            edge_label_map: SetMap::new(),

            max_id: None,
            has_node_label: false,
            has_edge_label: false,
        }
    }

    pub fn with_capacity(nodes: usize, edges: usize) -> Self {
        TypedGraphVec {
            nodes: HashMap::with_capacity(nodes),
            edges: Vec::with_capacity(edges),
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
            nodes: HashMap::new(),
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

        self.set_max(id);

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

        self.set_max(src);
        self.set_max(dst);

        self.edges.push(((src, dst), label_id));
    }

    #[inline]
    pub fn add_in_edge(&mut self, src: Id, dst: Id) {
        self.set_max(src);
        self.set_max(dst);

        self.in_edges.push((src, dst));
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

    pub fn into_static<Ty: GraphType, OL: IdType>(self) -> TypedStaticGraph<Id, NL, EL, Ty, OL> {
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

    fn get_node_labels<OL: IdType>(
        nodes: HashMap<Id, L>,
        max_node_id: Id,
        has_node_label: bool,
    ) -> Option<Vec<OL>> {
        info!("Creating node labels");

        if !has_node_label {
            return None;
        }
        let mut nodes = nodes.into_iter().collect_vec();

        // TODO
        nodes.par_sort_unstable();
        //        nodes.dedup_by_key(|&mut (i, _)| i);

        let mut labels = Vec::new();
        let mut current = Id::new(0);

        let mut last = Id::new(0);

        for (i, l) in nodes.into_iter() {
            while i > current {
                labels.push(OL::max_value());
                current.increment();
            }
            labels.push(OL::new(l.id()));
            current.increment();

            last = i;
        }

        let last = last.id();
        if last < max_node_id.id() {
            for _ in 0..max_node_id.id() - last {
                labels.push(OL::max_value());
            }
        }

        Some(labels)
    }

    fn get_edge_vec<OL: IdType>(
        mut graph: Vec<((Id, Id), L)>,
        max_node_id: Id,
        has_edge_label: bool,
    ) -> EdgeVec<Id, OL> {
        info!("Creating edges");

        // TODO
        graph.par_sort_unstable();
        graph.dedup_by_key(|&mut (e, _)| e);

        let mut offsets = OffsetIndex::new();
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
                _labels.push(OL::new(l.id()));
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

        EdgeVec::from_raw_index(offsets, edges, labels)
    }

    fn get_in_edge_vec<OL: IdType>(mut graph: Vec<(Id, Id)>, max_node_id: Id) -> EdgeVec<Id, OL> {
        info!("Creating in-edges");

        // TODO
        graph.par_sort_unstable();

        let iter = graph.into_iter().dedup();

        let mut offsets = Vec::new();
        let mut edges = Vec::new();

        let mut offset = 0usize;
        offsets.push(offset);

        let mut current = Id::new(0);

        let mut last = Id::new(0);

        for (s, d) in iter {
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

    fn set_max(&mut self, id: Id) {
        if self.max_id.map_or(true, |m| id > m) {
            self.max_id = Some(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_impl::{DiStaticGraph, UnStaticGraph};
    use crate::prelude::*;

    #[test]
    fn test_undirected() {
        let mut g = GraphVec::<&str>::new();
        g.add_node(0, Some("node0"));
        g.add_node(2, Some("node2"));
        g.add_node(2, Some("node2"));
        g.add_edge(0, 1, Some("(0,1)"));
        g.add_edge(1, 0, Some("(0,1)"));
        g.add_edge(0, 3, Some("(0,3)"));

        let un_graph = g.clone().into_static::<Undirected, u16>();

        let un_graph_true = UnStaticGraph::<&str, &str, u16>::from_raw(
            4,
            1,
            EdgeVec::with_labels(vec![0, 2, 3, 3, 3], vec![1, 3, 0], vec![0, 1, 0]),
            None,
            Some(vec![0, u16::max_value(), 1, u16::max_value()]),
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
        g.add_edge(0, 1, Some("(0,1)"));
        g.add_in_edge(1, 0);
        g.add_edge(0, 3, Some("(0,3)"));

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 2);

        let di_graph = g.clone().into_static::<Directed, u32>();

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

        let un_graph = g.clone().into_static::<Undirected, u32>();

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
