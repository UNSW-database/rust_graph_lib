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
use std::borrow::Cow;
use std::fs::metadata;
use std::hash::Hash;

use serde;

use generic::{
    DiGraphTrait, EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType,
};
use graph_impl::static_graph::mmap::EdgeVecMmap;
use graph_impl::static_graph::node::StaticNode;
use graph_impl::static_graph::static_edge_iter::StaticEdgeIndexIter;
use graph_impl::static_graph::EdgeVecTrait;
use graph_impl::{Edge, Graph};
use io::mmap::TypedMemoryMap;
use io::serde::{Deserialize, Deserializer};
use map::SetMap;

pub struct StaticGraphMmap<Id: IdType, NL: Hash + Eq, EL: Hash + Eq = NL, L: IdType = Id> {
    /// Outgoing edges, or edges for undirected
    edges: EdgeVecMmap<Id, L>,
    /// Incoming edges for directed, `None` for undirected
    in_edges: Option<EdgeVecMmap<Id, L>>,
    /// Maintain the node's labels, whose index is aligned with `offsets`.
    labels: Option<TypedMemoryMap<L>>,

    num_nodes: usize,
    num_edges: usize,
    node_label_map: SetMap<NL>,
    edge_label_map: SetMap<EL>,
}

#[derive(Serialize, Deserialize)]
pub struct StaticGraphMmapAux<NL: Hash + Eq, EL: Hash + Eq = NL> {
    num_nodes: usize,
    num_edges: usize,
    node_label_map: SetMap<NL>,
    edge_label_map: SetMap<EL>,
}

impl<NL: Hash + Eq, EL: Hash + Eq> StaticGraphMmapAux<NL, EL> {
    pub fn new(
        num_nodes: usize,
        num_edges: usize,
        node_label_map: SetMap<NL>,
        edge_label_map: SetMap<EL>,
    ) -> Self {
        StaticGraphMmapAux {
            num_nodes,
            num_edges,
            node_label_map,
            edge_label_map,
        }
    }

    pub fn empty(num_nodes: usize, num_edges: usize) -> Self {
        StaticGraphMmapAux {
            num_nodes,
            num_edges,
            node_label_map: SetMap::new(),
            edge_label_map: SetMap::new(),
        }
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> StaticGraphMmap<Id, NL, EL, L>
where
    for<'de> NL: serde::Deserialize<'de>,
    for<'de> EL: serde::Deserialize<'de>,
{
    pub fn new(prefix: &str) -> Self {
        let edge_prefix = format!("{}_OUT", prefix);
        let in_edge_prefix = format!("{}_IN", prefix);
        let labels_file = format!("{}.labels", prefix);

        let aux_map_file = format!("{}_aux.bin", prefix);

        let edges = EdgeVecMmap::new(&edge_prefix);

        let in_edges = if metadata(&format!("{}.offsets", in_edge_prefix)).is_ok() {
            Some(EdgeVecMmap::new(&in_edge_prefix))
        } else {
            None
        };

        let labels = if metadata(&labels_file).is_ok() {
            Some(TypedMemoryMap::new(&labels_file))
        } else {
            None
        };

        let aux_file = if metadata(&aux_map_file).is_ok() {
            Deserializer::import(&aux_map_file).unwrap()
        } else {
            let num_node = edges.num_nodes();
            let num_edge = if in_edges.is_some() {
                edges.num_edges()
            } else {
                edges.num_edges() >> 1
            };

            StaticGraphMmapAux::empty(num_node, num_edge)
        };

        StaticGraphMmap {
            num_nodes: aux_file.num_nodes,
            num_edges: aux_file.num_edges,
            edges,
            in_edges,
            labels,
            node_label_map: aux_file.node_label_map,
            edge_label_map: aux_file.edge_label_map,
        }
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> StaticGraphMmap<Id, NL, EL, L> {
    #[inline]
    pub fn inner_neighbors(&self, id: Id) -> &[Id] {
        self.edges.neighbors(id)
    }

    #[inline]
    pub fn inner_in_neighbors(&self, id: Id) -> &[Id] {
        if let Some(ref in_edges) = self.in_edges {
            in_edges.neighbors(id)
        } else {
            &[]
        }
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GraphTrait<Id, L>
    for StaticGraphMmap<Id, NL, EL, L>
{
    #[inline]
    fn get_node(&self, id: Id) -> NodeType<Id, L> {
        if !self.has_node(id) {
            return NodeType::None;
        }

        match self.labels {
            Some(ref labels) => {
                NodeType::StaticNode(StaticNode::new_static(id, labels[..][id.id()]))
            }
            None => NodeType::StaticNode(StaticNode::new(id, None)),
        }
    }

    #[inline]
    fn get_edge(&self, start: Id, target: Id) -> EdgeType<Id, L> {
        if !self.has_edge(start, target) {
            return None;
        }

        let _label = self.edges.find_edge_label_id(start, target);
        match _label {
            Some(label) => Some(Edge::new_static(start, target, *label)),
            None => Some(Edge::new(start, target, None)),
        }
    }

    #[inline]
    fn has_node(&self, id: Id) -> bool {
        id.id() < self.num_nodes
    }

    #[inline]
    fn has_edge(&self, start: Id, target: Id) -> bool {
        let neighbors = self.neighbors(start);
        // The neighbors must be sorted anyway
        let pos = neighbors.binary_search(&target);

        pos.is_ok()
    }

    #[inline]
    fn node_count(&self) -> usize {
        self.num_nodes
    }

    #[inline]
    fn edge_count(&self) -> usize {
        self.num_edges
    }

    #[inline]
    fn is_directed(&self) -> bool {
        // A directed graph should have in-coming edges ready
        self.in_edges.is_some()
    }

    #[inline]
    fn node_indices(&self) -> Iter<Id> {
        Iter::new(Box::new((0..self.num_nodes).map(|x| Id::new(x))))
    }

    #[inline]
    fn edge_indices(&self) -> Iter<(Id, Id)> {
        Iter::new(Box::new(StaticEdgeIndexIter::new(
            Box::new(&self.edges),
            self.is_directed(),
        )))
    }

    #[inline]
    fn nodes(&self) -> Iter<NodeType<Id, L>> {
        match self.labels {
            None => Iter::new(Box::new(
                self.node_indices()
                    .map(|i| NodeType::StaticNode(StaticNode::new(i, None))),
            )),
            Some(ref labels) => Iter::new(Box::new(
                self.node_indices()
                    .zip(labels[..].iter())
                    .map(|n| NodeType::StaticNode(StaticNode::new_static(n.0, *n.1))),
            )),
        }
    }

    #[inline]
    fn edges(&self) -> Iter<EdgeType<Id, L>> {
        let labels = self.edges.get_labels();
        if labels.is_empty() {
            Iter::new(Box::new(
                self.edge_indices().map(|i| Some(Edge::new(i.0, i.1, None))),
            ))
        } else {
            Iter::new(Box::new(
                self.edge_indices()
                    .zip(labels.iter())
                    .map(|e| Some(Edge::new_static((e.0).0, (e.0).1, *e.1))),
            ))
        }
    }

    #[inline]
    fn degree(&self, id: Id) -> usize {
        self.neighbors(id).len()
    }

    #[inline]
    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        Iter::new(Box::new(self.edges.neighbors(id).iter().map(|x| *x)))
    }

    #[inline]
    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        self.edges.neighbors(id).into()
    }

    #[inline]
    fn max_seen_id(&self) -> Option<Id> {
        Some(Id::new(self.node_count() - 1))
    }

    #[inline(always)]
    fn implementation(&self) -> Graph {
        Graph::StaicGraphMmap
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GraphLabelTrait<Id, NL, EL, L>
    for StaticGraphMmap<Id, NL, EL, L>
{
    #[inline(always)]
    fn get_node_label_map(&self) -> &SetMap<NL> {
        &self.node_label_map
    }

    #[inline(always)]
    fn get_edge_label_map(&self) -> &SetMap<EL> {
        &self.edge_label_map
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> DiGraphTrait<Id, L>
    for StaticGraphMmap<Id, NL, EL, L>
{
    #[inline]
    fn in_degree(&self, id: Id) -> usize {
        self.inner_in_neighbors(id).len()
    }

    #[inline]
    fn in_neighbors_iter(&self, id: Id) -> Iter<Id> {
        Iter::new(Box::new(self.inner_in_neighbors(id).iter().map(|x| *x)))
    }

    #[inline]
    fn in_neighbors(&self, id: Id) -> Cow<[Id]> {
        self.inner_in_neighbors(id).into()
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GeneralGraph<Id, NL, EL, L>
    for StaticGraphMmap<Id, NL, EL, L>
{
    #[inline(always)]
    fn as_graph(&self) -> &GraphTrait<Id, L> {
        self
    }

    #[inline(always)]
    fn as_labeled_graph(&self) -> &GraphLabelTrait<Id, NL, EL, L> {
        self
    }

    #[inline(always)]
    fn as_digraph(&self) -> Option<&DiGraphTrait<Id, L>> {
        if self.is_directed() {
            Some(self)
        } else {
            None
        }
    }
}
