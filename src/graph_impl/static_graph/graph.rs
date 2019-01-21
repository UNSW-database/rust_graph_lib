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
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use bincode::Result;
use itertools::Itertools;
use serde;

use generic::{
    DefaultId, DefaultTy, DiGraphTrait, Directed, EdgeType, GeneralGraph, GraphLabelTrait,
    GraphTrait, GraphType, IdType, Iter, NodeType, UnGraphTrait, Undirected,
};
use graph_impl::static_graph::mmap::graph_mmap::StaticGraphMmapAux;
use graph_impl::static_graph::node::StaticNode;
use graph_impl::static_graph::static_edge_iter::StaticEdgeIndexIter;
use graph_impl::static_graph::{EdgeVec, EdgeVecTrait};
use graph_impl::{Edge, GraphImpl};
use io::mmap::dump;
use io::serde::{Deserialize, Serialize, Serializer};
use map::SetMap;

pub type TypedUnStaticGraph<Id, NL, EL = NL, L = Id> = TypedStaticGraph<Id, NL, EL, Undirected, L>;
pub type TypedDiStaticGraph<Id, NL, EL = NL, L = Id> = TypedStaticGraph<Id, NL, EL, Directed, L>;
pub type StaticGraph<NL, EL, Ty = DefaultTy, L = DefaultId> =
    TypedStaticGraph<DefaultId, NL, EL, Ty, L>;
pub type UnStaticGraph<NL, EL = NL, L = DefaultId> = StaticGraph<NL, EL, Undirected, L>;
pub type DiStaticGraph<NL, EL = NL, L = DefaultId> = StaticGraph<NL, EL, Directed, L>;

/// `StaticGraph` is a memory-compact graph data structure.
/// The labels of both nodes and edges, if exist, are encoded as `Integer`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TypedStaticGraph<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType = Id>
{
    num_nodes: usize,
    num_edges: usize,
    edge_vec: EdgeVec<Id, L>,
    in_edge_vec: Option<EdgeVec<Id, L>>,
    // Maintain the node's labels, whose index is aligned with `offsets`.
    labels: Option<Vec<L>>,
    // A marker of thr graph type, namely, directed or undirected.
    graph_type: PhantomData<Ty>,
    // A map of node labels.
    node_label_map: SetMap<NL>,
    // A map of edge labels.
    edge_label_map: SetMap<EL>,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> PartialEq
    for TypedStaticGraph<Id, NL, EL, Ty, L>
{
    fn eq(&self, other: &TypedStaticGraph<Id, NL, EL, Ty, L>) -> bool {
        if !self.node_count() == other.node_count() || !self.edge_count() == other.edge_count() {
            return false;
        }

        for n in self.node_indices() {
            if !other.has_node(n) || self.get_node_label(n) != other.get_node_label(n) {
                return false;
            }
        }

        for (s, d) in self.edge_indices() {
            if !other.has_edge(s, d) || self.get_edge_label(s, d) != other.get_edge_label(s, d) {
                return false;
            }
        }

        true
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> Eq
    for TypedStaticGraph<Id, NL, EL, Ty, L>
{}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> Hash
    for TypedStaticGraph<Id, NL, EL, Ty, L>
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        {
            let nodes = self.node_indices().sorted();
            nodes.hash(state);

            let node_labels = nodes
                .into_iter()
                .map(|n| self.get_node_label(n))
                .collect_vec();
            node_labels.hash(state);
        }
        {
            let edges = self.edge_indices().sorted();
            edges.hash(state);
            let edge_labels = edges
                .into_iter()
                .map(|(s, d)| self.get_edge_label(s, d))
                .collect_vec();
            edge_labels.hash(state);
        }
    }
}
impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> Serialize
    for TypedStaticGraph<Id, NL, EL, Ty, L>
where
    Id: serde::Serialize,
    NL: serde::Serialize,
    EL: serde::Serialize,
    L: serde::Serialize,
{}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> Deserialize
    for TypedStaticGraph<Id, NL, EL, Ty, L>
where
    Id: for<'de> serde::Deserialize<'de>,
    NL: for<'de> serde::Deserialize<'de>,
    EL: for<'de> serde::Deserialize<'de>,
    L: for<'de> serde::Deserialize<'de>,
{}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    TypedStaticGraph<Id, NL, EL, Ty, L>
{
    pub fn empty() -> Self {
        Self::new(EdgeVec::default(), None, None, None)
    }

    pub fn new(
        edges: EdgeVec<Id, L>,
        in_edges: Option<EdgeVec<Id, L>>,
        num_nodes: Option<usize>,
        num_edges: Option<usize>,
    ) -> Self {
        if Ty::is_directed() {
            if in_edges.is_none() {
                panic!("In edges should be provided for directed graph.");
            }

            let num_of_in_edges = in_edges.as_ref().unwrap().num_edges();
            let num_of_out_edges = edges.num_edges();
            if num_of_in_edges != num_of_out_edges {
                debug!(
                    "{} out edges but {} in edges.",
                    num_of_out_edges, num_of_in_edges
                );
            }
        }

        let num_nodes = if let Some(num) = num_nodes {
            if num != edges.num_nodes() {
                debug!(
                    "number of nodes ({}) does not match the length of edge vector ({})",
                    num,
                    edges.num_nodes()
                );
            }
            num
        } else {
            edges.num_nodes()
        };

        let num_edges = if let Some(num) = num_edges {
            num
        } else {
            if Ty::is_directed() {
                edges.num_edges()
            } else {
                edges.num_edges() >> 1
            }
        };

        TypedStaticGraph {
            num_nodes,
            num_edges,
            edge_vec: edges,
            in_edge_vec: in_edges,
            labels: None,
            node_label_map: SetMap::<NL>::new(),
            edge_label_map: SetMap::<EL>::new(),
            graph_type: PhantomData,
        }
    }

    pub fn with_labels(
        edges: EdgeVec<Id, L>,
        in_edges: Option<EdgeVec<Id, L>>,
        labels: Vec<L>,
        node_label_map: SetMap<NL>,
        edge_label_map: SetMap<EL>,
        num_nodes: Option<usize>,
        num_edges: Option<usize>,
    ) -> Self {
        if Ty::is_directed() {
            if in_edges.is_none() {
                panic!("In edges should be provided for directed graph.");
            }

            let num_of_in_edges = in_edges.as_ref().unwrap().num_edges();
            let num_of_out_edges = edges.num_edges();
            if num_of_in_edges != num_of_out_edges {
                debug!(
                    "{} out edges but {} in edges.",
                    num_of_out_edges, num_of_in_edges
                );
            }
        }

        let num_nodes = if let Some(num) = num_nodes {
            if num != edges.num_nodes() {
                debug!(
                    "Number of nodes ({}) does not match the length of edge vector ({})",
                    num,
                    edges.num_nodes()
                );
            }
            num
        } else {
            edges.num_nodes()
        };

        let num_edges = if let Some(num) = num_edges {
            num
        } else {
            if Ty::is_directed() {
                edges.num_edges()
            } else {
                edges.num_edges() >> 1
            }
        };

        if num_nodes != labels.len() {
            debug!("{} nodes, but {} labels", num_nodes, labels.len());
        }

        TypedStaticGraph {
            num_nodes,
            num_edges,
            edge_vec: edges,
            in_edge_vec: in_edges,
            labels: Some(labels),
            node_label_map,
            edge_label_map,
            graph_type: PhantomData,
        }
    }

    pub fn from_raw(
        num_nodes: usize,
        num_edges: usize,
        edge_vec: EdgeVec<Id, L>,
        in_edge_vec: Option<EdgeVec<Id, L>>,
        labels: Option<Vec<L>>,
        node_label_map: SetMap<NL>,
        edge_label_map: SetMap<EL>,
    ) -> Self {
        if num_nodes != edge_vec.num_nodes() {
            debug!(
                "Number of nodes ({}) does not match the length of edge vector ({})",
                num_nodes,
                edge_vec.num_nodes()
            );
        }

        if Ty::is_directed() {
            if Ty::is_directed() {
                if in_edge_vec.is_none() {
                    panic!("In edges should be provided for directed graph.");
                }

                let num_of_in_edges = in_edge_vec.as_ref().unwrap().num_edges();
                let num_of_out_edges = edge_vec.num_edges();
                if num_of_in_edges != num_of_out_edges {
                    debug!(
                        "{} out edges but {} in edges.",
                        num_of_out_edges, num_of_in_edges
                    );
                }
            }

            if num_edges != edge_vec.num_edges() {
                debug!(
                    "Directed: num_edges {}, edge_vec {} edges",
                    num_edges,
                    edge_vec.num_edges()
                );
            }
        } else if num_edges != edge_vec.num_edges() >> 1 {
            debug!(
                "Undirected: num_edges {}, edge_vec {} edges",
                num_edges,
                edge_vec.num_edges()
            );
        }

        if labels.is_some() {
            let num_of_labels = labels.as_ref().unwrap().len();
            if num_nodes != num_of_labels {
                debug!(
                    "There are {} nodes, but {} labels",
                    num_nodes, num_of_labels
                );
            }
        }

        TypedStaticGraph {
            num_nodes,
            num_edges,
            edge_vec,
            in_edge_vec,
            labels,
            node_label_map,
            edge_label_map,
            graph_type: PhantomData,
        }
    }

    #[inline]
    pub fn get_edge_vec(&self) -> &EdgeVec<Id, L> {
        &self.edge_vec
    }

    #[inline]
    pub fn get_in_edge_vec(&self) -> &Option<EdgeVec<Id, L>> {
        &self.in_edge_vec
    }

    #[inline]
    pub fn get_labels(&self) -> &Option<Vec<L>> {
        &self.labels
    }

    #[inline]
    pub fn get_node_label_map(&self) -> &SetMap<NL> {
        &self.node_label_map
    }

    #[inline]
    pub fn get_edge_label_map(&self) -> &SetMap<EL> {
        &self.edge_label_map
    }

    #[inline]
    pub fn get_edge_vec_mut(&mut self) -> &mut EdgeVec<Id, L> {
        &mut self.edge_vec
    }

    #[inline]
    pub fn get_in_edge_vec_mut(&mut self) -> &mut Option<EdgeVec<Id, L>> {
        &mut self.in_edge_vec
    }

    #[inline]
    pub fn get_labels_mut(&mut self) -> &mut Option<Vec<L>> {
        &mut self.labels
    }

    #[inline]
    pub fn get_node_label_map_mut(&mut self) -> &mut SetMap<NL> {
        &mut self.node_label_map
    }

    #[inline]
    pub fn get_edge_label_map_mut(&mut self) -> &mut SetMap<EL> {
        &mut self.edge_label_map
    }

    #[inline]
    pub fn remove_node_labels(&mut self) {
        self.labels = None;
        self.node_label_map = SetMap::new();
    }

    #[inline]
    pub fn remove_edge_labels(&mut self) {
        self.edge_vec.remove_labels();
        self.edge_label_map = SetMap::new();
        if let Some(ref mut e) = self.in_edge_vec.as_mut() {
            e.remove_labels()
        }
    }

    #[inline]
    pub fn remove_labels(&mut self) {
        self.remove_node_labels();
        self.remove_edge_labels();
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.edge_vec.shrink_to_fit();
        if let Some(ref mut in_edge_vec) = self.in_edge_vec {
            in_edge_vec.shrink_to_fit();
        }
        if let Some(ref mut labels) = self.labels {
            labels.shrink_to_fit();
        }
    }
    #[inline]
    pub fn find_edge_index(&self, start: Id, target: Id) -> Option<usize> {
        self.edge_vec.find_edge_index(start, target)
    }
}

impl<Id: IdType + Copy, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    TypedStaticGraph<Id, NL, EL, Ty, L>
where
    NL: serde::Serialize + Clone,
    EL: serde::Serialize + Clone,
{
    pub fn dump_mmap(&self, prefix: &str) -> Result<()> {
        let edges_prefix = format!("{}_OUT", prefix);
        let in_edges_prefix = format!("{}_IN", prefix);
        let label_file = format!("{}.labels", prefix);

        let aux_map_file = format!("{}_aux.bin", prefix);

        self.edge_vec.dump_mmap(&edges_prefix)?;
        if let Some(ref in_edges) = self.in_edge_vec {
            in_edges.dump_mmap(&in_edges_prefix)?;
        }

        if let Some(ref labels) = self.labels {
            unsafe {
                dump(labels, ::std::fs::File::create(label_file)?)?;
            }
        }

        let aux_file = StaticGraphMmapAux::new(
            self.num_nodes,
            self.num_edges,
            self.node_label_map.clone(),
            self.edge_label_map.clone(),
        );

        Serializer::export(&aux_file, &aux_map_file)?;

        Ok(())
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> GraphTrait<Id, L>
    for TypedStaticGraph<Id, NL, EL, Ty, L>
{
    #[inline]
    fn get_node(&self, id: Id) -> NodeType<Id, L> {
        if !self.has_node(id) {
            return NodeType::None;
        }

        match self.labels {
            Some(ref labels) => NodeType::StaticNode(StaticNode::new_static(id, labels[id.id()])),
            None => NodeType::StaticNode(StaticNode::new(id, None)),
        }
    }

    #[inline]
    fn get_edge(&self, start: Id, target: Id) -> EdgeType<Id, L> {
        if !self.has_edge(start, target) {
            return EdgeType::None;
        }

        let _label = self.edge_vec.find_edge_label_id(start, target);
        match _label {
            Some(label) => EdgeType::Edge(Edge::new_static(start, target, *label)),
            None => EdgeType::Edge(Edge::new(start, target, None)),
        }
    }

    #[inline]
    fn has_node(&self, id: Id) -> bool {
        id.id() < self.num_nodes
    }

    #[inline]
    fn has_edge(&self, start: Id, target: Id) -> bool {
        self.edge_vec.has_edge(start, target)
    }

    #[inline]
    fn node_count(&self) -> usize {
        self.num_nodes
    }

    #[inline]
    fn edge_count(&self) -> usize {
        self.num_edges
    }

    #[inline(always)]
    fn is_directed(&self) -> bool {
        Ty::is_directed()
    }

    #[inline]
    fn node_indices(&self) -> Iter<Id> {
        Iter::new(Box::new((0..self.num_nodes).map(Id::new)))
    }

    #[inline]
    fn edge_indices(&self) -> Iter<(Id, Id)> {
        Iter::new(Box::new(StaticEdgeIndexIter::new(
            Box::new(&self.edge_vec),
            self.is_directed(),
        )))
    }

    #[inline]
    fn nodes(&self) -> Iter<NodeType<Id, L>> {
        match self.labels {
            None => {
                let node_iter = self
                    .node_indices()
                    .map(|i| NodeType::StaticNode(StaticNode::new(i, None)));

                Iter::new(Box::new(node_iter))
            }
            Some(ref labels) => {
                let node_iter = self
                    .node_indices()
                    .zip(labels.iter())
                    .map(|n| NodeType::StaticNode(StaticNode::new_static(n.0, *n.1)));

                Iter::new(Box::new(node_iter))
            }
        }
    }

    #[inline]
    fn edges(&self) -> Iter<EdgeType<Id, L>> {
        let labels = self.edge_vec.get_labels();
        if labels.is_empty() {
            Iter::new(Box::new(
                self.edge_indices()
                    .map(|i| EdgeType::Edge(Edge::new(i.0, i.1, None))),
            ))
        } else {
            Iter::new(Box::new(self.edge_indices().zip(labels.iter()).map(|e| {
                EdgeType::Edge(Edge::new_static((e.0).0, (e.0).1, *e.1))
            })))
        }
    }

    #[inline]
    fn degree(&self, id: Id) -> usize {
        self.edge_vec.degree(id)
    }

    #[inline]
    fn total_degree(&self, id: Id) -> usize {
        let mut degree = self.degree(id);
        if self.is_directed() {
            degree += self.in_edge_vec.as_ref().unwrap().neighbors(id).len()
        }

        degree
    }

    #[inline]
    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        let neighbors = self.edge_vec.neighbors(id);

        Iter::new(Box::new(neighbors.iter().map(|x| *x)))
    }

    #[inline]
    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        self.edge_vec.neighbors(id).into()
    }

    #[inline]
    fn max_seen_id(&self) -> Option<Id> {
        Some(Id::new(self.node_count() - 1))
    }

    #[inline]
    fn implementation(&self) -> GraphImpl {
        GraphImpl::StaticGraph
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    GraphLabelTrait<Id, NL, EL, L> for TypedStaticGraph<Id, NL, EL, Ty, L>
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

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> UnGraphTrait<Id, L>
    for TypedUnStaticGraph<Id, NL, EL, L>
{}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> DiGraphTrait<Id, L>
    for TypedDiStaticGraph<Id, NL, EL, L>
{
    #[inline]
    fn in_degree(&self, id: Id) -> usize {
        self.in_neighbors(id).len()
    }

    #[inline]
    fn in_neighbors_iter(&self, id: Id) -> Iter<Id> {
        let in_neighbors = self.in_edge_vec.as_ref().unwrap().neighbors(id);

        Iter::new(Box::new(in_neighbors.iter().map(|x| *x)))
    }

    #[inline]
    fn in_neighbors(&self, id: Id) -> Cow<[Id]> {
        self.in_edge_vec.as_ref().unwrap().neighbors(id).into()
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GeneralGraph<Id, NL, EL, L>
    for TypedUnStaticGraph<Id, NL, EL, L>
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
    fn as_general_graph(&self) -> &GeneralGraph<Id, NL, EL, L> {
        self
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GeneralGraph<Id, NL, EL, L>
    for TypedDiStaticGraph<Id, NL, EL, L>
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
    fn as_general_graph(&self) -> &GeneralGraph<Id, NL, EL, L> {
        self
    }

    #[inline(always)]
    fn as_digraph(&self) -> Option<&DiGraphTrait<Id, L>> {
        Some(self)
    }
}
