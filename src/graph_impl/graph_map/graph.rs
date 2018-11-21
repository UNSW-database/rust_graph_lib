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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::ops::Add;
use std::ops::Sub;

use fnv::{FnvBuildHasher, FnvHashMap};
use itertools::Itertools;
use serde;

use generic::{
    DefaultId, DefaultTy, DiGraphTrait, Directed, EdgeType, GeneralGraph, GraphLabelTrait,
    GraphTrait, GraphType, IdType, Iter, MapTrait, MutGraphLabelTrait, MutGraphTrait, MutMapTrait,
    MutNodeMapTrait, MutNodeTrait, NodeMapTrait, NodeTrait, NodeType, UnGraphTrait, Undirected,
};
use graph_impl::graph_map::{Edge, NodeMap};
use graph_impl::{EdgeVec, Graph, TypedStaticGraph};
use io::serde::{Deserialize, Serialize};
use map::SetMap;
use algorithm::graph_union::GraphUnion;
use algorithm::graph_minus::GraphMinus;

pub type TypedDiGraphMap<Id, NL, EL = NL, L = DefaultId> = TypedGraphMap<Id, NL, EL, Directed, L>;
pub type TypedUnGraphMap<Id, NL, EL = NL, L = DefaultId> = TypedGraphMap<Id, NL, EL, Undirected, L>;
pub type GraphMap<NL, EL, Ty = DefaultTy, L = DefaultId> = TypedGraphMap<DefaultId, NL, EL, Ty, L>;

/// Shortcut of creating a new directed graph where `L` is the data type of labels.
/// # Example
/// ```
/// use rust_graph::DiGraphMap;
/// let  g = DiGraphMap::<&str>::new();
/// ```
pub type DiGraphMap<NL, EL = NL, L = DefaultId> = GraphMap<NL, EL, Directed, L>;

/// Shortcut of creating a new undirected graph where `L` is the data type of labels.
/// # Example
/// ```
/// use rust_graph::UnGraphMap;
/// let g = UnGraphMap::<&str>::new();
/// ```
pub type UnGraphMap<NL, EL = NL, L = DefaultId> = GraphMap<NL, EL, Undirected, L>;

/// A graph data structure that nodes and edges are stored in hash maps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedGraphMap<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType = Id> {
    /// A map <node_id:node>.
    node_map: FnvHashMap<Id, NodeMap<Id, L>>,
    /// Num of edges.
    num_of_edges: usize,
    /// A map of node labels.
    node_label_map: SetMap<NL>,
    /// A map of edge labels.
    edge_label_map: SetMap<EL>,
    /// The maximum id has been seen until now.
    max_id: Option<Id>,
    /// A marker of thr graph type, namely, directed or undirected.
    graph_type: PhantomData<Ty>,
}

impl<Id: IdType + 'static, NL: Hash + Eq + Clone + 'static, EL: Hash + Eq + Clone + 'static, L: IdType + 'static> Add
for TypedGraphMap<Id, NL, EL, Directed, L>
{
    type Output = Box<GeneralGraph<Id, NL, EL, L>>;

    fn add(self, other: TypedGraphMap<Id, NL, EL, Directed, L>) -> Box<GeneralGraph<Id, NL, EL, L>> {
        let gu = GraphUnion::new(&self, &other);
        let result = gu.get_result_graph();
        result
    }
}

impl<Id: IdType + 'static, NL: Hash + Eq + Clone + 'static, EL: Hash + Eq + Clone + 'static, L: IdType + 'static> Add
for TypedGraphMap<Id, NL, EL, Undirected, L>
{
    type Output = Box<GeneralGraph<Id, NL, EL, L>>;

    fn add(self, other: TypedGraphMap<Id, NL, EL, Undirected, L>) -> Box<GeneralGraph<Id, NL, EL, L>> {
        let gu = GraphUnion::new(&self, &other);
        let result = gu.get_result_graph();
        result
    }
}

impl<Id: IdType + 'static, NL: Hash + Eq + Clone + 'static, EL: Hash + Eq + Clone + 'static, L: IdType + 'static> Sub
for TypedGraphMap<Id, NL, EL, Directed, L>
{
    type Output = Box<GeneralGraph<Id, NL, EL, L>>;

    fn sub(self, other: TypedGraphMap<Id, NL, EL, Directed, L>) -> Box<GeneralGraph<Id, NL, EL, L>> {
        let gu = GraphMinus::new(&self, &other);
        let result = gu.get_result_graph();
        result
    }
}

impl<Id: IdType + 'static, NL: Hash + Eq + Clone + 'static, EL: Hash + Eq + Clone + 'static, L: IdType + 'static> Sub
for TypedGraphMap<Id, NL, EL, Undirected, L>
{
    type Output = Box<GeneralGraph<Id, NL, EL, L>>;

    fn sub(self, other: TypedGraphMap<Id, NL, EL, Undirected, L>) -> Box<GeneralGraph<Id, NL, EL, L>> {
        let gu = GraphMinus::new(&self, &other);
        let result = gu.get_result_graph();
        result
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> Serialize
    for TypedGraphMap<Id, NL, EL, Ty, L>
where
    Id: serde::Serialize,
    NL: serde::Serialize,
    EL: serde::Serialize,
    L: serde::Serialize,
{}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> Deserialize
    for TypedGraphMap<Id, NL, EL, Ty, L>
where
    Id: for<'de> serde::Deserialize<'de>,
    NL: for<'de> serde::Deserialize<'de>,
    EL: for<'de> serde::Deserialize<'de>,
    L: for<'de> serde::Deserialize<'de>,
{}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    TypedGraphMap<Id, NL, EL, Ty, L>
{
    /// Constructs a new graph.
    pub fn new() -> Self {
        TypedGraphMap {
            node_map: FnvHashMap::<Id, NodeMap<Id, L>>::default(),
            num_of_edges: 0,
            node_label_map: SetMap::new(),
            edge_label_map: SetMap::new(),
            max_id: None,
            graph_type: PhantomData,
        }
    }

    pub fn with_capacity(nodes: usize, node_labels: usize, edge_labels: usize) -> Self {
        TypedGraphMap {
            node_map: HashMap::with_capacity_and_hasher(nodes, FnvBuildHasher::default()),
            num_of_edges: 0,
            node_label_map: SetMap::with_capacity(node_labels),
            edge_label_map: SetMap::with_capacity(edge_labels),
            max_id: None,
            graph_type: PhantomData,
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.node_map.shrink_to_fit();
    }

    /// Constructs a new graph using existing label-id mapping.
    /// # Example
    /// ```
    /// use rust_graph::prelude::*;
    /// use rust_graph::UnGraphMap;
    ///
    /// let mut g = UnGraphMap::<&str>::new();
    /// g.add_node(0, Some("a"));
    /// g.add_node(1, Some("b"));
    /// g.add_edge(0, 1, None);
    ///
    /// let mut p = UnGraphMap::with_label_map(g.get_node_label_map().clone(),
    ///                                                g.get_edge_label_map().clone());
    /// p.add_node(1, Some("b"));
    /// p.add_node(0, Some("a"));
    /// p.add_edge(0, 1, None);
    ///
    /// assert_eq!(g.get_node(0).get_label_id(), p.get_node(0).get_label_id());
    /// assert_eq!(g.get_node(1).get_label_id(), p.get_node(1).get_label_id());
    ///
    /// ```
    pub fn with_label_map(node_label_map: SetMap<NL>, edge_label_map: SetMap<EL>) -> Self {
        TypedGraphMap {
            node_map: FnvHashMap::default(),
            num_of_edges: 0,
            node_label_map,
            edge_label_map,
            max_id: None,
            graph_type: PhantomData,
        }
    }

    pub fn from_edges<I: IntoIterator<Item = (Id, Id)>>(edges: I) -> Self {
        let mut g = TypedGraphMap::new();
        for (src, dst) in edges {
            g.add_node(src, None);
            g.add_node(dst, None);
            g.add_edge(src, dst, None);
        }

        g
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> Default
    for TypedGraphMap<Id, NL, EL, Ty, L>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> MutGraphTrait<Id, NL, EL>
    for TypedGraphMap<Id, NL, EL, Ty, L>
{
    type N = NodeMap<Id, L>;
    type E = Option<L>;

    /// Add a node with `id` and `label`. If the node of the `id` already presents,
    /// replace the node's label with the new `label` and return `false`.
    /// Otherwise, add the node and return `true`.
    fn add_node(&mut self, id: Id, label: Option<NL>) -> bool {
        let label_id = label.map(|x| L::new(self.node_label_map.add_item(x)));

        if self.has_node(id) {
            // Node already exist, updating its label.
            self.get_node_mut(id).unwrap().set_label_id(label_id);

            return false;
        }

        let new_node = NodeMap::new(id, label_id);
        self.node_map.insert(id, new_node);
        match self.max_id {
            Some(i) => {
                if i < id {
                    self.max_id = Some(id)
                }
            }
            None => self.max_id = Some(id),
        }

        true
    }

    fn get_node_mut(&mut self, id: Id) -> Option<&mut Self::N> {
        self.node_map.get_mut(&id)
    }

    fn remove_node(&mut self, id: Id) -> Option<Self::N> {
        match self.node_map.remove(&id) {
            Some(node) => {
                if self.is_directed() {
                    for neighbor in node.neighbors_iter() {
                        self.get_node_mut(neighbor).unwrap().remove_in_edge(id);
                    }
                    for in_neighbor in node.in_neighbors_iter() {
                        self.get_node_mut(in_neighbor).unwrap().remove_edge(id);
                    }
                } else {
                    for neighbor in node.neighbors_iter() {
                        self.get_node_mut(neighbor).unwrap().remove_edge(id);
                    }
                }

                self.num_of_edges -= node.degree() + node.in_degree();

                Some(node)
            }
            None => None,
        }
    }

    /// Add the edge with given `start` and `target` vertices.
    /// If either end does not exist, add a new node with corresponding id
    /// and `None` label. If the edge already presents, return `false`,
    /// otherwise add the new edge and return `true`.
    fn add_edge(&mut self, start: Id, target: Id, label: Option<EL>) -> bool {
        if !self.has_node(start) {
            self.add_node(start, None);
        }
        if !self.has_node(target) {
            self.add_node(target, None);
        }

        let label_id = label.map(|x| L::new(self.edge_label_map.add_item(x)));

        let result = self.get_node_mut(start).unwrap().add_edge(target, label_id);

        if self.is_directed() {
            self.get_node_mut(target).unwrap().add_in_edge(start);
        } else if start != target {
            self.get_node_mut(target).unwrap().add_edge(start, label_id);
        }

        self.num_of_edges += 1;

        result
    }

    fn get_edge_mut(&mut self, start: Id, target: Id) -> Option<&mut Self::E> {
        if !self.has_edge(start, target) {
            return None;
        }

        Some(
            self.get_node_mut(start)
                .unwrap()
                .get_neighbor_mut(target)
                .unwrap(),
        )
    }

    fn remove_edge(&mut self, start: Id, target: Id) -> Option<Self::E> {
        if !self.has_edge(start, target) {
            return None;
        }

        let result = self.get_node_mut(start).unwrap().remove_edge(target);

        if self.is_directed() {
            self.get_node_mut(target).unwrap().remove_in_edge(start);
        } else {
            self.get_node_mut(target).unwrap().remove_edge(start);
        }

        self.num_of_edges -= 1;

        result
    }

    fn nodes_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::N> {
        Iter::new(Box::new(self.node_map.values_mut()))
    }

    fn edges_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::E> {
        if self.is_directed() {
            Iter::new(Box::new(
                self.nodes_mut().flat_map(|n| n.neighbors_iter_mut()),
            ))
        } else {
            Iter::new(Box::new(
                self.nodes_mut()
                    .flat_map(|n| n.non_less_neighbors_iter_mut()),
            ))
        }
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> GraphTrait<Id, L>
    for TypedGraphMap<Id, NL, EL, Ty, L>
{
    #[inline]
    fn get_node(&self, id: Id) -> NodeType<Id, L> {
        match self.node_map.get(&id) {
            Some(node) => NodeType::NodeMap(node),
            None => NodeType::None,
        }
    }

    #[inline]
    fn get_edge(&self, start: Id, target: Id) -> EdgeType<Id, L> {
        if !self.has_edge(start, target) {
            return EdgeType::None;
        }

        let label_id = self
            .get_node(start)
            .unwrap_nodemap()
            .get_neighbor(target)
            .unwrap();

        EdgeType::Edge(Edge::new(start, target, label_id))
    }

    #[inline]
    fn has_node(&self, id: Id) -> bool {
        self.node_map.contains_key(&id)
    }

    #[inline]
    fn has_edge(&self, start: Id, target: Id) -> bool {
        match self.get_node(start) {
            NodeType::NodeMap(node) => node.has_neighbor(target),
            _ => false,
        }
    }

    #[inline]
    fn node_count(&self) -> usize {
        self.node_map.len()
    }

    #[inline]
    fn edge_count(&self) -> usize {
        self.num_of_edges
    }

    #[inline(always)]
    fn is_directed(&self) -> bool {
        Ty::is_directed()
    }

    #[inline]
    fn node_indices(&self) -> Iter<Id> {
        Iter::new(Box::new(self.node_map.keys().map(|x| *x)))
    }

    #[inline]
    fn edge_indices(&self) -> Iter<(Id, Id)> {
        if self.is_directed() {
            Iter::new(Box::new(
                self.nodes()
                    .map(|n| n.unwrap_nodemap())
                    .flat_map(|n| n.neighbors_iter().map(move |i| (n.get_id(), i))),
            ))
        } else {
            Iter::new(Box::new(self.nodes().map(|n| n.unwrap_nodemap()).flat_map(
                |n| n.non_less_neighbors_iter().map(move |i| (n.get_id(), i)),
            )))
        }
    }

    #[inline]
    fn nodes(&self) -> Iter<NodeType<Id, L>> {
        Iter::new(Box::new(
            self.node_map.values().map(|node| NodeType::NodeMap(node)),
        ))
    }

    #[inline]
    fn edges(&self) -> Iter<EdgeType<Id, L>> {
        if self.is_directed() {
            Iter::new(Box::new(
                self.nodes()
                    .map(|n| n.unwrap_nodemap())
                    .flat_map(|n| n.neighbors_iter_full())
                    .map(EdgeType::Edge),
            ))
        } else {
            Iter::new(Box::new(
                self.nodes()
                    .map(|n| n.unwrap_nodemap())
                    .flat_map(|n| n.non_less_neighbors_iter_full())
                    .map(EdgeType::Edge),
            ))
        }
    }

    #[inline]
    fn degree(&self, id: Id) -> usize {
        match self.get_node(id) {
            NodeType::NodeMap(node) => node.degree(),
            NodeType::None => panic!("Node {:?} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    #[inline]
    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        match self.get_node(id) {
            NodeType::NodeMap(node) => node.neighbors_iter(),
            NodeType::None => panic!("Node {:?} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    #[inline]
    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        match self.get_node(id) {
            NodeType::NodeMap(node) => node.neighbors().into(),
            NodeType::None => panic!("Node {:?} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    #[inline]
    fn max_seen_id(&self) -> Option<Id> {
        self.max_id
    }

    #[inline(always)]
    fn implementation(&self) -> Graph {
        Graph::GraphMap
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    GraphLabelTrait<Id, NL, EL, L> for TypedGraphMap<Id, NL, EL, Ty, L>
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

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    MutGraphLabelTrait<Id, NL, EL, L> for TypedGraphMap<Id, NL, EL, Ty, L>
{
    fn update_node_label(&mut self, node_id: Id, label: Option<NL>) -> bool {
        if !self.has_node(node_id) {
            return false;
        }

        self.add_node(node_id, label);

        true
    }

    fn update_edge_label(&mut self, start: Id, target: Id, label: Option<EL>) -> bool {
        if !self.has_edge(start, target) {
            return false;
        }

        self.add_edge(start, target, label);

        true
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> UnGraphTrait<Id, L>
    for TypedUnGraphMap<Id, NL, EL, L>
{}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> DiGraphTrait<Id, L>
    for TypedDiGraphMap<Id, NL, EL, L>
{
    #[inline]
    fn in_degree(&self, id: Id) -> usize {
        match self.get_node(id) {
            NodeType::NodeMap(node) => node.in_degree(),
            NodeType::None => panic!("Node {:?} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    #[inline]
    fn in_neighbors_iter(&self, id: Id) -> Iter<Id> {
        match self.get_node(id) {
            NodeType::NodeMap(ref node) => node.in_neighbors_iter(),
            NodeType::None => panic!("Node {:?} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    #[inline]
    fn in_neighbors(&self, id: Id) -> Cow<[Id]> {
        match self.get_node(id) {
            NodeType::NodeMap(ref node) => node.in_neighbors().into(),
            NodeType::None => panic!("Node {:?} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GeneralGraph<Id, NL, EL, L>
    for TypedUnGraphMap<Id, NL, EL, L>
{
    #[inline(always)]
    fn as_graph(&self) -> &GraphTrait<Id, L> {
        self
    }

    #[inline(always)]
    fn as_labeled_graph(&self) -> &GraphLabelTrait<Id, NL, EL, L> {
        self
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GeneralGraph<Id, NL, EL, L>
    for TypedDiGraphMap<Id, NL, EL, L>
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
        Some(self)
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    TypedGraphMap<Id, NL, EL, Ty, L>
{
    pub fn reorder_id(
        self,
        reorder_node_id: bool,
        reorder_node_label: bool,
        reorder_edge_label: bool,
    ) -> ReorderResult<Id, NL, EL, Ty, L> {
        let node_id_map: Option<SetMap<_>> = if reorder_node_id {
            Some(
                self.nodes()
                    .map(|n| n.unwrap_nodemap())
                    .map(|n| (n.get_id(), n.degree() + n.in_degree()))
                    .sorted_by_key(|&(_, d)| d)
                    .into_iter()
                    .map(|(n, _)| n)
                    .collect(),
            )
        } else {
            None
        };

        let node_label_map: Option<SetMap<_>> = if reorder_node_label {
            Some(
                self.get_node_label_id_counter()
                    .most_common()
                    .into_iter()
                    .rev()
                    .skip_while(|(_, f)| *f == 0)
                    .map(|(n, _)| n)
                    .collect(),
            )
        } else {
            None
        };

        let edge_label_map: Option<SetMap<_>> = if reorder_edge_label {
            Some(
                self.get_edge_label_id_counter()
                    .most_common()
                    .into_iter()
                    .rev()
                    .skip_while(|(_, f)| *f == 0)
                    .map(|(n, _)| n)
                    .collect(),
            )
        } else {
            None
        };

        let graph = Some(self.reorder_id_with(&node_id_map, &node_label_map, &edge_label_map));

        ReorderResult { node_id_map, graph }
    }

    pub fn reorder_id_with(
        self,
        node_id_map: &Option<impl MapTrait<Id>>,
        node_label_map: &Option<impl MapTrait<L>>,
        edge_label_map: &Option<impl MapTrait<L>>,
    ) -> Self {
        if node_id_map.is_none() && node_label_map.is_none() && edge_label_map.is_none() {
            return self;
        }

        let num_of_edges = self.edge_count();

        let mut new_node_map = FnvHashMap::default();

        for (_, node) in self.node_map {
            let new_node_id = if let Some(ref map) = node_id_map {
                Id::new(map.find_index(&node.id).unwrap())
            } else {
                node.id
            };

            let new_node_label = if let Some(ref map) = node_label_map {
                node.label.map(|i| L::new(map.find_index(&i).unwrap()))
            } else {
                node.label
            };

            let new_neighbors = if node_id_map.is_some() || edge_label_map.is_some() {
                node.neighbors
                    .into_iter()
                    .map(|(n, l)| {
                        let new_n = if let Some(ref map) = node_id_map {
                            Id::new(map.find_index(&n).unwrap())
                        } else {
                            n
                        };

                        let new_l = l.map(|i| {
                            if let Some(ref map) = edge_label_map {
                                L::new(map.find_index(&i).unwrap())
                            } else {
                                i
                            }
                        });

                        (new_n, new_l)
                    }).collect()
            } else {
                node.neighbors
            };

            let new_in_neighbors = if let Some(ref map) = node_id_map {
                node.in_neighbors
                    .into_iter()
                    .map(|n| Id::new(map.find_index(&n).unwrap()))
                    .collect()
            } else {
                node.in_neighbors
            };

            let new_node = NodeMap {
                id: new_node_id,
                label: new_node_label,
                neighbors: new_neighbors,
                in_neighbors: new_in_neighbors,
            };

            new_node_map.insert(new_node_id, new_node);
        }

        new_node_map.shrink_to_fit();

        let new_node_label_map = if let Some(ref map) = node_label_map {
            reorder_label_map(map, self.node_label_map)
        } else {
            self.node_label_map
        };

        let new_edge_label_map = if let Some(ref map) = edge_label_map {
            reorder_label_map(map, self.edge_label_map)
        } else {
            self.edge_label_map
        };

        let new_max_id = new_node_map.keys().max().map(|i| *i);

        TypedGraphMap {
            node_map: new_node_map,
            num_of_edges,
            edge_label_map: new_edge_label_map,
            node_label_map: new_node_label_map,
            max_id: new_max_id,
            graph_type: PhantomData,
        }
    }

    pub fn into_static(mut self) -> TypedStaticGraph<Id, NL, EL, Ty, L> {
        let num_of_nodes = self.node_count();
        let num_of_edges = self.edge_count();

        let mut offset = 0usize;
        let mut offset_vec = Vec::new();
        let mut edge_vec = Vec::new();
        let mut edge_labels = if self.has_edge_labels() {
            Some(Vec::new())
        } else {
            None
        };

        let mut in_offset = None;
        let mut in_offset_vec = None;
        let mut in_edge_vec = None;

        if self.is_directed() {
            in_offset = Some(0usize);
            in_offset_vec = Some(Vec::new());
            in_edge_vec = Some(Vec::new());
        }

        let mut node_labels = if self.has_node_labels() {
            Some(Vec::new())
        } else {
            None
        };

        let mut nid = Id::new(0);
        let max_nid = self.node_indices().max().unwrap();

        offset_vec.push(offset);

        if let (Some(_in_offset), Some(_in_offset_vec)) = (in_offset, in_offset_vec.as_mut()) {
            _in_offset_vec.push(_in_offset);
        }

        while nid <= max_nid {
            if let Some(mut node) = self.node_map.remove(&nid) {
                let neighbors = mem::replace(&mut node.neighbors, BTreeMap::new());
                let num_of_neighbors = neighbors.len();

                if let Some(ref mut _edge_labels) = edge_labels {
                    for (n, l) in neighbors {
                        edge_vec.push(n);
                        _edge_labels.push(match l {
                            Some(_l) => _l,
                            None => L::max_value(),
                        });
                    }
                } else {
                    edge_vec.extend(neighbors.keys());
                }

                offset += num_of_neighbors;

                if let (Some(_in_offset), Some(_in_edge_vec)) =
                    (in_offset.as_mut(), in_edge_vec.as_mut())
                {
                    let in_neighbors = mem::replace(&mut node.in_neighbors, BTreeSet::new());

                    *_in_offset += in_neighbors.len();
                    _in_edge_vec.extend(in_neighbors);
                }

                if let Some(ref mut _node_labels) = node_labels {
                    match node.label {
                        Some(label) => _node_labels.push(label),
                        None => _node_labels.push(L::max_value()),
                    }
                }
            } else if let Some(ref mut _node_labels) = node_labels {
                _node_labels.push(L::max_value());
            }

            offset_vec.push(offset);

            if let (Some(_in_offset), Some(_in_offset_vec)) = (in_offset, in_offset_vec.as_mut()) {
                _in_offset_vec.push(_in_offset);
            }

            nid = nid.increment();

            //shrink the map to save memory
            self.shrink_to_fit();
        }

        let mut edge_vec = EdgeVec::from_raw(offset_vec, edge_vec, edge_labels);
        edge_vec.shrink_to_fit();

        let in_edge_vec =
            if let (Some(_in_offset_vec), Some(_in_edge_vec)) = (in_offset_vec, in_edge_vec) {
                let mut _edge = EdgeVec::new(_in_offset_vec, _in_edge_vec);
                _edge.shrink_to_fit();
                Some(_edge)
            } else {
                None
            };

        if let Some(ref mut _labels) = node_labels {
            _labels.shrink_to_fit();
        }

        let node_label_map = self.node_label_map;
        let edge_label_map = self.edge_label_map;

        TypedStaticGraph::from_raw(
            num_of_nodes,
            num_of_edges,
            edge_vec,
            in_edge_vec,
            node_labels,
            node_label_map,
            edge_label_map,
        )
    }
}

fn reorder_label_map<Id, L>(new_map: &impl MapTrait<Id>, old_map: impl MapTrait<L>) -> SetMap<L>
where
    Id: IdType,
    L: Hash + Eq,
{
    let mut old_map_vec: Vec<_> = old_map.items_vec().into_iter().map(|i| Some(i)).collect();
    let mut result = SetMap::new();

    for i in new_map.items() {
        let l = old_map_vec[i.id()].take().unwrap();
        result.add_item(l);
    }

    result
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReorderResult<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> {
    node_id_map: Option<SetMap<Id>>,
    graph: Option<TypedGraphMap<Id, NL, EL, Ty, L>>,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    ReorderResult<Id, NL, EL, Ty, L>
{
    #[inline]
    pub fn take_graph(&mut self) -> Option<TypedGraphMap<Id, NL, EL, Ty, L>> {
        self.graph.take()
    }

    #[inline]
    pub fn get_node_id_map(&self) -> Option<&SetMap<Id>> {
        self.node_id_map.as_ref()
    }

    #[inline]
    pub fn get_original_node_id(&self, id: Id) -> Id {
        match self.get_node_id_map() {
            Some(map) => *map.get_item(id.id()).unwrap(),
            None => id,
        }
    }

    #[inline]
    pub fn find_new_node_id(&self, id: Id) -> Id {
        match self.get_node_id_map() {
            Some(map) => Id::new(map.find_index(&id).unwrap()),
            None => id,
        }
    }
}
