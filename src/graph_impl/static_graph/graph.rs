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
#![feature(test)]

use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use itertools::Itertools;
use serde;

use generic::graph::LabelledGraphTrait;
use generic::{
    DefaultId, DefaultTy, DiGraphTrait, Directed, EdgeTrait, EdgeType, GeneralGraph,
    GraphLabelTrait, GraphTrait, GraphType, IdType, Iter, MapTrait, MutMapTrait, NodeTrait,
    NodeType, UnGraphTrait, Undirected,
};
use graph_impl::static_graph::node::StaticNode;
use graph_impl::static_graph::sorted_adj_vec::SortedAdjVec;
use graph_impl::static_graph::static_edge_iter::StaticEdgeIndexIter;
use graph_impl::static_graph::{EdgeVec, EdgeVecTrait};
use graph_impl::{Edge, GraphImpl};
use hashbrown::HashMap;
use io::serde::{Deserialize, Serialize};
use map::SetMap;
use std::cmp;
use std::ops::Add;
use test::Options;
use test::bench::iter;
use std::any::Any;

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
    sort_by_node: bool,

    // node Ids indexed by type and random access to node types.
    node_ids: Vec<Id>,
    // node_types[node_id] = node_label_id
    // the node_label_id has been shifted right and id 0 is prepared for no label item.
    node_types: Vec<usize>,
    node_type_offsets: Vec<usize>,

    fwd_adj_lists: Vec<Option<SortedAdjVec<Id>>>,
    bwd_adj_lists: Vec<Option<SortedAdjVec<Id>>>,

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

    //without node label and edge label
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
        } else if Ty::is_directed() {
            edges.num_edges()
        } else {
            edges.num_edges() >> 1
        };

        let mut g = TypedStaticGraph {
            num_nodes,
            num_edges,
            sort_by_node: false,
            node_ids: vec![],
            node_types: vec![],
            node_type_offsets: vec![],
            fwd_adj_lists: vec![],
            bwd_adj_lists: vec![],
            edge_vec: edges,
            in_edge_vec: in_edges,
            labels: None,
            node_label_map: SetMap::<NL>::new(),
            edge_label_map: SetMap::<EL>::new(),
            graph_type: PhantomData,
        };
        g.partition_nodes();
        g.partition_edges();
        g
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
        } else if Ty::is_directed() {
            edges.num_edges()
        } else {
            edges.num_edges() >> 1
        };

        if num_nodes != labels.len() {
            debug!("{} nodes, but {} labels", num_nodes, labels.len());
        }

        if edge_label_map.len() != 0 {}

        let mut g = TypedStaticGraph {
            num_nodes,
            num_edges,
            sort_by_node: false,
            node_ids: vec![],
            node_types: vec![],
            node_type_offsets: vec![],
            fwd_adj_lists: vec![],
            bwd_adj_lists: vec![],
            edge_vec: edges,
            in_edge_vec: in_edges,
            labels: Some(labels),
            node_label_map,
            edge_label_map,
            graph_type: PhantomData,
        };
        g.partition_nodes();
        g.partition_edges();
        g
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

        let mut g = TypedStaticGraph {
            num_nodes,
            num_edges,
            sort_by_node: false,
            node_ids: vec![],
            node_types: vec![],
            node_type_offsets: vec![],
            fwd_adj_lists: vec![],
            bwd_adj_lists: vec![],
            edge_vec,
            in_edge_vec,
            labels,
            node_label_map,
            edge_label_map,
            graph_type: PhantomData,
        };
        g.partition_nodes();
        g.partition_edges();
        g
    }

    pub fn is_sorted_by_node(&self) -> bool {
        self.sort_by_node
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

    // Partition nodes by type and generating node_ids && offsets for retrieving.
    fn partition_nodes(&mut self) {
        if 0 == self.num_of_node_labels() {
            let mut node_ids = vec![Id::new(0); self.num_nodes];
            for i in 0..self.num_nodes {
                node_ids[i] = Id::new(i);
            }
            self.node_ids = node_ids;
            self.node_types = vec![0; self.num_nodes];
            self.node_type_offsets = vec![0, self.num_nodes + 1];
            return;
        }
        let offsets = self.get_node_offsets();
        let num_nodes = offsets[offsets.len() - 1];

        let mut node_ids = vec![Id::new(0); num_nodes];
        let mut node_types = vec![0; num_nodes];
        let mut curr_idx_by_type = vec![0; offsets.len()];
        self.node_indices().for_each(|id| {
            let node_id = id.id();
            let node_label_id = self
                .get_node(id)
                .get_label_id()
                .map(|op| op.id() + 1)
                .unwrap_or(0);
            node_ids[offsets[node_label_id] + curr_idx_by_type[node_label_id]] = id;
            curr_idx_by_type[node_label_id] += 1;
            node_types[node_id] = node_label_id;
        });
        self.node_ids = node_ids;
        self.node_types = node_types;
        self.node_type_offsets = offsets;
    }

    // Partition edges by edge label or node label(if there did not exist edge labels in graph)
    fn partition_edges(&mut self) {
        self.sort_by_node = self.num_of_edge_labels() == 0 && self.num_of_node_labels() > 0;
        let (fwd_adj_meta_data, bwd_adj_meta_data) = self.get_adj_meta_data();
        let num_vertices = self.num_nodes;
        let mut fwd_adj_lists: Vec<Option<SortedAdjVec<Id>>> = vec![Option::None; num_vertices];
        let mut bwd_adj_lists: Vec<Option<SortedAdjVec<Id>>> = vec![Option::None; num_vertices];
        let mut fwd_adj_list_curr_idx = HashMap::new();
        let mut bwd_adj_list_curr_idx = HashMap::new();
        let offset_size = {
            if self.sort_by_node {
                self.num_of_node_labels()
            } else {
                self.num_of_edge_labels()
            }
        };
        for node_id in 0..num_vertices {
            fwd_adj_lists[node_id] = Some(SortedAdjVec::new(
                fwd_adj_meta_data.get(&node_id).unwrap().to_owned(),
            ));
            fwd_adj_list_curr_idx.insert(node_id, vec![0; offset_size + 1]);
            bwd_adj_lists[node_id] = Some(SortedAdjVec::new(
                bwd_adj_meta_data.get(&node_id).unwrap().to_owned(),
            ));
            bwd_adj_list_curr_idx.insert(node_id, vec![0; offset_size + 1]);
        }
        self.edge_indices()
            .flat_map(|(from, to)| {
                if !Ty::is_directed() {
                    return vec![(from, to), (to, from)];
                }
                vec![(from, to)]
            })
            .for_each(|(from, to)| {
                let label_id = self
                    .get_edge(from, to)
                    .get_label_id()
                    .map(|op| op.id() + 1)
                    .unwrap_or(0);
                let (from_type_or_label, to_type_or_label) =
                    if self.sort_by_node {
                        (self.node_types[from.id()], self.node_types[to.id()])
                    } else {
                        (label_id, label_id)
                    };
                let mut idx = fwd_adj_list_curr_idx.get(&from.id()).unwrap()[to_type_or_label];
                let mut offset = fwd_adj_meta_data.get(&from.id()).unwrap()[to_type_or_label];
                fwd_adj_list_curr_idx.get_mut(&from.id()).unwrap()[to_type_or_label] += 1;
                fwd_adj_lists[from.id()]
                    .as_mut()
                    .unwrap()
                    .set_neighbor_id(to, offset + idx);
                idx = bwd_adj_list_curr_idx.get(&to.id()).unwrap()[from_type_or_label];
                offset = bwd_adj_meta_data.get(&to.id()).unwrap()[from_type_or_label];
                bwd_adj_list_curr_idx.get_mut(&to.id()).unwrap()[from_type_or_label] += 1;
                bwd_adj_lists[to.id()]
                    .as_mut()
                    .unwrap()
                    .set_neighbor_id(from, offset + idx);
            });

        for node_id in 0..num_vertices {
            fwd_adj_lists[node_id].as_mut().unwrap().sort();
            bwd_adj_lists[node_id].as_mut().unwrap().sort();
        }

        self.fwd_adj_lists = fwd_adj_lists;
        self.bwd_adj_lists = bwd_adj_lists;
    }

    fn get_node_offsets(&mut self) -> Vec<usize> {
        let mut type_to_count_map: HashMap<usize, usize> = HashMap::new();
        self.node_indices().for_each(|x| {
            let label_id = self
                .get_node(x)
                .get_label_id()
                .map(|op| op.id() + 1)
                .unwrap_or(0);
            let default_v = 0;
            let v = type_to_count_map.get(&label_id).unwrap_or(&default_v);
            type_to_count_map.insert(label_id, v + 1);
        });

        let next_node_label_key = self.num_of_node_labels();
        let mut offsets = vec![0; next_node_label_key + 3];
        type_to_count_map.iter().for_each(|(label_id, cnt)| {
            let label_id = label_id.to_owned();
            let label_cnt = cnt.to_owned();

            if label_id < next_node_label_key + 1 {
                offsets[label_id + 1] = label_cnt;
            }
            offsets[next_node_label_key + 2] += label_cnt;
        });
        for i in 1..offsets.len() - 1 {
            offsets[i] += offsets[i - 1];
        }
        offsets
    }

    fn get_adj_meta_data(&self) -> (HashMap<usize, Vec<usize>>, HashMap<usize, Vec<usize>>) {
        let mut fwd_adj_list_metadata = HashMap::new();
        let mut bwd_adj_list_metadata = HashMap::new();
        let next_node_or_edge = {
            if self.sort_by_node {
                cmp::max(self.num_of_node_labels(), 1)
            } else {
                cmp::max(self.num_of_edge_labels(), 1)
            }
        };
        for i in 0..self.node_count() {
            fwd_adj_list_metadata.insert(i, vec![0; next_node_or_edge + 3]);
            bwd_adj_list_metadata.insert(i, vec![0; next_node_or_edge + 3]);
        }
        self.edge_indices()
            .flat_map(|(from, to)| {
                if Ty::is_directed() {
                    return vec![(from, to)];
                }
                return vec![(from, to), (to, from)];
            })
            .for_each(|(from, to)| {
                if self.sort_by_node {
                    let from_type = self.node_types[from.id()];
                    let to_type = self.node_types[to.id()];
                    fwd_adj_list_metadata.get_mut(&from.id()).unwrap()[to_type + 1] += 1;
                    bwd_adj_list_metadata.get_mut(&to.id()).unwrap()[from_type + 1] += 1;
                } else {
                    let label_id = self
                        .get_edge(from, to)
                        .get_label_id()
                        .map(|op| op.id() + 1)
                        .unwrap_or(0);
                    fwd_adj_list_metadata.get_mut(&from.id()).unwrap()[label_id + 1] += 1;
                    bwd_adj_list_metadata.get_mut(&to.id()).unwrap()[label_id + 1] += 1;
                }
            });
        fwd_adj_list_metadata.iter_mut().for_each(|(_id, offsets)| {
            for i in 1..next_node_or_edge + 2 {
                offsets[next_node_or_edge + 2] += offsets[i];
                offsets[i] += offsets[i - 1];
            }
        });
        bwd_adj_list_metadata.iter_mut().for_each(|(_id, offsets)| {
            for i in 1..next_node_or_edge + 2 {
                offsets[next_node_or_edge + 2] += offsets[i];
                offsets[i] += offsets[i - 1];
            }
        });

        (fwd_adj_list_metadata, bwd_adj_list_metadata)
    }

    pub fn get_node_ids(&self) -> &Vec<Id> {
        &self.node_ids
    }

    pub fn get_node_types(&self) -> &Vec<usize> {
        self.node_types.as_ref()
    }

    pub fn get_node_type_offsets(&self) -> &Vec<usize> {
        self.node_type_offsets.as_ref()
    }

    pub fn get_fwd_adj_list(&self) -> &Vec<Option<SortedAdjVec<Id>>> {
        self.fwd_adj_lists.as_ref()
    }

    pub fn get_bwd_adj_list(&self) -> &Vec<Option<SortedAdjVec<Id>>> {
        self.bwd_adj_lists.as_ref()
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

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
LabelledGraphTrait<Id, NL, EL, L> for TypedStaticGraph<Id, NL, EL, Ty, L>
{
    fn neighbours_of_node(&self, id: Id, label: Option<NL>) -> Iter<Id> {
        if !self.is_sorted_by_node() {
            panic!("Call `neighbours_of_edge` on a graph partition by node");
        }
        if let Some(label) = label {
            let mut iters = vec![];
            // we need to search backward(bwd) list if undirected.
            if !Ty::is_directed() {
                if let Some(bwd_list) = &self.bwd_adj_lists[id.id()] {
                    let offset = bwd_list.get_offsets();
                    let node_label_id = self.node_label_map.find_index(&label).map_or(0, |id| id + 1);
                    let start = offset[node_label_id];
                    let end = offset[node_label_id + 1];
                    iters.push(Iter::new(Box::new(bwd_list.get_neighbour_ids()[start..end].iter())));
                }
            }
            if let Some(fwd_list) = &self.fwd_adj_lists[id.id()] {
                let offset = fwd_list.get_offsets();
                let node_label_id = self.node_label_map.find_index(&label).map_or(0, |id| id + 1);
                let start = offset[node_label_id];
                let end = offset[node_label_id + 1];
                println!("s:{},e:{}", start, end);
                println!("offset：{:?}", offset);
                println!("neighbour_ids:{:?}", &fwd_list.get_neighbour_ids());
                println!("label_neighbour_ids:{:?}", &fwd_list.get_neighbour_ids()[start..end]);
                iters.push(Iter::new(Box::new(fwd_list.get_neighbour_ids()[start..end].iter())));
            }
            return Iter::new(Box::new(iters.into_iter().flat_map(|it| it.map(|x| *x))));
        }
        self.neighbors_iter(id)
    }

    fn neighbours_of_edge(&self, id: Id, label: Option<EL>) -> Iter<Id> {
        if self.is_sorted_by_node() {
            panic!("Call `neighbours_of_edge` on a graph partition by node");
        }
        if let Some(label) = label {
            let mut iters = vec![];
            // we need to search backward(bwd) list if undirected.
            if !Ty::is_directed() {
                if let Some(bwd_list) = &self.bwd_adj_lists[id.id()] {
                    let offset = bwd_list.get_offsets();
                    let edge_label_id = self.edge_label_map.find_index(&label).map_or(0, |id| id + 1);
                    let start = offset[edge_label_id];
                    let end = offset[edge_label_id + 1];
                    iters.push(Iter::new(Box::new(bwd_list.get_neighbour_ids()[start..end].iter())));
                }
            }
            if let Some(fwd_list) = &self.fwd_adj_lists[id.id()] {
                let offset = fwd_list.get_offsets();
                let edge_label_id = self.edge_label_map.find_index(&label).map_or(0, |id| id + 1);
                let start = offset[edge_label_id];
                let end = offset[edge_label_id + 1];
                iters.push(Iter::new(Box::new(fwd_list.get_neighbour_ids()[start..end].iter())));
            }
            return Iter::new(Box::new(iters.into_iter().flat_map(|it| it.map(|x| *x))));
        }
        self.neighbors_iter(id)
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

fn _merge_labels<NL>(_labels1: Option<Vec<NL>>, _labels2: Option<Vec<NL>>) -> Option<Vec<NL>> {
    match (_labels1, _labels2) {
        (None, None) => None,
        (Some(labels1), Some(labels2)) => {
            let (smaller, larger) = if labels1.len() <= labels2.len() {
                (labels1, labels2)
            } else {
                (labels2, labels1)
            };

            let mut result = Vec::with_capacity(larger.len());
            let slen = smaller.len();
            result.extend(smaller.into_iter());
            result.extend(larger.into_iter().skip(slen));

            Some(result)
        }
        (Some(labels), None) => Some(labels),
        (None, Some(labels)) => Some(labels),
    }
}

impl<Id: IdType, NL: Hash + Eq + Clone, EL: Hash + Eq + Clone, Ty: GraphType, L: IdType> Add
for TypedStaticGraph<Id, NL, EL, Ty, L>
{
    type Output = TypedStaticGraph<Id, NL, EL, Ty, L>;

    fn add(self, other: TypedStaticGraph<Id, NL, EL, Ty, L>) -> Self::Output {
        let mut node_label_map = self.node_label_map.clone();
        for item in other.node_label_map.items() {
            node_label_map.add_item(item.clone());
        }

        let mut edge_label_map = self.edge_label_map.clone();
        for item in other.edge_label_map.items() {
            edge_label_map.add_item(item.clone());
        }

        let mut graph = TypedStaticGraph {
            num_nodes: 0,
            num_edges: 0,
            sort_by_node: false,
            node_ids: vec![],
            node_types: vec![],
            node_type_offsets: vec![],
            fwd_adj_lists: vec![],
            bwd_adj_lists: vec![],
            edge_vec: self.edge_vec + other.edge_vec,
            in_edge_vec: match (self.in_edge_vec, other.in_edge_vec) {
                (None, None) => None,
                (Some(left), Some(right)) => Some(left + right),
                _ => panic!("Can not merge Some `in_edge_vec` with None."),
            },
            labels: _merge_labels(self.labels, other.labels),
            graph_type: PhantomData,
            node_label_map,
            edge_label_map,
        };

        graph.num_nodes = graph.edge_vec.num_nodes();
        graph.num_edges = if Ty::is_directed() {
            graph.edge_vec.num_edges()
        } else {
            graph.edge_vec.num_edges() >> 1
        };

        graph
    }
}
