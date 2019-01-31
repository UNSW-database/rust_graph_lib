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
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem;

use generic::{
    DefaultId, DefaultTy, DiGraphTrait, Directed, EdgeType, GeneralGraph, GraphLabelTrait,
    GraphTrait, GraphType, IdType, Iter, MapTrait, MutEdgeType, MutGraphLabelTrait, MutGraphTrait,
    MutMapTrait, MutNodeTrait, MutNodeType, NodeTrait, NodeType, OwnedEdgeType, OwnedNodeType,
    UnGraphTrait, Undirected,
};
use graph_impl::graph_map::{Edge, MutNodeMapTrait, NodeMap, NodeMapTrait};
use graph_impl::{EdgeVec, GraphImpl, TypedStaticGraph};
use map::SetMap;

type Node<Id, L> = (Id, Option<L>);
type Edge<Id, L> = (Id, Id, Option<L>);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphVec<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType = Id> {
    nodes: Vec<Node<Id, L>>,
    edges: Vec<Edge<Id, L>>,
    in_edges: Vec<Edge<Id, L>>,
    node_label_map: SetMap<NL>,
    edge_label_map: SetMap<EL>,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType> GraphVec<Id, NL, EL, L> {
    pub fn new() -> Self {
        GraphVec {
            nodes: Vec::new(),
            edges: Vec::new(),
            in_edges: Vec::new(),
            node_label_map: SetMap::new(),
            edge_label_map: SetMap::new(),
        }
    }

    #[inline]
    fn add_node(&mut self, id: Id, label: Option<NL>) {
        let label_id = label.map(|l| L::new(self.node_label_map.add_item(l)));
        self.nodes.push((id, label_id));
    }

    #[inline]
    fn add_edge(&mut self, src: Id, dst: Id, label: Option<EL>) {
        let label_id = label.map(|l| L::new(self.edge_label_map.add_item(l)));
        self.edges.push((src, dst, label_id));
    }

    #[inline]
    fn add_in_edge(&mut self, src: Id, dst: Id, label: Option<EL>) {
        let label_id = label.map(|l| L::new(self.edge_label_map.add_item(l)));
        self.in_edges.push((src, dst, label_id));
    }

    fn into_static<Ty: GraphType>() -> TypedStaticGraph<Id, NL, EL, Ty, L> {
        unimplemented!()
    }
}
