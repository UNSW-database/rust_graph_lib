use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use itertools::Itertools;

use generic::GraphType;
use generic::Iter;
use generic::MutMapTrait;
use generic::{DefaultId, IdType};
use generic::{DefaultTy, Directed, Undirected};
use generic::{
    DiGraphTrait, GeneralGraph, GraphLabelTrait, GraphTrait, MutGraphLabelTrait, MutGraphTrait,
    UnGraphTrait,
};
use generic::{EdgeType, MutEdgeTrait, MutNodeTrait};
use generic::{MapTrait, MutNodeMapTrait, NodeMapTrait, NodeTrait, NodeType};

use graph_impl::graph_map::Edge;
use graph_impl::graph_map::NodeMap;
use graph_impl::Graph;
use graph_impl::TypedStaticGraph;

use map::SetMap;

pub type TypedDiGraphMap<Id, NL, EL = NL> = TypedGraphMap<Id, NL, EL, Directed>;
pub type TypedUnGraphMap<Id, NL, EL = NL> = TypedGraphMap<Id, NL, EL, Undirected>;
pub type GraphMap<NL, EL, Ty = DefaultTy> = TypedGraphMap<DefaultId, NL, EL, Ty>;

/// Shortcut of creating a new directed graph where `L` is the data type of labels.
/// # Example
/// ```
/// use rust_graph::DiGraphMap;
/// let  g = DiGraphMap::<&str>::new();
/// ```
pub type DiGraphMap<NL, EL = NL> = GraphMap<NL, EL, Directed>;

/// Shortcut of creating a new undirected graph where `L` is the data type of labels.
/// # Example
/// ```
/// use rust_graph::UnGraphMap;
/// let g = UnGraphMap::<&str>::new();
/// ```
pub type UnGraphMap<NL, EL = NL> = GraphMap<NL, EL, Undirected>;

/// A graph data structure that nodes and edges are stored in hash maps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedGraphMap<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> {
    /// A map <node_id:node>.
    node_map: HashMap<Id, NodeMap<Id>>,
    /// A map <(start,target):edge>.
    edge_map: HashMap<(Id, Id), Edge<Id>>,
    /// A map of node labels.
    node_label_map: SetMap<NL>,
    /// A map of edge labels.
    edge_label_map: SetMap<EL>,
    /// The maximum id has been seen until now.
    max_id: Option<Id>,
    /// A marker of thr graph type, namely, directed or undirected.
    graph_type: PhantomData<Ty>,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> TypedGraphMap<Id, NL, EL, Ty> {
    /// Constructs a new graph.
    pub fn new() -> Self {
        TypedGraphMap {
            node_map: HashMap::<Id, NodeMap<Id>>::new(),
            edge_map: HashMap::<(Id, Id), Edge<Id>>::new(),
            node_label_map: SetMap::<NL>::new(),
            edge_label_map: SetMap::<EL>::new(),
            max_id: None,
            graph_type: PhantomData,
        }
    }

    pub fn with_capacity(
        nodes: usize,
        edge: usize,
        node_labels: usize,
        edge_labels: usize,
    ) -> Self {
        TypedGraphMap {
            node_map: HashMap::<Id, NodeMap<Id>>::with_capacity(nodes),
            edge_map: HashMap::<(Id, Id), Edge<Id>>::with_capacity(edge),
            node_label_map: SetMap::<NL>::with_capacity(node_labels),
            edge_label_map: SetMap::<EL>::with_capacity(edge_labels),
            max_id: None,
            graph_type: PhantomData,
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.node_map.shrink_to_fit();
        self.edge_map.shrink_to_fit();
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
            node_map: HashMap::<Id, NodeMap<Id>>::new(),
            edge_map: HashMap::<(Id, Id), Edge<Id>>::new(),
            node_label_map,
            edge_label_map,
            max_id: None,
            graph_type: PhantomData,
        }
    }

    //    pub fn from_edges(edges: impl IntoIterator<Item = (Id, Id)>) -> Self {
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

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> Default
    for TypedGraphMap<Id, NL, EL, Ty>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> MutGraphTrait<Id, NL, EL>
    for TypedGraphMap<Id, NL, EL, Ty>
{
    type N = NodeMap<Id>;
    type E = Edge<Id>;

    /// Add a node with `id` and `label`. If the node of the `id` already presents,
    /// replace the node's label with the new `label` and return `false`.
    /// Otherwise, add the node and return `true`.
    fn add_node(&mut self, id: Id, label: Option<NL>) -> bool {
        let label_id = label.map(|x| Id::new(self.node_label_map.add_item(x)));

        if self.has_node(id) {
            warn!(
                "GraphMap::add_node - Node {} already exist, updating its label.",
                id,
            );

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
                        self.edge_map.remove(&(id, neighbor));
                    }
                    for in_neighbor in node.in_neighbors_iter() {
                        self.edge_map.remove(&(in_neighbor, id));
                    }
                } else {
                    for neighbor in node.neighbors_iter() {
                        let mut src = id;
                        let mut dst = neighbor;
                        if !self.is_directed() {
                            swap_edge(&mut src, &mut dst);
                        }
                        self.get_node_mut(neighbor).unwrap().remove_edge(id);
                        self.edge_map.remove(&(src, dst));
                    }
                }

                Some(node)
            }
            None => None,
        }
    }

    /// Add the edge with given `start` and `target` vertices.
    /// If either end does not exist, add a new node with corresponding id
    /// and `None` label. If the edge already presents, return `false`,
    /// otherwise add the new edge and return `true`.
    fn add_edge(&mut self, mut start: Id, mut target: Id, label: Option<EL>) -> bool {
        if !self.is_directed() {
            swap_edge(&mut start, &mut target);
        }

        let label_id = label.map(|x| Id::new(self.edge_label_map.add_item(x)));

        if self.has_edge(start, target) {
            warn!(
                "GraphMap::add_edge - Edge ({},{}) already exist, updating its label.",
                start, target,
            );

            self.edge_map
                .get_mut(&(start, target))
                .unwrap()
                .set_label_id(label_id);

            return false;
        }

        if !self.has_node(start) {
            self.add_node(start, None);
        }
        if !self.has_node(target) {
            self.add_node(target, None);
        }

        self.get_node_mut(start).unwrap().add_edge(target);

        if self.is_directed() {
            self.get_node_mut(target).unwrap().add_in_edge(start);
        } else if start != target {
            self.get_node_mut(target).unwrap().add_edge(start);
        }

        let new_edge = Edge::new(start, target, label_id);
        self.edge_map.insert((start, target), new_edge);

        true
    }

    fn get_edge_mut(&mut self, mut start: Id, mut target: Id) -> Option<&mut Self::E> {
        if !self.is_directed() {
            swap_edge(&mut start, &mut target);
        }
        self.edge_map.get_mut(&(start, target))
    }

    fn remove_edge(&mut self, mut start: Id, mut target: Id) -> Option<Self::E> {
        if !self.has_edge(start, target) {
            return None;
        }

        if !self.is_directed() {
            swap_edge(&mut start, &mut target);
        }

        self.get_node_mut(start).unwrap().remove_edge(target);
        if self.is_directed() {
            self.get_node_mut(target).unwrap().remove_in_edge(start);
        } else {
            self.get_node_mut(target).unwrap().remove_edge(start);
        }

        self.edge_map.remove(&(start, target))
    }

    fn nodes_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::N> {
        Iter::new(Box::new(self.node_map.values_mut()))
    }

    fn edges_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::E> {
        Iter::new(Box::new(self.edge_map.values_mut()))
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> GraphTrait<Id>
    for TypedGraphMap<Id, NL, EL, Ty>
{
    fn get_node(&self, id: Id) -> NodeType<Id> {
        match self.node_map.get(&id) {
            Some(node) => NodeType::NodeMap(node),
            None => NodeType::None,
        }
    }

    fn get_edge(&self, mut start: Id, mut target: Id) -> EdgeType<Id> {
        if !self.is_directed() {
            swap_edge(&mut start, &mut target);
        }
        match self.edge_map.get(&(start, target)) {
            None => EdgeType::None,
            Some(edge) => EdgeType::EdgeMap(edge),
        }
    }

    fn has_node(&self, id: Id) -> bool {
        self.node_map.contains_key(&id)
    }

    fn has_edge(&self, mut start: Id, mut target: Id) -> bool {
        if !self.is_directed() {
            swap_edge(&mut start, &mut target);
        }
        self.edge_map.contains_key(&(start, target))
    }

    fn node_count(&self) -> usize {
        self.node_map.len()
    }

    fn edge_count(&self) -> usize {
        self.edge_map.len()
    }

    #[inline(always)]
    fn is_directed(&self) -> bool {
        Ty::is_directed()
    }

    fn node_indices(&self) -> Iter<Id> {
        Iter::new(Box::new(self.node_map.keys().map(|x| *x)))
    }

    fn edge_indices(&self) -> Iter<(Id, Id)> {
        Iter::new(Box::new(self.edge_map.keys().map(|x| *x)))
    }

    fn nodes(&self) -> Iter<NodeType<Id>> {
        Iter::new(Box::new(
            self.node_map.values().map(|node| NodeType::NodeMap(node)),
        ))
    }

    fn edges(&self) -> Iter<EdgeType<Id>> {
        Iter::new(Box::new(
            self.edge_map.values().map(|edge| EdgeType::EdgeMap(edge)),
        ))
    }

    fn degree(&self, id: Id) -> usize {
        match self.get_node(id) {
            NodeType::NodeMap(node) => node.degree(),
            NodeType::None => panic!("Node {} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        match self.get_node(id) {
            NodeType::NodeMap(node) => node.neighbors_iter(),
            NodeType::None => panic!("Node {} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        match self.get_node(id) {
            NodeType::NodeMap(node) => node.neighbors().into(),
            NodeType::None => panic!("Node {} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    fn max_seen_id(&self) -> Option<Id> {
        self.max_id
    }

    fn max_possible_id(&self) -> Id {
        Id::max_value()
    }

    fn implementation(&self) -> Graph {
        Graph::GraphMap
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> GraphLabelTrait<Id, NL, EL>
    for TypedGraphMap<Id, NL, EL, Ty>
{
    fn get_node_label_map(&self) -> &SetMap<NL> {
        &self.node_label_map
    }

    fn get_edge_label_map(&self) -> &SetMap<EL> {
        &self.edge_label_map
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> MutGraphLabelTrait<Id, NL, EL>
    for TypedGraphMap<Id, NL, EL, Ty>
{
    fn update_node_label(&mut self, node_id: Id, label: Option<NL>) -> bool {
        if !self.has_node(node_id) {
            return false;
        }

        let label_id = label.map(|x| Id::new(self.node_label_map.add_item(x)));
        self.get_node_mut(node_id).unwrap().set_label_id(label_id);

        true
    }

    fn update_edge_label(&mut self, start: Id, target: Id, label: Option<EL>) -> bool {
        if !self.has_edge(start, target) {
            return false;
        }

        let label_id = label.map(|x| Id::new(self.edge_label_map.add_item(x)));
        self.get_edge_mut(start, target)
            .unwrap()
            .set_label_id(label_id);

        true
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> UnGraphTrait<Id> for TypedUnGraphMap<Id, NL, EL> {}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> DiGraphTrait<Id> for TypedDiGraphMap<Id, NL, EL> {
    fn in_degree(&self, id: Id) -> usize {
        match self.get_node(id) {
            NodeType::NodeMap(node) => node.in_degree(),
            NodeType::None => panic!("Node {} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    fn in_neighbors_iter(&self, id: Id) -> Iter<Id> {
        match self.get_node(id) {
            NodeType::NodeMap(ref node) => node.in_neighbors_iter(),
            NodeType::None => panic!("Node {} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    fn in_neighbors(&self, id: Id) -> Cow<[Id]> {
        match self.get_node(id) {
            NodeType::NodeMap(ref node) => node.in_neighbors().into(),
            NodeType::None => panic!("Node {} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }

    fn num_of_in_neighbors(&self, id: Id) -> usize {
        match self.get_node(id) {
            NodeType::NodeMap(node) => node.num_of_in_neighbors(),
            NodeType::None => panic!("Node {} do not exist.", id),
            _ => panic!("Unknown error."),
        }
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> GeneralGraph<Id, NL, EL>
    for TypedUnGraphMap<Id, NL, EL>
{
    fn as_graph(&self) -> &GraphTrait<Id> {
        self
    }

    fn as_labeled_graph(&self) -> &GraphLabelTrait<Id, NL, EL> {
        self
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> GeneralGraph<Id, NL, EL>
    for TypedDiGraphMap<Id, NL, EL>
{
    fn as_graph(&self) -> &GraphTrait<Id> {
        self
    }

    fn as_labeled_graph(&self) -> &GraphLabelTrait<Id, NL, EL> {
        self
    }

    fn as_digraph(&self) -> Option<&DiGraphTrait<Id>> {
        Some(self)
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> TypedGraphMap<Id, NL, EL, Ty> {
    pub fn reorder_id(
        self,
        reorder_node_id: bool,
        reorder_node_label: bool,
        reorder_edge_label: bool,
    ) -> Self {
        let node_id_map: Option<SetMap<_>> = if reorder_node_id {
            Some(
                self.nodes()
                    .map(|n| n.unwrap_nodemap())
                    .map(|n| (n.get_id(), n.degree()))
                    .sorted_by_key(|&(_, d)| d)
                    .into_iter()
                    .map(|(n, _)| n)
                    .collect(),
            )
        } else {
            None
        };

        let node_label_map: Option<SetMap<_>> = if reorder_node_id {
            Some(
                self.get_node_label_id_counter()
                    .most_common()
                    .into_iter()
                    .rev()
                    .skip_while(|(_, f)| *f <= 0)
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
                    .skip_while(|(_, f)| *f <= 0)
                    .map(|(n, _)| n)
                    .collect(),
            )
        } else {
            None
        };

        self.reorder_id_with(node_id_map, node_label_map, edge_label_map)
    }

    pub fn reorder_id_with(
        self,
        node_id_map: Option<impl MapTrait<Id>>,
        node_label_map: Option<impl MapTrait<Id>>,
        edge_label_map: Option<impl MapTrait<Id>>,
    ) -> Self {
        if node_id_map.is_none() && node_label_map.is_none() && edge_label_map.is_none() {
            return self;
        }

        let mut new_node_map = HashMap::new();
        let mut new_edge_map = HashMap::new();

        println!("Generating nodemap...");
        for (_, node) in self.node_map {
            let new_node_id = if let Some(ref map) = node_id_map {
                Id::new(map.find_index(&node.id).unwrap())
            } else {
                node.id
            };

            let new_node_label = if let Some(ref map) = node_label_map {
                node.label.map(|i| Id::new(map.find_index(&i).unwrap()))
            } else {
                node.label
            };

            let new_neighbors = if let Some(ref map) = node_id_map {
                node.neighbors
                    .into_iter()
                    .map(|n| Id::new(map.find_index(&n).unwrap()))
                    .collect()
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

        println!("Generating edgemap...");
        for (_, edge) in self.edge_map {
            let mut new_src = if let Some(ref map) = node_id_map {
                Id::new(map.find_index(&edge.src).unwrap())
            } else {
                edge.src
            };

            let mut new_dst = if let Some(ref map) = node_id_map {
                Id::new(map.find_index(&edge.dst).unwrap())
            } else {
                edge.dst
            };

            if !Ty::is_directed() {
                swap_edge(&mut new_src, &mut new_dst);
            }

            let new_edge_label = if let Some(ref map) = edge_label_map {
                edge.label.map(|i| Id::new(map.find_index(&i).unwrap()))
            } else {
                edge.label
            };

            let new_edge = Edge {
                src: new_src,
                dst: new_dst,
                label: new_edge_label,
            };

            new_edge_map.insert((new_src, new_dst), new_edge);
        }

        new_edge_map.shrink_to_fit();

        println!("Generating labelmap...");
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
            edge_map: new_edge_map,
            node_label_map: new_node_label_map,
            edge_label_map: new_edge_label_map,
            max_id: new_max_id,
            graph_type: PhantomData,
        }
    }

    pub fn to_static(
        self,
        reorder_node_id: bool,
        reorder_node_label: bool,
        reorder_edge_label: bool,
    ) -> TypedStaticGraph<Id, NL, EL, Ty> {
        unimplemented!()
    }
}

#[inline(always)]
fn swap_edge<Id: IdType>(src: &mut Id, dst: &mut Id) {
    ::std::mem::swap(src, dst);
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
