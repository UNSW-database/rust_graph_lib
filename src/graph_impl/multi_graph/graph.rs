use generic::{EdgeType, GraphTrait, GraphType, IdType, Iter, NodeType};
use graph_impl::multi_graph::node::MultiNode;
use graph_impl::static_graph::sorted_adj_vec::SortedAdjVec;
use graph_impl::static_graph::static_edge_iter::StaticEdgeIndexIter;
use graph_impl::static_graph::EdgeVecTrait;
use graph_impl::{Edge, EdgeVec, GraphImpl};
use map::SetMap;
use std::borrow::Cow;
use std::hash::Hash;
use std::marker::PhantomData;

// TODO: Implement a multi_graph support multi_edge and multi_label
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TypedMultiGraph<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType = Id>
{
    num_nodes: usize,
    num_edges: usize,

    // node Ids indexed by type and random access to node types.
    node_ids: Vec<usize>,
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
    for TypedMultiGraph<Id, NL, EL, Ty, L>
{
    fn eq(&self, other: &Self) -> bool {
        if !self.node_count() == other.node_count() || !self.edge_count() == other.edge_count() {
            return false;
        }
        for n in self.node_indices() {
            //            if !other.has_node(n) || self.get_node_label(n) != other.get_node_label(n) {
            //                return false;
            //            }
        }
        for (s, d) in self.edge_indices() {
            //            if !other.has_edge(s, d) || self.get_edge_label(s, d) != other.get_edge_label(s, d) {
            //                return false;
            //            }
        }
        true
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> GraphTrait<Id, L>
    for TypedMultiGraph<Id, NL, EL, Ty, L>
{
    fn get_node(&self, id: Id) -> NodeType<Id, L> {
        if !self.has_node(id) {
            return NodeType::None;
        }
        match self.labels {
            Some(ref labels) => NodeType::MultiNode(MultiNode::new_static(id, labels[id.id()])),
            None => NodeType::MultiNode(MultiNode::new(id, None)),
        }
    }

    fn get_edge(&self, start: Id, target: Id) -> EdgeType<Id, L> {
        unimplemented!()
    }

    fn has_node(&self, id: Id) -> bool {
        unimplemented!()
    }

    fn has_edge(&self, start: Id, target: Id) -> bool {
        unimplemented!()
    }

    fn node_count(&self) -> usize {
        unimplemented!()
    }

    fn edge_count(&self) -> usize {
        unimplemented!()
    }

    fn is_directed(&self) -> bool {
        unimplemented!()
    }

    fn node_indices(&self) -> Iter<Id> {
        unimplemented!()
    }

    fn edge_indices(&self) -> Iter<(Id, Id)> {
        Iter::new(Box::new(StaticEdgeIndexIter::new(
            Box::new(&self.edge_vec),
            self.is_directed(),
        )))
    }

    fn nodes(&self) -> Iter<NodeType<Id, L>> {
        unimplemented!()
    }

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

    fn degree(&self, id: Id) -> usize {
        self.edge_vec.degree(id)
    }

    fn total_degree(&self, id: Id) -> usize {
        let mut degree = self.degree(id);
        if self.is_directed() {
            degree += self.in_edge_vec.as_ref().unwrap().neighbors(id).len()
        }
        degree
    }

    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        let neighbors = self.edge_vec.neighbors(id);
        Iter::new(Box::new(neighbors.iter().map(|x| *x)))
    }

    // if exist multi edges, the neighbours will repeat.
    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        self.edge_vec.neighbors(id).into()
    }

    fn max_seen_id(&self) -> Option<Id> {
        Some(Id::new(self.node_count() - 1))
    }

    fn implementation(&self) -> GraphImpl {
        GraphImpl::MultiGraph
    }
}
