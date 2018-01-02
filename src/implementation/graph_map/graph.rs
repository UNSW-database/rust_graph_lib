use std::hash::Hash;

use std::collections::HashMap;
use std::marker::PhantomData;


use generic::GraphTrait;
use generic::EdgeTrait;
use generic::NodeTrait;
use generic::ItemMap;
use generic::{Iter, IndexIter};
use generic::GraphType;
use generic::{Directed, Undirected};

use implementation::graph_map::Node;
use implementation::graph_map::Edge;
use implementation::graph_map::LabelMap;


struct GraphMap<L, Ty: GraphType> {
    nodes: HashMap<usize, Node>,
    edges: HashMap<usize, Edge>,
    node_labels: LabelMap<L>,
    edge_labels: LabelMap<L>,
    new_edge_id: usize,
    graph_type: PhantomData<Ty>,
}

impl<L, Ty: GraphType> GraphMap<L, Ty> {
    pub fn new() -> Self {
        GraphMap {
            nodes: HashMap::<usize, Node>::new(),
            edges: HashMap::<usize, Edge>::new(),
            node_labels: LabelMap::<L>::new(),
            edge_labels: LabelMap::<L>::new(),
            new_edge_id: 0,
            graph_type: PhantomData,
        }
    }
}

pub type DiGraphMap<L> = GraphMap<L, Directed>;
pub type UnGraphMap<L> = GraphMap<L, Undirected>;


impl<L: Hash + Eq, Ty: GraphType> GraphTrait<L> for GraphMap<L, Ty>
{
    type N = Node;
    type E = Edge;

    fn add_node(&mut self, id: usize, label: Option<L>) {
        if self.has_node(id) {
            panic!("Node {} already exist.", id);
        }

        let label_id = label.map(|x| self.node_labels.add_item(x));

        let new_node = Node::new(id, label_id);
        self.nodes.insert(id, new_node);
    }

    fn get_node(&self, id: usize) -> Option<&Self::N> {
        self.nodes.get(&id)
    }

    fn get_node_mut(&mut self, id: usize) -> Option<&mut Self::N> {
        self.nodes.get_mut(&id)
    }

    fn remove_node(&mut self, id: usize) -> Option<Self::N> {
        if !self.has_node(id) {
            return None;
        }

        let node = self.nodes.remove(&id).unwrap();

        for out_edge in node.out_edges() {
            self.edges.remove(&out_edge);
        }

        if self.is_directed() {
            for out_neighbor in node.out_neighbors() {
                self.get_node_mut(out_neighbor).unwrap().remove_in_edge(id);
            }
            for in_edge in node.in_edges() {
                self.edges.remove(&in_edge);
            }
        } else {
            for out_neighbor in node.out_neighbors() {
                self.get_node_mut(out_neighbor).unwrap().remove_out_edge(id);
            }
        }

        Some(node)
    }

    fn add_edge(&mut self, start: usize, target: usize, label: Option<L>) -> usize {
        if !self.has_node(start) {
            panic!("The node with id {} has not been created yet.", start);
        }
        if !self.has_node(target) {
            panic!("The node with id {} has not been created yet.", target);
        }
        if self.find_edge_id(start, target).is_some() {
            panic!("Edge ({},{}) already exist.", start, target)
        }

        let edge_id = self.new_edge_id;

        self.get_node_mut(start).unwrap().add_out_edge(target, edge_id);

        if self.is_directed() {
            self.get_node_mut(target).unwrap().add_in_edge(start, edge_id);
        } else {
            self.get_node_mut(target).unwrap().add_out_edge(start, edge_id);
        }

        let label_id = label.map(|x| self.edge_labels.add_item(x));

        let new_edge = Edge::new(edge_id, start, target, label_id);
        self.edges.insert(edge_id, new_edge);

        self.new_edge_id += 1;
        edge_id
    }

    fn get_edge(&self, id: usize) -> Option<&Self::E> {
        self.edges.get(&id)
    }

    fn get_edge_mut(&mut self, id: usize) -> Option<&mut Self::E> {
        self.edges.get_mut(&id)
    }


    fn find_edge_id(&self, start: usize, target: usize) -> Option<usize> {
        if !self.has_node(start) {
            return None;
        }

        self.get_node(start).unwrap().get_out_edge(target)
    }


    fn find_edge(&self, start: usize, target: usize) -> Option<&Self::E> {
        match self.find_edge_id(start, target) {
            Some(id) => self.get_edge(id),
            None => None
        }
    }

    fn find_edge_mut(&mut self, start: usize, target: usize) -> Option<&mut Self::E> {
        match self.find_edge_id(start, target) {
            Some(id) => self.get_edge_mut(id),
            None => None
        }
    }

    fn remove_edge(&mut self, start: usize, target: usize) -> Option<Self::E> {
        match self.find_edge_id(start, target) {
            Some(edge_id) => {
                self.get_node_mut(start).unwrap().remove_out_edge(target);
                if self.is_directed() {
                    self.get_node_mut(target).unwrap().remove_in_edge(start);
                } else {
                    self.get_node_mut(target).unwrap().remove_out_edge(start);
                }
                self.edges.remove(&edge_id)
            }
            None => None
        }
    }

    fn has_node(&self, id: usize) -> bool {
        self.nodes.contains_key(&id)
    }

    fn has_edge(&self, id: usize) -> bool {
        self.edges.contains_key(&id)
    }

    fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn edge_count(&self) -> usize {
        self.edges.len()
    }

    fn is_directed(&self) -> bool {
        Ty::is_directed()
    }

    fn node_indices<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.nodes.keys().map(|i| { *i })))
    }

    fn edge_indices<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.edges.keys().map(|i| { *i })))
    }

    fn nodes<'a>(&'a self) -> Iter<'a, &Self::N> {
        Iter::new(Box::new(self.nodes.values()))
    }

    fn edges<'a>(&'a self) -> Iter<'a, &Self::E> {
        Iter::new(Box::new(self.edges.values()))
    }

    fn nodes_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::N> {
        Iter::new(Box::new(self.nodes.values_mut()))
    }

    fn edges_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::E> {
        Iter::new(Box::new(self.edges.values_mut()))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_get_node() {
        let mut g = UnGraphMap::<&str>::new();

        g.add_node(0, Some("a"));
        assert_eq!(g.node_count(), 1);
        g.add_node(1, Some("b"));
        assert_eq!(g.node_count(), 2);
        g.add_node(2, Some("a"));
        assert_eq!(g.node_count(), 3);
        g.add_node(3, None);
        assert_eq!(g.node_count(), 4);

        let n0_expected = Node::new(0, Some(0));
        let n1_expected = Node::new(1, Some(1));
        let mut n2_expected = Node::new(2, Some(0));
        let mut n3_expected = Node::new(3, None);

        assert_eq!(g.get_node(0), Some(&n0_expected));
        assert_eq!(g.get_node(1), Some(&n1_expected));
        assert_eq!(g.get_node_mut(2), Some(&mut n2_expected));
        assert_eq!(g.get_node_mut(3), Some(&mut n3_expected));
        assert_eq!(g.get_node(4), None);
    }

    #[test]
    #[should_panic]
    fn test_duplicate_node() {
        let mut g = UnGraphMap::<&str>::new();

        g.add_node(0, Some("a"));
        g.add_node(0, None);
    }

    #[test]
    fn test_remove_node_directed() {
        let mut g = DiGraphMap::<&str>::new();

        g.add_node(0, None);
        g.add_node(1, None);
        g.add_node(2, None);

        g.add_edge(0, 1, None);
        g.add_edge(0, 2, None);
        g.add_edge(1, 0, None);

        g.remove_node(0);

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 0);
        assert!(!g.has_node(0));
        assert_eq!(g.find_edge_id(0, 1), None);
        assert_eq!(g.find_edge_id(0, 2), None);
        assert_eq!(g.find_edge_id(1, 0), None);
    }

    #[test]
    fn test_remove_node_undirected() {
        let mut g = UnGraphMap::<&str>::new();

        g.add_node(0, None);
        g.add_node(1, None);
        g.add_node(2, None);

        g.add_edge(0, 1, None);
        g.add_edge(0, 2, None);
        g.add_edge(1, 2, None);

        g.remove_node(0);

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 1);
        assert!(!g.has_node(0));
        assert_eq!(g.find_edge_id(0, 1), None);
        assert_eq!(g.find_edge_id(1, 0), None);
        assert_eq!(g.find_edge_id(0, 2), None);
        assert_eq!(g.find_edge_id(2, 0), None);
    }

    #[test]
    fn test_add_get_edge_directed() {
        let mut g = DiGraphMap::<&str>::new();

        g.add_node(0, None);
        g.add_node(1, None);
        g.add_node(2, None);


        let e0_id = g.add_edge(0, 1, Some("a"));
        assert_eq!(g.edge_count(), 1);
        let e1_id = g.add_edge(1, 2, Some("b"));
        assert_eq!(g.edge_count(), 2);
        let e2_id = g.add_edge(2, 0, Some("a"));
        assert_eq!(g.edge_count(), 3);
        let e3_id = g.add_edge(1, 0, None);
        assert_eq!(g.edge_count(), 4);


        let e0_expected = Edge::new(0, 0, 1, Some(0));
        let e1_expected = Edge::new(1, 1, 2, Some(1));
        let mut e2_expected = Edge::new(2, 2, 0, Some(0));
        let mut e3_expected = Edge::new(3, 1, 0, None);

        assert_eq!(g.get_edge(e0_id), Some(&e0_expected));
        assert_eq!(g.get_edge(e1_id), Some(&e1_expected));
        assert_eq!(g.get_edge_mut(e2_id), Some(&mut e2_expected));
        assert_eq!(g.get_edge_mut(e3_id), Some(&mut e3_expected));
        assert_eq!(g.get_edge(4), None);

        assert_eq!(g.find_edge_id(0, 1), Some(e0_id));
        assert_eq!(g.find_edge(1, 2), Some(&e1_expected));
        assert_eq!(g.find_edge_mut(2, 0), Some(&mut e2_expected));

        assert_eq!(g.find_edge(1, 3), None);
        assert_eq!(g.find_edge_mut(2, 1), None);
    }


    #[test]
    fn test_add_get_edge_undirected() {
        let mut g = UnGraphMap::<&str>::new();

        g.add_node(0, None);
        g.add_node(1, None);
        g.add_node(2, None);
        g.add_node(3, None);


        let e0_id = g.add_edge(0, 1, Some("a"));
        assert_eq!(g.edge_count(), 1);
        let e1_id = g.add_edge(1, 2, Some("b"));
        assert_eq!(g.edge_count(), 2);
        let e2_id = g.add_edge(2, 0, Some("a"));
        assert_eq!(g.edge_count(), 3);


        let e0_expected = Edge::new(0, 0, 1, Some(0));
        let e1_expected = Edge::new(1, 1, 2, Some(1));
        let mut e2_expected = Edge::new(2, 2, 0, Some(0));

        assert_eq!(g.get_edge(e0_id), Some(&e0_expected));
        assert_eq!(g.get_edge(e1_id), Some(&e1_expected));
        assert_eq!(g.get_edge_mut(e2_id), Some(&mut e2_expected));
        assert_eq!(g.get_edge(4), None);

        assert_eq!(g.find_edge_id(0, 1), Some(e0_id));
        assert_eq!(g.find_edge_id(1, 0), Some(e0_id));
        assert_eq!(g.find_edge(1, 2), Some(&e1_expected));
        assert_eq!(g.find_edge(2, 1), Some(&e1_expected));
        assert_eq!(g.find_edge_mut(2, 0), Some(&mut e2_expected));
        assert_eq!(g.find_edge_mut(0, 2), Some(&mut e2_expected));

        assert_eq!(g.find_edge(1, 3), None);
        assert_eq!(g.find_edge_mut(0, 3), None);
    }

    #[test]
    #[should_panic]
    fn test_multi_edge_directed() {
        let mut g = DiGraphMap::<&str>::new();
        g.add_node(0, None);
        g.add_node(1, None);
        g.add_edge(0, 1, None);
        g.add_edge(0, 1, None);
    }

    #[test]
    #[should_panic]
    fn test_multi_edge_undirected() {
        let mut g = UnGraphMap::<&str>::new();
        g.add_node(0, None);
        g.add_node(1, None);
        g.add_edge(0, 1, None);
        g.add_edge(1, 0, None);
    }


    #[test]
    #[should_panic]
    fn test_invalid_edge() {
        let mut g = DiGraphMap::<&str>::new();
        g.add_edge(0, 1, None);
    }

    #[test]
    fn test_remove_edge_directed() {
        let mut g = DiGraphMap::<&str>::new();
        g.add_node(0, None);
        g.add_node(1, None);
        g.add_edge(0, 1, None);
        g.add_edge(1, 0, None);


        g.remove_edge(0, 1);
        assert_eq!(g.edge_count(), 1);
        assert_eq!(g.find_edge(0, 1), None);
    }

    #[test]
    fn test_remove_edge_undirected() {
        let mut g = UnGraphMap::<&str>::new();
        g.add_node(0, None);
        g.add_node(1, None);
        g.add_edge(0, 1, None);

        g.remove_edge(1, 0);
        assert_eq!(g.edge_count(), 0);
        assert_eq!(g.find_edge(0, 1), None);
    }
}