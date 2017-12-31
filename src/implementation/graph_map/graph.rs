use std::hash::Hash;

use std::collections::HashMap;


use generic::GraphTrait;
use generic::EdgeTrait;
use generic::NodeTrait;
use generic::ItemMap;
use generic::IndexIter;

use implementation::graph_map::Node;
use implementation::graph_map::Edge;
use implementation::graph_map::LabelMap;


struct GraphMap<L> {
    nodes: HashMap<usize, Node>,
    edges: HashMap<usize, Edge>,
    labels: LabelMap<L>,
    new_edge_id: usize,
    is_directed: bool,
}

impl<L> GraphMap<L> {
    pub fn new(is_directed: bool) -> Self {
        GraphMap {
            nodes: HashMap::<usize, Node>::new(),
            edges: HashMap::<usize, Edge>::new(),
            labels: LabelMap::<L>::new(),
            new_edge_id: 0,
            is_directed,
        }
    }
}


impl<L: Hash + Eq> GraphTrait<L> for GraphMap<L>
{
    type N = Node;
    type E = Edge;

    fn add_node(&mut self, id: usize, label: Option<L>) {
        if self.has_node(id) {
            panic!("Node {} already exist.", id);
        }

        let label_id = label.map(|x| self.labels.add_item(x));

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

        for neighbor in node.neighbors() {
            self.edges.remove(&node.get_edge(neighbor).unwrap());
            if !self.is_directed() {
                self.get_node_mut(neighbor).unwrap().remove_edge(id);
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
        if self.find_edge(start, target).is_some() {
            panic!("Edge ({},{}) already exist.", start, target)
        }

        let edge_id = self.new_edge_id;

        self.get_node_mut(start).unwrap().add_edge(target, edge_id);

        if !self.is_directed {
            self.get_node_mut(target).unwrap().add_edge(start, edge_id);
        }

        let label_id = label.map(|x| self.labels.add_item(x));

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

        self.get_node(start).unwrap().get_edge(target)
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
        if let Some(edge_id) = self.find_edge_id(start, target) {
            self.get_node_mut(start).unwrap().remove_edge(target);
            if !self.is_directed {
                self.get_node_mut(target).unwrap().remove_edge(start);
            }

            self.edges.remove(&edge_id)
        } else {
            None
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

    fn degree(&self, node: usize) -> usize {
        if !self.has_node(node) {
            panic!("Node {} not found.", node)
        }

        self.get_node(node).unwrap().degree()
    }

    fn is_directed(&self) -> bool {
        self.is_directed
    }

    fn get_label(&self, id: usize) -> Option<&L> {
        self.labels.find_item(id)
    }

    fn nodes<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.nodes.keys().map(|i| { *i })))
    }

    fn edges<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.edges.keys().map(|i| { *i })))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_get_node() {
        let mut g = GraphMap::<&str>::new(true);

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
        let mut g = GraphMap::<&str>::new(true);

        g.add_node(0, Some("a"));
        g.add_node(0, None);
    }

    #[test]
    fn test_remove_node_directed() {
        let mut g = GraphMap::<&str>::new(true);

        g.add_node(0, None);
        g.add_node(1, None);
        g.add_node(2, None);

        g.add_edge(0, 1, None);
        g.add_edge(0, 2, None);
        g.add_edge(1, 0, None);

        g.remove_node(0);

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 1);
        assert!(!g.has_node(0));
        assert_eq!(g.find_edge(0, 1), None);
        assert_eq!(g.find_edge(0, 2), None);
    }

    #[test]
    fn test_remove_node_undirected() {
        let mut g = GraphMap::<&str>::new(false);

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
        assert_eq!(g.find_edge(0, 1), None);
        assert_eq!(g.find_edge(1, 0), None);
        assert_eq!(g.find_edge(0, 2), None);
        assert_eq!(g.find_edge(2, 0), None);
    }

    #[test]
    fn test_add_get_edge_directed() {
        let mut g = GraphMap::<&str>::new(true);

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
        let mut g = GraphMap::<&str>::new(false);

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
        let mut g = GraphMap::<&str>::new(true);
        g.add_node(0, None);
        g.add_node(1, None);
        g.add_edge(0, 1, None);
        g.add_edge(0, 1, None);
    }

    #[test]
    #[should_panic]
    fn test_multi_edge_undirected() {
        let mut g = GraphMap::<&str>::new(false);
        g.add_node(0, None);
        g.add_node(1, None);
        g.add_edge(0, 1, None);
        g.add_edge(1, 0, None);
    }


    #[test]
    #[should_panic]
    fn test_invalid_edge() {
        let mut g = GraphMap::<&str>::new(true);
        g.add_edge(0, 1, None);
    }

    #[test]
    fn test_remove_edge_directed() {
        let mut g = GraphMap::<&str>::new(true);
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
        let mut g = GraphMap::<&str>::new(false);
        g.add_node(0, None);
        g.add_node(1, None);
        g.add_edge(0, 1, None);

        g.remove_edge(1, 0);
        assert_eq!(g.edge_count(), 0);
        assert_eq!(g.find_edge(0, 1), None);
    }
}