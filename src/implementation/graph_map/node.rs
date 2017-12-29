use std::collections::HashMap;

use generic::NodeTrait;
use generic::IndexIter;

#[derive(Debug, PartialEq, Clone)]
pub struct Node {
    id: usize,
    label: Option<usize>,
    adj: HashMap<usize, usize>, // <adj node:edge id>
}

impl Node {
    pub fn new(id: usize, label: Option<usize>) -> Self {
        Node {
            id,
            label,
            adj: HashMap::<usize, usize>::new(),
        }
    }
}

impl NodeTrait for Node {
    fn get_id(&self) -> usize {
        self.id
    }

    fn set_label(&mut self, label: usize) {
        self.label = Some(label);
    }

    fn get_label(&self) -> Option<usize> {
        self.label
    }

    fn add_edge(&mut self, adj: usize, edge: usize) {
        if self.get_edge(adj).is_some() {
            panic!("Edge ({},{}) already exist.", self.get_id(), adj);
        }
        self.adj.insert(adj, edge);
    }

    fn get_edge(&self, adj: usize) -> Option<usize> {
        self.adj.get(&adj).map(|x| *x)
    }

    fn remove_edge(&mut self, adj: usize) {
        if self.get_edge(adj).is_none() {
            panic!("Edge ({},{}) not found.", self.get_id(), adj)
        }

        self.adj.remove(&adj);
    }

    fn degree(&self) -> usize {
        self.adj.len()
    }

    fn neighbors<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.adj.keys().map(|i| { *i })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_neighbors() {
        let mut node = Node::new(0, None);

        node.add_edge(1, 1);
        node.add_edge(2, 2);
        node.add_edge(3, 3);

        for neighbor in node.neighbors() {
            println!("*************************************{}", neighbor);
        }
    }
}