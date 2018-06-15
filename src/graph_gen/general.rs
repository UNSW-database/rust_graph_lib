use std::hash::Hash;

use rand::thread_rng;

use generic::GraphType;
use generic::IdType;
use generic::MutGraphTrait;

use graph_impl::TypedGraphMap;
use map::SetMap;

use graph_gen::helper::{complete_edge_pairs, random_edge_label, random_node_label};

pub fn empty_graph<Id, NL, EL, Ty>(
    n: usize,
    node_label: Vec<NL>,
    edge_label: Vec<EL>,
) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    let mut rng = thread_rng();

    let node_label_map = SetMap::from_vec(node_label);
    let edge_label_map = SetMap::from_vec(edge_label);

    let mut g = TypedGraphMap::with_label_map(node_label_map, edge_label_map);

    for i in 0..n {
        let label = random_node_label(&mut rng, &g);
        g.add_node(i, label);
    }

    g
}

pub fn complete_graph<Id, NL, EL, Ty>(
    n: usize,
    node_label: Vec<NL>,
    edge_label: Vec<EL>,
) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    let mut rng = thread_rng();

    let mut g = empty_graph::<Id, NL, EL, Ty>(n, node_label, edge_label);
    for (s, d) in complete_edge_pairs::<Ty>(n) {
        let label = random_edge_label(&mut rng, &g);
        g.add_edge(s, d, label);
    }

    g
}

pub fn empty_graph_unlabeled<Id, NL, EL, Ty>(n: usize) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    empty_graph(n, Vec::new(), Vec::new())
}

pub fn complete_graph_unlabeled<Id, NL, EL, Ty>(n: usize) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    complete_graph(n, Vec::new(), Vec::new())
}
