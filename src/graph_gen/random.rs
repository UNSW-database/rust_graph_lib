use std::hash::Hash;

use rand::seq::sample_iter;
use rand::{thread_rng, Rng};

use generic::GraphType;
use generic::IdType;
use generic::MutGraphTrait;

use graph_gen::general::empty_graph;
use graph_gen::helper::{complete_edge_pairs, random_edge_label};
use graph_impl::TypedGraphMap;

pub fn random_gnp_graph<Id, NL, EL, Ty>(
    n: usize,
    p: f32,
    node_label: Vec<NL>,
    edge_label: Vec<EL>,
) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    if p < 0f32 || p > 1f32 {
        panic!("p must be in the range of [0,1]");
    }

    let mut rng = thread_rng();

    let mut g = empty_graph::<Id, NL, EL, Ty>(n, node_label, edge_label);

    for (s, d) in complete_edge_pairs::<Ty>(n) {
        if rng.gen_range(0f32, 1f32) < p {
            let label = random_edge_label(&mut rng, &g);
            g.add_edge(Id::new(s), Id::new(d), label);
        }
    }

    g
}

pub fn random_gnm_graph<Id, NL, EL, Ty>(
    n: usize,
    m: usize,
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
    let sampled_edges = sample_iter(&mut rng, complete_edge_pairs::<Ty>(n), m);

    if let Ok(mut edges) = sampled_edges {
        for (s, d) in edges.drain(..) {
            let label = random_edge_label(&mut rng, &g);
            g.add_edge(Id::new(s), Id::new(d), label);
        }
        g
    } else {
        panic!("m is too large.");
    }
}

pub fn random_gnp_graph_unlabeled<Id, NL, EL, Ty>(n: usize, p: f32) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    random_gnp_graph(n, p, Vec::new(), Vec::new())
}

pub fn random_gnm_graph_unlabeled<Id, NL, EL, Ty>(
    n: usize,
    m: usize,
) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    random_gnm_graph(n, m, Vec::new(), Vec::new())
}
