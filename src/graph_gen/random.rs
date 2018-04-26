use std::hash::Hash;

use rand::{thread_rng, Rng};

use generic::IdType;
use generic::GraphType;
use generic::MutGraphTrait;

use graph_impl::TypedGraphMap;
use map::SetMap;

pub fn random_graph<Id, L, Ty>(
    n: usize,
    p: f32,
    node_label: Vec<L>,
    edge_label: Vec<L>,
) -> TypedGraphMap<Id, L, Ty>
where
    Id: IdType,
    L: Hash + Eq + Clone,
    Ty: GraphType,
{
    let mut rng = thread_rng();
    let node_label_map = SetMap::from_vec(node_label.clone());
    let edge_label_map = SetMap::from_vec(edge_label.clone());

    let mut g = TypedGraphMap::with_label_map(node_label_map, edge_label_map);

    for i in 0..n {
        let label = rng.choose(&node_label).map(|x| x.clone());
        g.add_node(i, label);
    }

    for s in 0..n {
        for d in 0..n {
            if !Ty::is_directed() && d < s {
                continue;
            }

            if rng.gen_range(0f32, 1f32) >= p {
                continue;
            }

            let label = rng.choose(&edge_label).map(|x| x.clone());
            g.add_edge(s, d, label);
        }
    }

    g
}

pub fn random_graph_unlabeled<Id, L, Ty>(n: usize, p: f32) -> TypedGraphMap<Id, L, Ty>
where
    Id: IdType,
    L: Hash + Eq + Clone,
    Ty: GraphType,
{
    random_graph(n, p, Vec::new(), Vec::new())
}
