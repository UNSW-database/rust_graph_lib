use std::hash::Hash;

use generic::{DiGraphTrait, GraphLabelTrait, GraphTrait};
use generic::{EdgeTrait, NodeTrait};
use generic::GraphType;

use generic::{MapTrait, MutMapTrait};
use generic::{Directed, Undirected};

use graph_impl::{DiGraphMap, DiStaticGraph, GraphMap, StaticGraph, UnGraphMap, UnStaticGraph};

use graph_impl::static_graph::EdgeVec;

use map::{SetMap, VecMap};

/// Marker for None label
pub const END: usize = ::std::usize::MAX;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticGraphConverter<L, Ty>
where
    L: Hash + Eq,
    Ty: GraphType,
{
    pub graph: StaticGraph<Ty>,
    pub node_id_map: VecMap<usize>,
    pub node_label_map: VecMap<L>,
    pub edge_label_map: VecMap<L>,
}

pub type DiStaticGraphConverter<L> = StaticGraphConverter<L, Directed>;
pub type UnStaticGraphConverter<L> = StaticGraphConverter<L, Undirected>;

impl<L> From<DiGraphMap<L>> for DiStaticGraphConverter<L>
where
    L: Hash + Eq + Clone,
{
    fn from(g: DiGraphMap<L>) -> Self {
        let node_id_map = get_node_id_map(&g);
        let node_label_map = get_node_label_id_map(&g);
        let edge_label_map = get_edge_label_id_map(&g);

        let edge_vec = get_edge_vec(&g, &node_id_map, &edge_label_map);
        let node_labels = get_node_labels(&g, &node_id_map, &node_label_map);

        let in_edge_vec = Some(get_in_edge_vec(&g, &node_id_map));

        let graph = match node_labels {
            Some(labels) => {
                DiStaticGraph::with_labels(g.node_count(), edge_vec, in_edge_vec, labels)
            }
            None => DiStaticGraph::new(g.node_count(), edge_vec, in_edge_vec),
        };

        let node_id_map = VecMap::from(node_id_map);
        let node_label_map = VecMap::from(merge_map(&node_label_map, g.get_node_label_map()));
        let edge_label_map = VecMap::from(merge_map(&edge_label_map, g.get_edge_label_map()));

        StaticGraphConverter {
            graph,
            node_id_map,
            node_label_map,
            edge_label_map,
        }
    }
}

impl<L> From<UnGraphMap<L>> for UnStaticGraphConverter<L>
where
    L: Hash + Eq + Clone,
{
    fn from(g: UnGraphMap<L>) -> Self {
        let node_id_map = get_node_id_map(&g);
        let node_label_map = get_node_label_id_map(&g);
        let edge_label_map = get_edge_label_id_map(&g);

        let edge_vec = get_edge_vec(&g, &node_id_map, &edge_label_map);
        let node_labels = get_node_labels(&g, &node_id_map, &node_label_map);

        let in_edge_vec = None;

        let graph = match node_labels {
            Some(labels) => {
                UnStaticGraph::with_labels(g.node_count(), edge_vec, in_edge_vec, labels)
            }
            None => UnStaticGraph::new(g.node_count(), edge_vec, in_edge_vec),
        };

        let node_id_map = VecMap::from(node_id_map);
        let node_label_map = VecMap::from(merge_map(&node_label_map, g.get_node_label_map()));
        let edge_label_map = VecMap::from(merge_map(&edge_label_map, g.get_edge_label_map()));

        StaticGraphConverter {
            graph,
            node_id_map,
            node_label_map,
            edge_label_map,
        }
    }
}

impl<L> From<UnGraphMap<L>> for UnStaticGraph
where
    L: Hash + Eq + Clone,
{
    fn from(g: UnGraphMap<L>) -> Self {
        UnStaticGraphConverter::from(g).graph
    }
}

impl<L> From<DiGraphMap<L>> for DiStaticGraph
where
    L: Hash + Eq + Clone,
{
    fn from(g: DiGraphMap<L>) -> Self {
        DiStaticGraphConverter::from(g).graph
    }
}

/// Map node id to a continuous range (sort by degree)
fn get_node_id_map<L, Ty>(g: &GraphMap<L, Ty>) -> SetMap<usize>
where
    L: Hash + Eq,
    Ty: GraphType,
{
    let mut node_degree: Vec<_> = g.nodes().map(|n| (n.get_id(), n.degree())).collect();
    node_degree.sort_unstable_by_key(|&(_, d)| d);

    let mut node_id_map = SetMap::<usize>::new();
    for (n, _) in node_degree {
        node_id_map.add_item(n);
    }
    node_id_map
}

/// Re-assign node label id sorted by its frequency
fn get_node_label_id_map<L, Ty>(g: &GraphMap<L, Ty>) -> SetMap<usize>
where
    L: Hash + Eq,
    Ty: GraphType,
{
    let mut label_counter: Vec<_> = g.get_node_label_id_counter()
        .into_iter()
        .filter(|&(_, f)| f > 0)
        .collect();
    label_counter.sort_unstable_by_key(|&(_, f)| f);

    let mut label_map = SetMap::<usize>::new();
    for (n, _) in label_counter {
        label_map.add_item(n);
    }
    label_map
}

fn merge_map<L>(new_map: &SetMap<usize>, old_map: &SetMap<L>) -> SetMap<L>
where
    L: Hash + Eq + Clone,
{
    let mut merged = SetMap::<L>::new();

    for i in new_map.items() {
        let item = old_map.find_item(*i).unwrap().clone();
        merged.add_item(item);
    }

    merged
}

/// Re-assign edge label id sorted by its frequency
fn get_edge_label_id_map<L, Ty>(g: &GraphMap<L, Ty>) -> SetMap<usize>
where
    L: Hash + Eq,
    Ty: GraphType,
{
    let mut label_counter: Vec<_> = g.get_edge_label_id_counter()
        .into_iter()
        .filter(|&(_, f)| f > 0)
        .collect();
    label_counter.sort_unstable_by_key(|&(_, f)| f);

    let mut label_map = SetMap::<usize>::new();
    for (n, _) in label_counter {
        label_map.add_item(n);
    }
    label_map
}

/// Convert node labels into a `Vec`
fn get_node_labels<L, Ty>(
    g: &GraphMap<L, Ty>,
    node_map: &SetMap<usize>,
    label_map: &SetMap<usize>,
) -> Option<Vec<usize>>
where
    L: Hash + Eq,
    Ty: GraphType,
{
    if g.node_labels().next().is_none() {
        return None;
    }

    let mut labels: Vec<usize> = Vec::with_capacity(g.node_count());

    for node_id in node_map.items() {
        labels.push(match g.get_node(*node_id).unwrap().get_label_id() {
            Some(label) => label_map.find_index(&label).unwrap(),
            None => END,
        });
    }

    Some(labels)
}

/// Convert edges into `EdgeVec`
fn get_edge_vec<L, Ty>(
    g: &GraphMap<L, Ty>,
    node_map: &SetMap<usize>,
    label_map: &SetMap<usize>,
) -> EdgeVec
where
    L: Hash + Eq,
    Ty: GraphType,
{
    let has_edge_label = g.edge_labels().next().is_some();
    let offset_len = g.node_count() + 1;
    let edge_len = if g.is_directed() {
        g.edge_count()
    } else {
        2 * g.edge_count()
    };

    let mut offset: usize = 0;
    let mut offset_vec: Vec<usize> = Vec::with_capacity(offset_len);
    let mut edge_vec: Vec<usize> = Vec::with_capacity(edge_len);

    let mut edge_labels: Option<Vec<usize>> = if has_edge_label {
        Some(Vec::with_capacity(edge_len))
    } else {
        None
    };

    for node_id in node_map.items() {
        offset_vec.push(offset);

        let mut neighbors: Vec<_> = g.neighbor_indices(*node_id)
            .map(|i| node_map.find_index(&i).unwrap())
            .collect();

        neighbors.sort();
        offset += neighbors.len();

        for neighbor in neighbors {
            edge_vec.push(neighbor);

            if let Some(ref mut labels) = edge_labels {
                let original_node = node_map.find_item(neighbor).unwrap();

                labels.push(match g.find_edge(*node_id, *original_node)
                    .unwrap()
                    .get_label_id()
                {
                    Some(label) => label_map.find_index(&label).unwrap(),
                    None => END,
                });
            }
        }
    }

    offset_vec.push(edge_len);

    match edge_labels {
        Some(labels) => EdgeVec::with_labels(offset_vec, edge_vec, labels),
        None => EdgeVec::new(offset_vec, edge_vec),
    }
}

/// Convert in-edges into `EdgeVec` (edge labels will be ignored)
fn get_in_edge_vec<L>(g: &DiGraphMap<L>, node_map: &SetMap<usize>) -> EdgeVec
where
    L: Hash + Eq,
{
    let offset_len = g.node_count() + 1;
    let edge_len = g.edge_count();

    let mut offset: usize = 0;
    let mut offset_vec: Vec<usize> = Vec::with_capacity(offset_len);
    let mut edge_vec: Vec<usize> = Vec::with_capacity(edge_len);

    for node_id in node_map.items() {
        offset_vec.push(offset);

        let mut neighbors: Vec<_> = g.in_neighbor_indices(*node_id)
            .map(|i| node_map.find_index(&i).unwrap())
            .collect();

        neighbors.sort();
        offset += neighbors.len();

        for neighbor in neighbors {
            edge_vec.push(neighbor);
        }
    }

    offset_vec.push(edge_len);

    EdgeVec::new(offset_vec, edge_vec)
}
