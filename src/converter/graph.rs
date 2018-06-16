use std::hash::Hash;

use generic::{DefaultId, IdType};
use generic::{DiGraphTrait, GraphLabelTrait, GraphTrait};
use generic::{Directed, GraphType, Undirected};
use generic::{EdgeTrait, NodeTrait};
use generic::{MapTrait, MutMapTrait};

use graph_impl::graph_map::node::NodeMapTrait;
use graph_impl::static_graph::EdgeVec;
use graph_impl::{TypedDiGraphMap, TypedGraphMap, TypedUnGraphMap};
use graph_impl::{TypedDiStaticGraph, TypedStaticGraph, TypedUnStaticGraph};

use map::SetMap;

pub type TypedDiStaticGraphConverter<Id, NL, EL> = TypedStaticGraphConverter<Id, NL, EL, Directed>;
pub type TypedUnStaticGraphConverter<Id, NL, EL> =
    TypedStaticGraphConverter<Id, NL, EL, Undirected>;
pub type StaticGraphConverter<NL, EL, Ty> = TypedStaticGraphConverter<DefaultId, NL, EL, Ty>;
pub type DiStaticGraphConverter<NL, EL> = StaticGraphConverter<NL, EL, Directed>;
pub type UnStaticGraphConverter<NL, EL> = StaticGraphConverter<NL, EL, Undirected>;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct TypedStaticGraphConverter<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
    Ty: GraphType,
{
    graph: TypedStaticGraph<Id, Ty>,
    node_id_map: SetMap<Id>,
    node_label_map: SetMap<NL>,
    edge_label_map: SetMap<EL>,
}

impl<Id, NL, EL, Ty> TypedStaticGraphConverter<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
    Ty: GraphType,
{
    pub fn get_graph(&self) -> &TypedStaticGraph<Id, Ty> {
        &self.graph
    }

    pub fn get_original_node_id(&self, new_id: usize) -> Option<usize> {
        self.node_id_map.get_item(new_id).map(|x| x.id())
    }

    pub fn find_new_node_id(&self, old_id: usize) -> Option<usize> {
        self.node_id_map.find_index(&Id::new(old_id))
    }

    pub fn get_node_label(&self, label_id: usize) -> Option<&NL> {
        self.node_label_map.get_item(label_id)
    }

    pub fn get_edge_label(&self, label_id: usize) -> Option<&EL> {
        self.edge_label_map.get_item(label_id)
    }

    pub fn find_node_label_index(&self, label: &NL) -> Option<usize> {
        self.node_label_map.find_index(&label)
    }

    pub fn find_edge_label_index(&self, label: &EL) -> Option<usize> {
        self.edge_label_map.find_index(&label)
    }

    pub fn get_node_label_map(&self) -> &SetMap<NL> {
        &self.node_label_map
    }

    pub fn get_edge_label_map(&self) -> &SetMap<EL> {
        &self.edge_label_map
    }
}

impl<Id, NL, EL> TypedDiStaticGraphConverter<Id, NL, EL>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
{
    pub fn new(g: &TypedDiGraphMap<Id, NL, EL>) -> Self {
        let node_id_map = _get_node_id_map(g);
        let node_label_map = _get_node_label_id_map(g);
        let edge_label_map = _get_edge_label_id_map(g);

        let edge_vec = _get_edge_vec(g, &node_id_map, &edge_label_map);
        let node_labels = _get_node_labels(g, &node_id_map, &node_label_map);

        let in_edge_vec = Some(_get_in_edge_vec(g, &node_id_map));

        let graph = match node_labels {
            Some(labels) => {
                TypedDiStaticGraph::with_labels(g.node_count(), edge_vec, in_edge_vec, labels)
            }
            None => TypedDiStaticGraph::new(g.node_count(), edge_vec, in_edge_vec),
        };

        let node_label_map = _merge_map(&node_label_map, g.get_node_label_map());
        let edge_label_map = _merge_map(&edge_label_map, g.get_edge_label_map());

        TypedDiStaticGraphConverter {
            graph,
            node_id_map,
            node_label_map,
            edge_label_map,
        }
    }
}

impl<Id, NL, EL> TypedUnStaticGraphConverter<Id, NL, EL>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
{
    pub fn new(g: &TypedUnGraphMap<Id, NL, EL>) -> Self {
        let node_id_map = _get_node_id_map(g);
        let node_label_map = _get_node_label_id_map(g);
        let edge_label_map = _get_edge_label_id_map(g);

        let edge_vec = _get_edge_vec(g, &node_id_map, &edge_label_map);
        let node_labels = _get_node_labels(g, &node_id_map, &node_label_map);

        let in_edge_vec = None;

        let graph = match node_labels {
            Some(labels) => {
                TypedUnStaticGraph::with_labels(g.node_count(), edge_vec, in_edge_vec, labels)
            }
            None => TypedUnStaticGraph::new(g.node_count(), edge_vec, in_edge_vec),
        };

        //        let node_id_map = VecMap::from(node_id_map);
        let node_label_map = _merge_map(&node_label_map, g.get_node_label_map());
        let edge_label_map = _merge_map(&edge_label_map, g.get_edge_label_map());

        TypedUnStaticGraphConverter {
            graph,
            node_id_map,
            node_label_map,
            edge_label_map,
        }
    }
}

/// Map node id to a continuous range (sort by degree)
fn _get_node_id_map<Id, NL, EL, Ty>(g: &TypedGraphMap<Id, NL, EL, Ty>) -> SetMap<Id>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
    Ty: GraphType,
{
    let mut node_degree: Vec<_> = g.nodes().map(|n| (n.get_id(), n.degree())).collect();
    node_degree.sort_unstable_by_key(|&(_, d)| d);

    let mut node_id_map = SetMap::new();
    for (n, _) in node_degree {
        node_id_map.add_item(n);
    }
    node_id_map
}

/// Re-assign node label id sorted by its frequency
fn _get_node_label_id_map<Id, NL, EL, Ty>(g: &TypedGraphMap<Id, NL, EL, Ty>) -> SetMap<Id>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
    Ty: GraphType,
{
    let mut label_counter: Vec<_> = g.get_node_label_id_counter()
        .into_iter()
        .filter(|&(_, f)| f > 0)
        .collect();
    label_counter.sort_unstable_by_key(|&(_, f)| f);

    let mut label_map = SetMap::new();
    for (n, _) in label_counter {
        label_map.add_item(n);
    }
    label_map
}

/// Re-assign edge label id sorted by its frequency
fn _get_edge_label_id_map<Id, NL, EL, Ty>(g: &TypedGraphMap<Id, NL, EL, Ty>) -> SetMap<Id>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
    Ty: GraphType,
{
    let mut label_counter: Vec<_> = g.get_edge_label_id_counter()
        .into_iter()
        .filter(|&(_, f)| f > 0)
        .collect();
    label_counter.sort_unstable_by_key(|&(_, f)| f);

    let mut label_map = SetMap::new();
    for (n, _) in label_counter {
        label_map.add_item(n);
    }
    label_map
}

fn _merge_map<Id, L>(new_map: &SetMap<Id>, old_map: &SetMap<L>) -> SetMap<L>
where
    Id: IdType,
    L: Hash + Eq + Clone,
{
    let mut merged = SetMap::<L>::new();

    for i in new_map.items() {
        let item = old_map.get_item(i.id()).unwrap().clone();
        merged.add_item(item);
    }

    merged
}

/// Convert node labels into a `Vec`
fn _get_node_labels<Id, NL, EL, Ty>(
    g: &TypedGraphMap<Id, NL, EL, Ty>,
    node_map: &SetMap<Id>,
    label_map: &SetMap<Id>,
) -> Option<Vec<Id>>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
    Ty: GraphType,
{
    g.node_labels().next()?;
    //    if g.node_labels().next().is_none() {
    //        return None;
    //    }

    let mut labels = Vec::with_capacity(g.node_count());

    for node_id in node_map.items() {
        labels.push(match g.get_node(*node_id).unwrap().get_label_id() {
            Some(label) => Id::new(label_map.find_index(&label).unwrap()),
            None => Id::max_value(),
        });
    }

    Some(labels)
}

/// Convert edges into `EdgeVec`
fn _get_edge_vec<Id, NL, EL, Ty>(
    g: &TypedGraphMap<Id, NL, EL, Ty>,
    node_map: &SetMap<Id>,
    label_map: &SetMap<Id>,
) -> EdgeVec<Id>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
    Ty: GraphType,
{
    let has_edge_label = g.edge_labels().next().is_some();
    let offset_len = g.node_count() + 1;
    let edge_len = if g.is_directed() {
        g.edge_count()
    } else {
        2 * g.edge_count()
    };

    let mut offset = 0;
    let mut offset_vec = Vec::with_capacity(offset_len);
    let mut edge_vec = Vec::with_capacity(edge_len);

    let mut edge_labels = if has_edge_label {
        Some(Vec::with_capacity(edge_len))
    } else {
        None
    };

    for node_id in node_map.items() {
        offset_vec.push(Id::new(offset));

        let mut neighbors: Vec<_> = g.neighbors_iter(*node_id)
            .map(|i| node_map.find_index(&i).unwrap())
            .collect();

        neighbors.sort_unstable();
        offset += neighbors.len();

        for neighbor in neighbors {
            edge_vec.push(Id::new(neighbor));

            if let Some(ref mut labels) = edge_labels {
                let original_node = node_map.get_item(neighbor).unwrap();

                labels.push(
                    match g.get_edge(*node_id, *original_node).unwrap().get_label_id() {
                        Some(label) => Id::new(label_map.find_index(&label).unwrap()),
                        None => Id::max_value(),
                    },
                );
            }
        }
    }

    offset_vec.push(Id::new(edge_len));

    match edge_labels {
        Some(labels) => EdgeVec::with_labels(offset_vec, edge_vec, labels),
        None => EdgeVec::new(offset_vec, edge_vec),
    }
}

/// Convert in-edges into `EdgeVec` (edge labels will be ignored)
fn _get_in_edge_vec<Id, NL, EL>(
    g: &TypedDiGraphMap<Id, NL, EL>,
    node_map: &SetMap<Id>,
) -> EdgeVec<Id>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
{
    let offset_len = g.node_count() + 1;
    let edge_len = g.edge_count();

    let mut offset = 0;
    let mut offset_vec = Vec::with_capacity(offset_len);
    let mut edge_vec = Vec::with_capacity(edge_len);

    for node_id in node_map.items() {
        offset_vec.push(Id::new(offset));

        let mut neighbors: Vec<_> = g.in_neighbors_iter(*node_id)
            .map(|i| node_map.find_index(&i).unwrap())
            .collect();

        neighbors.sort_unstable();
        offset += neighbors.len();

        for neighbor in neighbors {
            edge_vec.push(Id::new(neighbor));
        }
    }

    offset_vec.push(Id::new(edge_len));

    EdgeVec::new(offset_vec, edge_vec)
}
