use std::hash::Hash;

use generic::node::NodeMapTrait;
use generic::Iter;
use generic::{DefaultId, IdType};
use generic::{DefaultTy, Directed, GraphType, Undirected};
use generic::{DiGraphTrait, GraphLabelTrait, GraphTrait};
use generic::{EdgeTrait, NodeTrait};
use generic::{MapTrait, MutMapTrait};

use graph_impl::static_graph::EdgeVec;
use graph_impl::{TypedDiStaticGraph, TypedGraphMap, TypedUnStaticGraph};

use map::SetMap;

pub type TypedDiStaticGraphConverter<Id, NL, EL> = TypedStaticGraphConverter<Id, NL, EL, Directed>;
pub type TypedUnStaticGraphConverter<Id, NL, EL> =
    TypedStaticGraphConverter<Id, NL, EL, Undirected>;
pub type StaticGraphConverter<NL, EL, Ty = DefaultTy> =
    TypedStaticGraphConverter<DefaultId, NL, EL, Ty>;
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
    graphmap: TypedGraphMap<Id, NL, EL, Ty>,
    reorder_node_id: bool,
    reorder_label_id: bool,
    node_id_map: Option<SetMap<Id>>,
    node_label_id_map: Option<SetMap<Id>>,
    edge_label_id_map: Option<SetMap<Id>>,
    node_label_map: Option<SetMap<NL>>,
    edge_label_map: Option<SetMap<EL>>,
}

impl<Id, NL, EL, Ty> TypedStaticGraphConverter<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
    Ty: GraphType,
{
    pub fn new(
        g: TypedGraphMap<Id, NL, EL, Ty>,
        reorder_node_id: bool,
        reorder_label_id: bool,
    ) -> Self {
        let mut converter = TypedStaticGraphConverter {
            graphmap: g,
            reorder_node_id,
            reorder_label_id,
            node_id_map: None,
            node_label_id_map: None,
            edge_label_id_map: None,
            node_label_map: None,
            edge_label_map: None,
        };

        if reorder_node_id {
            let node_id_map = converter.reorder_node_id_map();
            converter.set_node_id_map(Some(node_id_map));
        } else {
            let max_node_id = converter.get_graphmap().node_indices().max().unwrap().id();
            let num_of_nodes = converter.get_graphmap().node_count();
            assert_eq!(max_node_id + 1, num_of_nodes);
        }

        if reorder_label_id {
            let node_label_id_map = converter.reorder_node_label_id_map();
            converter.set_node_label_id_map(Some(node_label_id_map));

            let edge_label_id_map = converter.reorder_edge_label_id_map();
            converter.set_edge_label_id_map(Some(edge_label_id_map));
        }

        converter
    }

    //    pub fn with_node_label_map(mut self, node_label_map: SetMap<NL>) -> Self {
    //        let old_node_label_map = self.get_graphmap().get_node_label_map();
    //        let node_label_id_map = _convert_map(old_node_label_map.clone(), node_label_map);
    //        self.set_node_label_id_map(Some(node_label_id_map));
    //
    //        self
    //    }
    //
    //    pub fn with_edge_label_map(mut self, edge_label_map: SetMap<EL>) -> Self {
    //        let old_edge_label_map = self.get_graphmap().get_edge_label_map().clone();
    //        let node_label_id_map = _convert_map(old_edge_label_map, edge_label_map);
    //        self.set_edge_label_map(Some(edge_label_map));
    //
    //        self
    //    }

    pub fn get_graphmap(&self) -> &TypedGraphMap<Id, NL, EL, Ty> {
        &self.graphmap
    }

    pub fn to_graphmap(self) -> TypedGraphMap<Id, NL, EL, Ty> {
        self.graphmap
    }

    pub fn clean_graphmap(&mut self) {
        self.graphmap = TypedGraphMap::new();
    }

    pub fn get_node_id_map(&self) -> &Option<SetMap<Id>> {
        &self.node_id_map
    }

    pub fn get_node_label_id_map(&self) -> &Option<SetMap<Id>> {
        &self.node_label_id_map
    }

    pub fn get_edge_label_id_map(&self) -> &Option<SetMap<Id>> {
        &self.edge_label_id_map
    }

    pub fn get_node_label_map(&self) -> &Option<SetMap<NL>> {
        &self.node_label_map
    }

    pub fn get_edge_label_map(&self) -> &Option<SetMap<EL>> {
        &self.edge_label_map
    }

    pub fn get_original_node_id(&self, id: Id) -> Id {
        match self.get_node_id_map() {
            Some(map) => map.get_item(id.id()).unwrap().clone(),
            None => id,
        }
    }

    pub fn find_new_node_id(&self, id: Id) -> Id {
        match self.get_node_id_map() {
            Some(map) => Id::new(map.find_index(&id).unwrap()),
            None => id,
        }
    }

    pub fn find_new_node_label_id(&self, id: Id) -> Id {
        match self.get_node_label_id_map() {
            Some(map) => Id::new(map.find_index(&id).unwrap()),
            None => id,
        }
    }

    pub fn find_new_edge_label_id(&self, id: Id) -> Id {
        match self.get_edge_label_id_map() {
            Some(map) => Id::new(map.find_index(&id).unwrap()),
            None => id,
        }
    }

    fn set_node_id_map(&mut self, node_id_map: Option<SetMap<Id>>) {
        self.node_id_map = node_id_map;
    }

    fn set_node_label_id_map(&mut self, node_label_id_map: Option<SetMap<Id>>) {
        self.node_label_id_map = node_label_id_map;
    }

    fn set_edge_label_id_map(&mut self, edge_label_id_map: Option<SetMap<Id>>) {
        self.edge_label_id_map = edge_label_id_map;
    }

    fn set_node_label_map(&mut self, node_label_map: Option<SetMap<NL>>) {
        self.node_label_map = node_label_map;
    }
    fn set_edge_label_map(&mut self, edge_label_map: Option<SetMap<EL>>) {
        self.edge_label_map = edge_label_map;
    }

    /// Map node id to a continuous range (sort by degree)
    fn reorder_node_id_map(&self) -> SetMap<Id> {
        let mut node_degree: Vec<_> = self.get_graphmap()
            .nodes()
            .map(|n| n.unwrap_nodemap())
            .map(|n| (n.get_id(), n.degree()))
            .collect();
        node_degree.sort_unstable_by_key(|&(_, d)| d);

        node_degree.into_iter().map(|(n, _)| n).collect()
    }

    /// Re-assign node label id sorted by its frequency
    fn reorder_node_label_id_map(&self) -> SetMap<Id> {
        let mut label_counter: Vec<_> = self.get_graphmap()
            .get_node_label_id_counter()
            .into_iter()
            .filter(|&(_, f)| f > 0)
            .collect();
        label_counter.sort_unstable_by_key(|&(_, f)| f);

        label_counter.into_iter().map(|(n, _)| n).collect()
    }

    /// Re-assign edge label id sorted by its frequency
    fn reorder_edge_label_id_map(&self) -> SetMap<Id> {
        let mut label_counter: Vec<_> = self.get_graphmap()
            .get_edge_label_id_counter()
            .into_iter()
            .filter(|&(_, f)| f > 0)
            .collect();
        label_counter.sort_unstable_by_key(|&(_, f)| f);

        label_counter.into_iter().map(|(n, _)| n).collect()
    }

    fn ids(&self) -> Iter<Id> {
        match self.get_node_id_map() {
            Some(map) => Iter::new(Box::new(map.items().cloned())),
            None => Iter::new(Box::new((0..self.get_graphmap().node_count()).map(Id::new))),
        }
    }
}

impl<Id, NL, EL, Ty> TypedStaticGraphConverter<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    /// Convert node labels into a `Vec`
    fn get_node_label_vec(&self) -> Option<Vec<Id>> {
        let g = self.get_graphmap();

        g.node_labels().next()?;

        let mut labels = Vec::with_capacity(g.node_count());

        for node_id in self.ids() {
            let label = match g.get_node(node_id).unwrap_nodemap().get_label_id() {
                Some(label) => self.find_new_node_label_id(label),
                None => Id::max_value(),
            };

            labels.push(label);
        }

        Some(labels)
    }

    /// Convert edges into `EdgeVec`
    fn get_edge_vec(&self) -> EdgeVec<Id> {
        let g = self.get_graphmap();

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

        for node_id in self.ids() {
            offset_vec.push(offset);

            let mut neighbors: Vec<_> = match self.get_node_id_map() {
                Some(map) => g.neighbors_iter(node_id)
                    .map(|i| Id::new(map.find_index(&i).unwrap()))
                    .collect(),
                None => g.neighbors_iter(node_id).collect(),
            };

            neighbors.sort_unstable();
            offset += neighbors.len();

            for neighbor in neighbors {
                edge_vec.push(neighbor);

                if let Some(ref mut labels) = edge_labels {
                    let original_node = self.get_original_node_id(neighbor);

                    labels.push(match g.get_edge(node_id, original_node).get_label_id() {
                        Some(label) => self.find_new_edge_label_id(label),
                        None => Id::max_value(),
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

    fn compute_new_node_label_map(&self) -> SetMap<NL> {
        match self.get_node_label_id_map() {
            Some(node_label_id_map) => match self.get_node_label_map() {
                Some(node_label_map) => unimplemented!(),
                None => _merge_map(node_label_id_map, self.get_graphmap().get_node_label_map()),
            },
            None => self.get_graphmap().get_node_label_map().clone(),
        }
    }

    fn compute_new_edge_label_map(&self) -> SetMap<EL> {
        match self.get_edge_label_id_map() {
            Some(edge_label_id_map) => match self.get_edge_label_map() {
                Some(edge_label_map) => unimplemented!(),
                None => _merge_map(edge_label_id_map, self.get_graphmap().get_edge_label_map()),
            },
            None => self.get_graphmap().get_edge_label_map().clone(),
        }
    }
}

impl<Id, NL, EL> TypedUnStaticGraphConverter<Id, NL, EL>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
{
    pub fn convert(&self) -> TypedUnStaticGraph<Id, NL, EL> {
        let edge_vec = self.get_edge_vec();
        let node_labels = self.get_node_label_vec();

        let in_edge_vec = None;

        let node_label_map = self.compute_new_node_label_map();
        let edge_label_map = self.compute_new_edge_label_map();

        TypedUnStaticGraph::from_raw(
            self.get_graphmap().node_count(),
            self.get_graphmap().edge_count(),
            edge_vec,
            in_edge_vec,
            node_labels,
            node_label_map,
            edge_label_map,
        )
    }
}

impl<Id, NL, EL> TypedDiStaticGraphConverter<Id, NL, EL>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
{
    pub fn convert(&self) -> TypedDiStaticGraph<Id, NL, EL> {
        let edge_vec = self.get_edge_vec();
        let node_labels = self.get_node_label_vec();

        let in_edge_vec = Some(self.get_in_edge_vec());

        let node_label_map = self.compute_new_node_label_map();
        let edge_label_map = self.compute_new_edge_label_map();

        TypedDiStaticGraph::from_raw(
            self.get_graphmap().node_count(),
            self.get_graphmap().edge_count(),
            edge_vec,
            in_edge_vec,
            node_labels,
            node_label_map,
            edge_label_map,
        )
    }

    /// Convert in-edges into `EdgeVec` (edge labels will be ignored)
    fn get_in_edge_vec(&self) -> EdgeVec<Id> {
        let g = self.get_graphmap();

        let offset_len = g.node_count() + 1;
        let edge_len = g.edge_count();

        let mut offset = 0;
        let mut offset_vec = Vec::with_capacity(offset_len);
        let mut edge_vec = Vec::with_capacity(edge_len);

        for node_id in self.ids() {
            offset_vec.push(offset);

            let mut neighbors: Vec<_> = match self.get_node_id_map() {
                Some(map) => g.in_neighbors_iter(node_id)
                    .map(|i| Id::new(map.find_index(&i).unwrap()))
                    .collect(),
                None => g.neighbors_iter(node_id).collect(),
            };

            neighbors.sort_unstable();
            offset += neighbors.len();

            for neighbor in neighbors {
                edge_vec.push(neighbor);
            }
        }

        offset_vec.push(edge_len);

        EdgeVec::new(offset_vec, edge_vec)
    }
}

fn _convert_map<Id, L>(source_map: SetMap<L>, target_map: SetMap<L>) -> SetMap<Id>
where
    Id: IdType,
    L: Hash + Eq,
{
    assert_eq!(source_map.len(), target_map.len());

    let mut map = SetMap::new();

    for item in target_map.items() {
        map.add_item(Id::new(source_map.find_index(item).unwrap()));
    }

    map
}

fn _merge_map<Id, L>(new_map: &SetMap<Id>, old_map: &SetMap<L>) -> SetMap<L>
where
    Id: IdType,
    L: Hash + Eq + Clone,
{
    let mut merged = SetMap::new();

    for i in new_map.items() {
        let item = old_map.get_item(i.id()).unwrap().clone();
        merged.add_item(item);
    }

    merged
}

//    pub fn with_label_map(
//        g: &TypedDiGraphMap<Id, NL, EL>,
//        node_label_map: SetMap<NL>,
//        edge_label_map: SetMap<EL>,
//    ) -> Self {
//        let node_id_map = _get_node_id_map(g);
//        let node_label_map = _convert_map(g.get_node_label_map().clone(), node_label_map);
//        let edge_label_map = _convert_map(g.get_edge_label_map().clone(), edge_label_map);
//
//        let edge_vec = _get_edge_vec(g, &node_id_map, &edge_label_map);
//        let node_labels = _get_node_labels(g, &node_id_map, &node_label_map);
//
//        let in_edge_vec = Some(_get_in_edge_vec(g, &node_id_map));
//
//        let node_label_map = _merge_map(&node_label_map, g.get_node_label_map());
//        let edge_label_map = _merge_map(&edge_label_map, g.get_edge_label_map());
//
//        let graph = TypedDiStaticGraph::from_raw(
//            g.node_count(),
//            g.edge_count(),
//            edge_vec,
//            in_edge_vec,
//            node_labels,
//            node_label_map,
//            edge_label_map,
//        );
//
//        TypedDiStaticGraphConverter { graph, node_id_map }
//    }
//}
