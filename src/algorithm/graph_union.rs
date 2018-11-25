use std::hash::Hash;
use std::ops::Add;

use generic::dtype::IdType;
use graph_impl::graph_map::{new_general_graphmap, TypedDiGraphMap, TypedUnGraphMap};
use prelude::*;

macro_rules! add_graph {
    ($graph0:ident,$graph1:ident,$graph:ident) => {
        for id in $graph0.node_indices() {
            $graph.add_node(id, $graph0.get_node_label(id).cloned());
        }
        for id in $graph1.node_indices() {
            $graph.add_node(id, $graph1.get_node_label(id).cloned());
        }
        for (src, dst) in $graph0.edge_indices() {
            $graph.add_edge(src, dst, $graph0.get_edge_label(src, dst).cloned());
        }
        for (src, dst) in $graph1.edge_indices() {
            $graph.add_edge(src, dst, $graph1.get_edge_label(src, dst).cloned());
        }
    };
}

/// Graph Union of two graphs, g0 and g1.
///
/// Firstly, nodes and edges from g0 are added to the result graph.
/// Then nodes and edges from g1 are added to the result graph.
///
/// Example:
///
/// ```
/// use rust_graph::algorithm::graph_union;
/// use rust_graph::prelude::*;
/// use rust_graph::graph_impl::DiGraphMap;
///
/// let mut graph0 = DiGraphMap::<u32, u32, u32>::new();
/// graph0.add_node(1, Some(0));
/// graph0.add_node(2, Some(1));
/// graph0.add_edge(1, 2, Some(10));
///
/// let mut graph1 = DiGraphMap::<u32, u32, u32>::new();
/// graph1.add_node(3, Some(2));
/// graph1.add_node(4, Some(3));
/// graph1.add_edge(3, 4, Some(20));
///
/// let result_graph = graph_union(&graph0, &graph1);
///
/// ```
///
pub fn graph_union<
    'a,
    'b,
    'c,
    Id: IdType + 'c,
    NL: Eq + Hash + Clone + 'c,
    EL: Eq + Hash + Clone + 'c,
    L: IdType + 'c,
>(
    graph0: &'a GeneralGraph<Id, NL, EL, L>,
    graph1: &'b GeneralGraph<Id, NL, EL, L>,
) -> Box<GeneralGraph<Id, NL, EL, L> + 'c> {
    let mut result_graph = new_general_graphmap(graph0.is_directed());
    {
        let graph = result_graph.as_mut_graph().unwrap();
        add_graph!(graph0, graph1, graph);
    }
    result_graph
}

/// Trait implementation for boxed general graphs addition.
impl<
        'a,
        Id: IdType + 'a,
        NL: Hash + Eq + Clone + 'a,
        EL: Hash + Eq + Clone + 'a,
        L: IdType + 'a,
    > Add for Box<GeneralGraph<Id, NL, EL, L> + 'a>
{
    type Output = Box<GeneralGraph<Id, NL, EL, L> + 'a>;

    fn add(
        self,
        other: Box<GeneralGraph<Id, NL, EL, L> + 'a>,
    ) -> Box<GeneralGraph<Id, NL, EL, L> + 'a> {
        graph_union(self.as_ref(), other.as_ref())
    }
}

/// Trait implementation for general graphs addition.
impl<'a, Id: IdType, NL: Hash + Eq + Clone, EL: Hash + Eq + Clone, L: IdType> Add
    for &'a GeneralGraph<Id, NL, EL, L>
{
    type Output = Box<GeneralGraph<Id, NL, EL, L> + 'a>;

    fn add(self, other: &'a GeneralGraph<Id, NL, EL, L>) -> Box<GeneralGraph<Id, NL, EL, L> + 'a> {
        graph_union(self, other)
    }
}

/// Trait implementation for TypedDiGraphMap addition.
impl<Id: IdType, NL: Hash + Eq + Clone, EL: Hash + Eq + Clone, L: IdType> Add
    for TypedDiGraphMap<Id, NL, EL, L>
{
    type Output = TypedDiGraphMap<Id, NL, EL, L>;

    fn add(self, other: TypedDiGraphMap<Id, NL, EL, L>) -> TypedDiGraphMap<Id, NL, EL, L> {
        let mut graph = TypedDiGraphMap::new();
        add_graph!(self, other, graph);
        graph
    }
}

/// Trait implementation for TypedUnGraphMap addition.
impl<Id: IdType, NL: Hash + Eq + Clone, EL: Hash + Eq + Clone, L: IdType> Add
    for TypedUnGraphMap<Id, NL, EL, L>
{
    type Output = TypedUnGraphMap<Id, NL, EL, L>;

    fn add(self, other: TypedUnGraphMap<Id, NL, EL, L>) -> TypedUnGraphMap<Id, NL, EL, L> {
        let mut graph = TypedUnGraphMap::new();
        add_graph!(self, other, graph);
        graph
    }
}

/// Trait implementation for boxed TypedDiGraphMap addition.
impl<Id: IdType, NL: Hash + Eq + Clone, EL: Hash + Eq + Clone, L: IdType> Add
    for Box<TypedDiGraphMap<Id, NL, EL, L>>
{
    type Output = TypedDiGraphMap<Id, NL, EL, L>;

    fn add(self, other: Box<TypedDiGraphMap<Id, NL, EL, L>>) -> TypedDiGraphMap<Id, NL, EL, L> {
        let mut graph = TypedDiGraphMap::new();
        add_graph!(self, other, graph);
        graph
    }
}

/// Trait implementation for boxed TypedUnGraphMap addition.
impl<Id: IdType, NL: Hash + Eq + Clone, EL: Hash + Eq + Clone, L: IdType> Add
    for Box<TypedUnGraphMap<Id, NL, EL, L>>
{
    type Output = TypedUnGraphMap<Id, NL, EL, L>;

    fn add(self, other: Box<TypedUnGraphMap<Id, NL, EL, L>>) -> TypedUnGraphMap<Id, NL, EL, L> {
        let mut graph = TypedUnGraphMap::new();
        add_graph!(self, other, graph);
        graph
    }
}
