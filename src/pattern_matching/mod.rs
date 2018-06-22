use generic::GraphTrait;
use generic::IdType;

/// `CandidateTrait` maintains the commonly used functions for a Candidate structure that
/// computes the candidate sets of all pattern nodes. We say a data node is a candidate
/// node of a given pattern node, if, by following specific rules, it can match that
/// pattern node. A candidate set of a given pattern node, is a set of candidate nodes
/// regarding that pattern node.
pub trait CandidateTrait {
    /// Get the candidate set of a given node.
    ///
    /// # Arguments
    ///
    /// * `node` - The id of the pattern node.
    ///
    fn get_cands(&self, node: usize) -> &[usize];

    /// Get the number of candidates of a given node.
    fn num_cands(&self, node: usize) -> usize {
        self.get_cands(node).len()
    }

    /// Compute the candidate set for all pattern nodes
    fn compute(&mut self);
}

/// Use to compute the *initial candidate* set for each pattern node.
/// Given a pattern node v, there are multiple strategies to use:
///
/// * By label: A candidate node must have the same label as v.
///
/// * By degree: A candidate node u must have `d{G}(u) >= d{P}(v)`,
///    where `d{g}(*)` means the degree of `*` in `g`.
///
/// * By neighbour's label (NLF): A candidate node u must have each |N{G,l}(u)| > |N{P,l}(v)|, where
///    N{g,l}(u) means the neighbors of `u` in `g` that have label `l`.
///
pub trait CandidateConstraint<Id: IdType> {
    /// A filter function defines whether a pattern node can be potentially matched
    /// to a data node.
    ///
    /// # Arguments
    ///
    /// * `p_node` - The id of the pattern node.
    /// * `d_node` - The id of the data node.
    ///
    /// # Example
    ///
    /// ```
    /// use std::marker::PhantomData;
    ///
    /// use rust_graph::generic::IdType;
    /// use rust_graph::generic::GraphTrait;
    /// use rust_graph::pattern_matching::CandidateConstraint;
    ///
    /// struct LabelConstraint<'a, Id, G>
    /// where
    ///     Id: IdType,
    ///     G: 'a + GraphTrait<Id>,
    /// {
    ///     data: &'a G,
    ///     pattern: &'a G,
    ///     _marker: PhantomData<Id>,
    /// }
    ///
    /// impl<'a, Id: IdType, G: GraphTrait<Id>> CandidateConstraint<Id> for LabelConstraint<'a, Id, G> {
    ///     fn filter(&self, p_node: Id, d_node: Id) -> bool {
    ///         let p_label_opt = self.pattern.get_node_label_id(p_node);
    ///         let d_label_opt = self.data.get_node_label_id(d_node);
    ///
    ///         match (p_label_opt, d_label_opt) {
    ///             (None, None) => true,
    ///             (Some(p_label), Some(d_label)) => p_label == d_label,
    ///             _ => false,
    ///         }
    ///     }
    /// }
    ///
    /// ```
    ///
    /// While calling the functions, uppose, `v` is the pattern node that we are computing the
    /// candidate set. `v`'s candidate will be given by: `g.get_node_indices().filter(|x| CandidateConstraint.filter(v, x))`.
    ///
    /// # Return value
    ///
    /// `true` if the `p_node` can be matched to `d_node` following the given strategy.
    ///
    fn filter(&self, p_node: Id, d_node: Id) -> bool;
}

pub trait PatternMatchTrait<Id, G, L>
where
    Id: IdType,
    G: GraphTrait<Id>,
{
    /// To apply a new pattern graph for matching.
    fn set_pattern_graph(&mut self, graph: G);

    /// Get the starting node for pattern matching
    fn get_start_node(&self) -> Id;

    /// Given a `start_node` and a `matched_node`, compute a matching order,
    /// which is a permutation of pattern nodes, indicating a dfs traversal order
    /// while processing pattern matching. Note that, in recent technique like TurboIso,
    /// or CFLMatch, the matching order may be different after matching the start_node
    /// to different candidate nodes.
    ///
    /// # Arguments
    ///
    /// * `start_node` - The starting node for pattern matching, from `get_start_node`.
    /// * `matched_node` - The candidate nodes that can be matched to `start_node`.
    /// * `candidates` - The candidate set of all pattern nodes.
    ///
    /// # Return value
    ///
    /// A matching order regarding `start_node` and `matched_node`.
    ///
    fn compute_matching_order<C: CandidateTrait>(
        &self,
        start_node: usize,
        matched_node: usize,
        candidates: &C,
    ) -> Vec<usize>;

    /// In case that we match a `pattern_node` (as a start_node) to a given `data_node`,
    /// we compute all matches correspondingly. Suppose the start node `v` has `k` matches,
    /// `u[1], ..., u[k]`, we can call `compute_matching_from(v, u[i], orders)` in `k`
    /// different threads (processes, machines). This function only requires you to
    ///
    /// # Arguments
    ///
    /// * `start_node` - The pattern node to start the match, from `get_start_node()`
    /// * `matched_node` - The data node that matches the `start_node`.
    /// * `matching_order` - The matching order, from `compute_matching_order`.
    /// * `candidates` - The candidate set of all nodes.
    ///
    /// # Return value
    ///
    /// The number of matches. Note: Please consider how to consume the results.
    ///
    fn compute_matching_from<C: CandidateTrait>(
        &self,
        start_node: usize,
        matched_node: usize,
        matching_order: &[usize],
        candidates: &C,
    ) -> usize;
}
