use generic::GraphTrait;

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

    /// Given a pattern graph `pattern` and data graph `data`,
    /// compute the candidate set for all pattern nodes
    fn compute<'a, L, G: GraphTrait<L> + 'a>(&mut self, pattern: &'a G, data: &'a G);
}


pub trait CandidateConstraint {
    /// A filter function defines whether a pattern node can be potentially matched
    /// to a data node.
    ///
    /// # Arguments
    ///
    /// * `p_node` - The id of the pattern node.
    /// * `d_node` - The id of the data node.
    /// * `pattern` - The pattern graph to match.
    /// * `data` - The data graph to match.
    ///
    /// # Return value
    ///
    /// `true` if the pattern node can be matched.
    fn filter<'a, L, G: GraphTrait<L> + 'a>(&self, p_node: usize, d_node: usize,
        pattern: &'a G, data: &'a G) -> bool;
}

pub trait PatternMatchTrait<G, L>
    where G: GraphTrait<L> {

    /// To apply a new pattern graph for matching.
    fn set_pattern_graph(&mut self, graph: G);

    /// Get the starting node for pattern matching
    fn get_start_node(&self) -> usize;

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
        &self, start_node: usize, matched_node: usize, candidates: &C) -> Vec<usize>;

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
        &self, start_node: usize, matched_node: usize,
        matching_order: &[usize], candidates: &C) -> usize;

}
