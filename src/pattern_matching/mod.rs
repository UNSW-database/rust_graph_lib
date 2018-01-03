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
    fn get_candidates(&self, node: usize) -> &[usize];

    /// Get the number of candidates of a given node.
    fn get_num_candidates(&self, node: usize) -> usize {
        self.get_candidates(node).len()
    }

    /// Compute the candidate for all pattern nodes
    fn compute(&mut self);
}


pub trait CandidateConstraint {
    /// A filter function defines whether a pattern node can be potentially matched
    /// to a data node.
    ///
    /// # Arguments
    ///
    /// * `p_node` - The id of the pattern node.
    /// * `d_node` - The id of the data node.
    ///
    /// # Return value
    ///
    /// `true` if the pattern node can be matched.
    fn filter(&self, p_node: usize, d_node: usize) -> bool;
}

pub trait PatternMatchTrait<G, L>
    where G: GraphTrait<L> {

    fn set_pattern_graph(&mut self, graph: &G);

    fn set_data_graph(&mut self, graph: &G);

    /// Get the starting node for pattern matching
    fn get_start_node(&self) -> usize;

    /// Given a `pattern_node` and a `matched_node`, compute a
    /// matching order, which is a permutation of all the pattern nodes.
    fn compute_matching_order(
        &self, pattern_node: usize, matched_node: usize) -> Vec<usize>;

    /// In case that we match a `pattern_node` (as a start_node) to a given `data_node`,
    /// we compute all matches correspondingly. Suppose the start node `v` has `k` matches,
    /// `u[1], ..., u[k]`, we can call `compute_matching_from(v, u[i], orders)` in `k`
    /// different threads (processes, machines). This function only requires you to
    /// return the number of matches, but should consider how to consume the results.
    fn compute_matching_from(
        &self, pattern_node: usize, matched_node: usize, matching_order: &[usize]) -> usize;

}