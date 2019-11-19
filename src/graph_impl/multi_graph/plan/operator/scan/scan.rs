use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::operator::BaseOperator;
use graph_impl::static_graph::sorted_adj_vec::SortedAdjVec;
use std::rc::Rc;

//TODO: Fixing Scan Operator
pub struct Scan {
    pub base_op: BaseOperator,
    from_query_vertex: String,
    to_query_vertex: String,
    from_type: usize,
    to_type: usize,
    label_or_to_type: usize,
    fwd_adj_list: Vec<SortedAdjVec<usize>>,
    vertex_ids: Vec<usize>,
    vertex_types: Vec<usize>,
    from_vertex_start_idx: usize,
    from_vertex_end_idx: usize,
}

impl Scan {
    //TODO: fixing functions
    pub fn new(out_subgraph: Rc<QueryGraph>) -> Scan {
        Scan {
            base_op: BaseOperator::new(out_subgraph, None),
            from_query_vertex: "".to_string(),
            to_query_vertex: "".to_string(),
            from_type: 0,
            to_type: 0,
            label_or_to_type: 0,
            fwd_adj_list: vec![],
            vertex_ids: vec![],
            vertex_types: vec![],
            from_vertex_start_idx: 0,
            from_vertex_end_idx: 0,
        }
    }
}
