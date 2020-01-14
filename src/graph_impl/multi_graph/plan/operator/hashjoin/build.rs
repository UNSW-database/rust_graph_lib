use generic::{GraphTrait, GraphType, IdType};
use graph_impl::multi_graph::plan::operator::hashjoin::hash_table::HashTable;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};

#[derive(Clone)]
pub struct Build<Id: IdType> {
    pub base_op: BaseOperator<Id>,
    pub hash_table: Option<HashTable<Id>>,
    pub probing_subgraph: Option<Box<QueryGraph>>,
    query_vertex_to_hash: String,
    pub build_hash_idx: usize,
    pub hashed_tuple_len: usize,
}

impl<Id: IdType> Build<Id> {
    pub fn new(
        in_subgraph: Box<QueryGraph>,
        query_vertex_to_hash: String,
        build_hash_idx: usize,
    ) -> Build<Id> {
        let mut build = Build {
            base_op: BaseOperator::empty(),
            hash_table: None,
            probing_subgraph: None,
            query_vertex_to_hash: query_vertex_to_hash.clone(),
            build_hash_idx,
            hashed_tuple_len: 0,
        };
        build.hashed_tuple_len = in_subgraph.get_num_qvertices() - 1;
        build.base_op.out_tuple_len = in_subgraph.get_num_qvertices();
        build.base_op.in_subgraph = Some(in_subgraph);
        build.base_op.name = "HASH ON (".to_owned() + &query_vertex_to_hash + ")";
        build
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Build<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        if self.base_op.probe_tuple.len() == 0 {
            self.base_op.probe_tuple = probe_tuple;
            self.hash_table.as_mut().map(|table| {
                table.allocate_initial_memory(graph.node_count() + 1);
            });
        }
    }

    fn process_new_tuple(&mut self) {
        let probe_tuple = self.base_op.probe_tuple.clone();
        self.hash_table.as_mut().map(|table| {
            table.insert_tuple(probe_tuple);
        });
    }

    fn execute(&mut self) {
        self.base_op.execute()
    }

    fn get_alds_as_string(&self) -> String {
        self.base_op.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_op.update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let mut build = Build::new(
            self.base_op.in_subgraph.as_ref().unwrap().clone(),
            self.query_vertex_to_hash.clone(),
            self.build_hash_idx,
        );
        build.base_op.prev = self.base_op.prev.as_ref().map(|prev| prev.clone());
        build.base_op.next = vec![Operator::Build(build.clone())];
        build.probing_subgraph = self.probing_subgraph.clone();
        Operator::Build(build)
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        if let Operator::Build(build) = op {
            let base_op = &mut self.base_op;
            let in_subgraph = base_op.in_subgraph.as_mut().map_or(false, |in_subgraph| {
                in_subgraph.is_isomorphic_to(build.base_op.in_subgraph.as_mut().unwrap())
            });
            let prev = base_op.prev.as_mut().map_or(false, |prev| {
                prev.is_same_as(build.base_op.prev.as_mut().unwrap().as_mut())
            });
            return in_subgraph && prev;
        }
        false
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_op.get_num_out_tuples()
    }
}
