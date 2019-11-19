use generic::{GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hash};
use std::iter::FromIterator;
use std::rc::Rc;

#[derive(Clone)]
pub enum OpType {
    Base,
    Sink,
}

#[derive(Clone)]
pub struct BaseOperator {
    pub name: String,
    pub op_type: OpType,
    pub next: Option<Vec<BaseOperator>>,
    pub prev: Option<Rc<BaseOperator>>,
    pub probe_tuple: Vec<usize>,
    pub out_tuple_len: usize,
    pub in_subgraph: Option<Rc<QueryGraph>>,
    pub out_subgraph: Rc<QueryGraph>,
    pub out_qvertex_to_idx_map: HashMap<String, usize>,
    pub last_repeated_vertex_idx: usize,
    pub num_out_tuples: usize,
    pub icost: usize,
}

impl BaseOperator {
    pub fn new(out_subgraph: Rc<QueryGraph>, in_subgraph: Option<Rc<QueryGraph>>) -> BaseOperator {
        BaseOperator {
            name: "".to_string(),
            op_type: OpType::Base,
            next: None,
            prev: None,
            probe_tuple: vec![],
            out_tuple_len: out_subgraph.get_num_qvertices(),
            in_subgraph,
            out_subgraph,
            out_qvertex_to_idx_map: HashMap::new(),
            last_repeated_vertex_idx: 0,
            num_out_tuples: 0,
            icost: 0,
        }
    }

    pub fn copy(&self, is_thread_safe: bool) -> Option<Rc<BaseOperator>> {
        unimplemented!();
    }
    pub fn copy_default(&self) -> Option<Rc<BaseOperator>> {
        self.copy(false)
    }
}

pub trait Operator {
    fn get_op(&self) -> BaseOperator;
    fn get_name(&self) -> String;
    fn get_icost(&self) -> usize;
    fn get_type(&self) -> OpType;
    fn get_num_out_tuples(&self) -> usize;
    fn get_query_vertex_id_map(&self) -> &HashMap<String, usize>;
    fn get_out_query_vertices(&self) -> Vec<String> {
        self.get_query_vertex_id_map()
            .iter()
            .map(|(key, _val)| key.clone())
            .collect()
    }
    fn init<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<usize>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    );
    fn is_same_as(&self, op: &BaseOperator) -> bool;
    fn get_next_vec(&self) -> &Option<Vec<BaseOperator>>;
    fn get_next_vec_as_mut(&mut self) -> &mut Option<Vec<BaseOperator>>;
    fn get_prev(&self) -> Option<&Rc<BaseOperator>>;
    fn get_next(&self, index: usize) -> &BaseOperator {
        let next = self.get_next_vec().as_ref().unwrap();
        &next[index]
    }
    fn set_next(&mut self, op: BaseOperator) {
        self.get_next_vec_as_mut().replace(vec![op]);
    }
    fn set_next_vec(&mut self, op: Vec<BaseOperator>) {
        self.get_next_vec_as_mut().replace(op);
    }
    fn set_next_batch(&mut self, ops: Vec<BaseOperator>);
    fn process_new_tuple();
    fn execute(&self) {
        if let Some(prev) = self.get_prev() {
            prev.execute();
        }
    }
    fn get_alds_as_string() -> String {
        String::from("")
    }
    fn update_operator_name(query_vertex_to_index_map: HashMap<String, usize>);
    fn get_operator_metrics_next_operators(
        &self,
        operator_metrics: &mut Vec<(String, usize, usize)>,
    ) {
        operator_metrics.push((self.get_name(), self.get_icost(), self.get_num_out_tuples()));
        if let Some(next) = self.get_next_vec() {
            next.iter()
                .filter(|op| {
                    if let OpType::Sink = op.get_type() {
                        return true;
                    }
                    false
                })
                .for_each(|op| {
                    op.get_operator_metrics_next_operators(operator_metrics);
                });
        }
    }

    fn get_last_operators(&self, last_operators: &mut Vec<BaseOperator>) {
        if let Some(next) = self.get_next_vec() {
            for op in next {
                op.get_last_operators(last_operators);
            }
        } else {
            last_operators.push(self.get_op());
        }
    }
    fn has_multi_edge_extends(&self) -> bool {
        if let Some(prev) = self.get_prev() {
            return prev.has_multi_edge_extends();
        }
        return false;
    }
}

impl Operator for BaseOperator {
    fn get_op(&self) -> BaseOperator {
        self.clone()
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn get_icost(&self) -> usize {
        self.icost
    }

    fn get_type(&self) -> OpType {
        self.op_type.clone()
    }

    fn get_num_out_tuples(&self) -> usize {
        self.num_out_tuples
    }

    fn get_query_vertex_id_map(&self) -> &HashMap<String, usize> {
        &self.out_qvertex_to_idx_map
    }

    fn init<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<usize>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        panic!("`init()` on `BaseOperator`")
    }

    fn is_same_as(&self, op: &BaseOperator) -> bool {
        panic!("`is_same_as()` on `BaseOperator`")
    }

    fn get_next_vec(&self) -> &Option<Vec<BaseOperator>> {
        &self.next
    }

    fn get_next_vec_as_mut(&mut self) -> &mut Option<Vec<BaseOperator>> {
        &mut self.next
    }

    fn get_prev(&self) -> Option<&Rc<BaseOperator>> {
        self.prev.as_ref()
    }

    fn set_next_batch(&mut self, ops: Vec<BaseOperator>) {
        self.next = Some(ops);
    }

    fn process_new_tuple() {
        unimplemented!()
    }

    fn update_operator_name(query_vertex_to_index_map: HashMap<String, usize>) {
        panic!("`update_operator_name()` on `BaseOperator`")
    }
}
