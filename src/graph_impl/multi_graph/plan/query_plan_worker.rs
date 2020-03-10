use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::intersect::BaseIntersect;
use graph_impl::multi_graph::plan::operator::extend::intersect::Intersect;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::Operator;
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::scan::scan_blocking::VertexIdxLimits;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::plan::query_plan::QueryPlan;
use graph_impl::TypedStaticGraph;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::time::SystemTime;

pub struct QPWorkers<Id: IdType> {
    query_plans: Vec<QueryPlan<Id>>,
    elapsed_time: u128,
    intersection_cost: usize,
    num_intermediate_tuples: usize,
    num_out_tuples: usize,
    operator_metrics: Vec<(String, usize, usize)>,
}

impl<Id: IdType> QPWorkers<Id> {
    pub fn new(query_plan: QueryPlan<Id>, num_threads: usize) -> QPWorkers<Id> {
        let mut worker = QPWorkers {
            query_plans: vec![],
            elapsed_time: 0,
            intersection_cost: 0,
            num_intermediate_tuples: 0,
            num_out_tuples: 0,
            operator_metrics: vec![],
        };
        if num_threads == 1 {
            worker.query_plans.push(query_plan);
        } else {
            // num_threads > 1
            for i in 0..num_threads {
                worker.query_plans.push(query_plan.copy(true));
            }
            let global_vertex_idx_limits = VertexIdxLimits {
                from_variable_index_limit: 0,
                to_variable_index_limit: 0,
            };
            for query_plan in &mut worker.query_plans {
                for last_op in &mut query_plan.subplans {
                    let mut op = last_op.clone();
                    loop {
                        let prev = {
                            let op_ref = op.borrow();
                            get_op_attr_as_ref!(op_ref.deref(), prev)
                                .as_ref()
                                .map(|op| op.clone())
                        };
                        if prev.is_none() {
                            break;
                        }
                        op = prev.as_ref().unwrap().clone();
                    }
                    let mut op_mut = op.borrow_mut();
                    if let Operator::Scan(Scan::ScanBlocking(sb)) = op_mut.deref_mut() {
                        //TODO:Lock need to be fixed
                        sb.global_vertices_idx_limits = global_vertex_idx_limits.clone();
                    }
                }
            }
        }
        worker
    }

    pub fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.query_plans
            .iter_mut()
            .for_each(|plan| plan.init(graph));
    }

    pub fn execute(&mut self) {
        if self.query_plans.len() == 1 {
            self.query_plans[0].execute();
            self.elapsed_time = self.query_plans[0].elapsed_time;
        } else {
            let begin_time = SystemTime::now();
            //            let mut workers = vec![];
            for plan in &self.query_plans {
                let mut plan = plan.clone();
                //                workers.push(thread::spawn(move || {
                //                    plan.execute();
                //                }));
                plan.execute();
            }
            //            for worker in workers {
            //                worker.join();
            //            }
            self.elapsed_time = SystemTime::now()
                .duration_since(begin_time)
                .unwrap()
                .as_millis();
        }
    }

    pub fn get_output_log(&mut self) -> String {
        if self.query_plans.len() == 1 {
            return self.query_plans[0].get_output_log();
        }
        if self.operator_metrics.is_empty() {
            self.query_plans
                .iter_mut()
                .for_each(|plan| plan.set_stats());
            self.aggregate_output();
        }
        let mut str_joiner = vec![];
        str_joiner.push(format!("{}", self.elapsed_time));
        str_joiner.push(format!("{}", self.num_out_tuples));
        str_joiner.push(format!("{}", self.num_intermediate_tuples));
        str_joiner.push(format!("{}", self.intersection_cost));
        for operator_metric in &self.operator_metrics {
            str_joiner.push(format!("{}", operator_metric.0)); /* operator name */
            if !operator_metric.0.contains("PROBE")
                && !operator_metric.0.contains("HASH")
                && !operator_metric.0.contains("SCAN")
            {
                str_joiner.push(format!("{}", operator_metric.1)); /* i-cost */
            }
            if !operator_metric.0.contains("HASH") {
                str_joiner.push(format!("{}", operator_metric.2)); /* output tuples size */
            }
        }
        str_joiner.join(",")
    }

    fn aggregate_output(&mut self) {
        self.operator_metrics = vec![];
        for plan in &mut self.query_plans {
            self.intersection_cost += plan.icost;
            self.num_intermediate_tuples += plan.num_intermediate_tuples;
            self.num_out_tuples += plan.num_out_tuples;
        }
        let query_plan = &mut self.query_plans[0].operator_metrics;
        for metric in &mut self.query_plans[0].operator_metrics {
            self.operator_metrics
                .push((metric.0.clone(), metric.1, metric.2));
        }
        for i in 0..self.query_plans.len() {
            for j in 0..self.operator_metrics.len() {
                self.operator_metrics[j].1 += self.query_plans[i].operator_metrics[j].1;
                self.operator_metrics[j].2 += self.query_plans[i].operator_metrics[j].2;
            }
        }
    }
}
