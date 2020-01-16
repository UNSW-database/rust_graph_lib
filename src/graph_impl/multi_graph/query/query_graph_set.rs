use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use hashbrown::HashMap;

pub struct QueryGraphSet {
    query_graphs: HashMap<String, Vec<QueryGraph>>,
}

impl QueryGraphSet {
    pub fn new() -> Self {
        Self {
            query_graphs: HashMap::new(),
        }
    }

    pub fn add(&mut self, mut query_graph: QueryGraph) {
        let encoding = query_graph.get_encoding();
        let graph = self.query_graphs.get_mut(&encoding);
        if graph.is_none() {
            self.query_graphs.insert(encoding, vec![query_graph]);
        }
    }

    pub fn contains(&mut self, query_graph: &mut QueryGraph) -> bool {
        if let Some(query_graphs) = self.query_graphs.get_mut(&query_graph.get_encoding()) {
            for other_query_graph in query_graphs {
                if query_graph.is_isomorphic_to(other_query_graph) {
                    return true;
                }
            }
        }
        false
    }

    pub fn get_query_graph_set(&self) -> Vec<QueryGraph> {
        self.query_graphs
            .values()
            .map(|g| g.clone())
            .flatten()
            .collect()
    }
}
