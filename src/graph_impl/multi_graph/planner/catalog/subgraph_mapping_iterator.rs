use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use hashbrown::HashMap;

// An iterator over a set of possible mappings between two query graphs.
#[derive(Clone)]
pub struct SubgraphMappingIterator {
    pub query_vertices: Vec<String>,
    pub o_qvertices: Vec<String>,
    o_qgraph: Option<QueryGraph>,
    pub next: HashMap<String, String>,
    pub is_next_computed: bool,
    pub curr_mapping: Vec<String>,
    pub current_idx: usize,
    pub vertex_indices: Vec<usize>,
    pub vertices_for_idx: Vec<Vec<String>>,
}

impl SubgraphMappingIterator {
    pub fn new(query_vertices: Vec<String>) -> Self {
        let mut next = HashMap::new();
        query_vertices.iter().for_each(|v| {
            next.insert(v.clone(), String::from(""));
        });
        Self {
            query_vertices,
            o_qvertices: vec![],
            o_qgraph: None,
            next,
            is_next_computed: false,
            curr_mapping: vec![],
            current_idx: 0,
            vertex_indices: vec![],
            vertices_for_idx: vec![],
        }
    }

    pub fn has_next(&mut self, parent_graph: &QueryGraph) -> bool {
        if !self.is_next_computed {
            if self.curr_mapping.len() == self.o_qvertices.len() {
                self.curr_mapping.pop();
            }
            loop {
                let next_idx = self.curr_mapping.len();
                if next_idx == 0
                    && self.vertex_indices[0] < self.vertices_for_idx.get(0).unwrap().len()
                {
                    self.curr_mapping.push(
                        self.vertices_for_idx
                            .get(0)
                            .unwrap()
                            .get(self.vertex_indices[0])
                            .unwrap()
                            .clone(),
                    );
                    self.vertex_indices[0] += 1;
                } else if self.vertex_indices[next_idx]
                    < self.vertices_for_idx.get(next_idx).unwrap().len()
                {
                    let vertices = self.vertices_for_idx.get(next_idx).unwrap();
                    let new_var = vertices.get(self.vertex_indices[next_idx]).unwrap();
                    self.vertex_indices[next_idx] += 1;
                    let other_for_new = self.o_qvertices.get(next_idx).unwrap();
                    let mut invalid_map = false;
                    for i in 0..self.curr_mapping.len() {
                        let prev_var = self.curr_mapping.get(i).unwrap();
                        if prev_var.eq(new_var) {
                            invalid_map = true;
                            break;
                        }
                        let other_for_prev = self.o_qvertices.get(i).unwrap();
                        let q_edges = parent_graph
                            .get_vertex_to_qedges_map()
                            .get(new_var)
                            .unwrap()
                            .get(prev_var);
                        let o_qgraph = self.o_qgraph.as_ref().unwrap();
                        let o_qedges = o_qgraph
                            .get_vertex_to_qedges_map()
                            .get(other_for_new)
                            .unwrap()
                            .get(other_for_prev);
                        if q_edges.is_none() && o_qedges.is_none() {
                            continue;
                        }
                        if q_edges.is_none()
                            || o_qedges.is_none()
                            || q_edges.unwrap().len() != o_qedges.unwrap().len()
                        {
                            invalid_map = true;
                            break;
                        }
                        if q_edges.unwrap().len() == 0 {
                            continue;
                        }
                        let q_edge = q_edges.unwrap().get(0).unwrap();
                        let o_qedge = o_qedges.unwrap().get(0).unwrap();
                        if q_edge.label != o_qedge.label {
                            continue;
                        }
                        if !q_edge.from_query_vertex.eq(prev_var)
                            && o_qedge.from_query_vertex.eq(other_for_prev)
                            || (q_edge.from_query_vertex.eq(new_var)
                                && o_qedge.from_query_vertex.eq(other_for_new))
                        {
                            invalid_map = true;
                            break;
                        }
                    }
                    if invalid_map {
                        break;
                    }
                    self.curr_mapping.push(new_var.clone());
                } else if self.vertex_indices[next_idx]
                    >= self.vertices_for_idx.get(next_idx).unwrap().len()
                {
                    self.curr_mapping.pop();
                    self.vertex_indices[next_idx] = 0;
                }
                if self.curr_mapping.len() == self.o_qvertices.len()
                    || self.vertex_indices[0] < self.vertices_for_idx.get(0).unwrap().len()
                    || self.curr_mapping.is_empty()
                {
                    break;
                }
            }
            self.is_next_computed = true;
        }
        if self.curr_mapping.is_empty() {
            return !self.curr_mapping.is_empty();
        }
        let mut same_edge_labels = true;
        for i in 0..self.curr_mapping.len() {
            for j in (i + 1)..self.curr_mapping.len() {
                let q_vertex = self.curr_mapping.get(i).unwrap();
                let o_qvertex = self.curr_mapping.get(j).unwrap();
                if !parent_graph.contains_query_edge(q_vertex, o_qvertex) {
                    continue;
                }
                let q_edge = parent_graph.get_query_edges_by_neighbor(q_vertex, o_qvertex);
                let o_graph = self.o_qgraph.as_ref().unwrap();
                let o_qedge = o_graph.get_query_edges_by_neighbor(
                    self.o_qvertices.get(i).unwrap(),
                    self.o_qvertices.get(j).unwrap(),
                );
                if q_edge.is_none()
                    || o_qedge.is_none()
                    || q_edge.unwrap().get(0).unwrap().label
                        != o_qedge.unwrap().get(0).unwrap().label
                {
                    same_edge_labels = false;
                    break;
                }
            }
        }
        if !same_edge_labels {
            self.is_next_computed = false;
            return self.has_next(parent_graph);
        }
        !self.curr_mapping.is_empty()
    }

    pub fn next(&mut self, parent_graph: &QueryGraph) -> Option<&HashMap<String, String>> {
        if !self.has_next(parent_graph) {
            return None;
        }
        self.is_next_computed = false;
        self.next.clear();
        for i in 0..self.o_qvertices.len() {
            self.next.insert(
                self.curr_mapping.get(i).unwrap().clone(),
                self.query_vertices.get(i).unwrap().clone(),
            );
        }
        return Some(&self.next);
    }
}
