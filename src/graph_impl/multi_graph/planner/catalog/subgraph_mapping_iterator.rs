use graph_impl::multi_graph::planner::catalog::catalog::LOGGER_FLAG;
use graph_impl::multi_graph::planner::catalog::query_edge::QueryEdge;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use hashbrown::HashMap;

// An iterator over a set of possible mappings between two query graphs.
#[derive(Clone)]
pub struct SubgraphMappingIterator {
    pub query_vertices: Vec<String>,
    pub o_qvertices: Vec<String>,
    pub o_qgraph: QueryGraph,
    pub next: HashMap<String, String>,
    pub is_next_computed: bool,
    pub curr_mapping: Vec<String>,
    pub current_idx: usize,
    pub vertex_indices: Vec<usize>,
    pub vertices_for_idx: Vec<Vec<String>>,

    pub qvertex_to_qedges_map: HashMap<String, HashMap<String, Vec<QueryEdge>>>,
    pub qvertex_to_type_map: HashMap<String, i32>,
    pub qvertex_to_deg_map: HashMap<String, Vec<usize>>,
}

impl SubgraphMappingIterator {
    pub fn new(query_vertices: Vec<String>) -> Self {
        let mut next = HashMap::new();
        query_vertices.iter().for_each(|v| {
            next.insert(v.clone(), String::from(""));
        });
        SubgraphMappingIterator {
            query_vertices,
            o_qvertices: vec![],
            o_qgraph: QueryGraph::empty(),
            next,
            is_next_computed: false,
            curr_mapping: vec![],
            current_idx: 0,
            vertex_indices: vec![],
            vertices_for_idx: vec![],
            qvertex_to_qedges_map: HashMap::new(),
            qvertex_to_type_map: HashMap::new(),
            qvertex_to_deg_map: HashMap::new(),
        }
    }

    pub fn init(&mut self, query_graph: &QueryGraph, o_query_graph: &QueryGraph) {
        self.o_qvertices = o_query_graph.get_query_vertices();
        self.o_qgraph = o_query_graph.clone();
        self.current_idx = 0;
        self.vertex_indices = vec![0; self.o_qvertices.len()];
        self.curr_mapping.clear();
        self.qvertex_to_qedges_map = query_graph.qvertex_to_qedges_map.clone();
        self.qvertex_to_deg_map = query_graph.qvertex_to_deg_map.clone();
        self.qvertex_to_type_map = query_graph.qvertex_to_type_map.clone();
        for i in 0..self.o_qvertices.len() {
            if self.vertices_for_idx.len() <= i {
                self.vertices_for_idx.push(vec![]);
            } else {
                self.vertices_for_idx[i].clear();
            }
            let o_qvertex = &self.o_qvertices[i];
            let o_qvertex_deg = &o_query_graph.qvertex_to_deg_map[o_qvertex];
            let o_qvertex_type = o_query_graph.qvertex_to_type_map[o_qvertex];

            for j in 0..self.query_vertices.len() {
                let q_vertex = &self.query_vertices[j];
                let vertex_type = self.qvertex_to_type_map[q_vertex];
                let q_vertex_deg = &self.qvertex_to_deg_map[q_vertex];
                if o_qvertex_type == vertex_type
                    && (o_qvertex_deg.eq(q_vertex_deg)
                        || (self.o_qvertices.len() < self.query_vertices.len()
                            && q_vertex_deg[0] >= o_qvertex_deg[0]
                            && q_vertex_deg[1] >= o_qvertex_deg[1]))
                {
                    self.vertices_for_idx[i].push(q_vertex.clone());
                }
            }
            if 0 == self.vertices_for_idx[i].len() {
                self.is_next_computed = true;
                return;
            }
        }
        self.is_next_computed = false;
        self.has_next();
    }

    pub fn has_next(&mut self) -> bool {
        if !self.is_next_computed {
            if self.curr_mapping.len() == self.o_qvertices.len() {
                self.curr_mapping.pop();
            }
            loop {
                let next_idx = self.curr_mapping.len();
                if next_idx == 0 && self.vertex_indices[0] < self.vertices_for_idx[0].len() {
                    self.curr_mapping
                        .push(self.vertices_for_idx[0][self.vertex_indices[0]].clone());
                    self.vertex_indices[0] += 1;
                } else if self.vertex_indices[next_idx] < self.vertices_for_idx[next_idx].len() {
                    let new_var = &self.vertices_for_idx[next_idx][self.vertex_indices[next_idx]];
                    self.vertex_indices[next_idx] += 1;
                    let other_for_new = &self.o_qvertices[next_idx];
                    let mut outer_flag = false;
                    for i in 0..self.curr_mapping.len() {
                        let prev_var = &self.curr_mapping[i];
                        if prev_var == new_var {
                            outer_flag = true;
                            break;
                        }
                        let other_for_prev = &self.o_qvertices[i];
                        let q_edges = self.qvertex_to_qedges_map[new_var].get(prev_var);
                        let o_qedges =
                            self.o_qgraph.qvertex_to_qedges_map[other_for_new].get(other_for_prev);
                        if q_edges.is_none() && o_qedges.is_none() {
                            continue;
                        }
                        if q_edges.is_none()
                            || o_qedges.is_none()
                            || q_edges.unwrap().len() != o_qedges.unwrap().len()
                        {
                            outer_flag = true;
                            break;
                        }
                        if q_edges.unwrap().len() == 0 {
                            continue;
                        }
                        let q_edge = &q_edges.unwrap()[0];
                        let o_qedge = &o_qedges.unwrap()[0];
                        if q_edge.label != o_qedge.label {
                            continue;
                        }
                        if !((&q_edge.from_query_vertex == prev_var
                            && &o_qedge.from_query_vertex == other_for_prev)
                            || (&q_edge.from_query_vertex == new_var
                                && &o_qedge.from_query_vertex == other_for_new))
                        {
                            outer_flag = true;
                            break;
                        }
                    }
                    if outer_flag {
                        continue;
                    }
                    self.curr_mapping.push(new_var.clone());
                } else if self.vertex_indices[next_idx] >= self.vertices_for_idx[next_idx].len() {
                    self.curr_mapping.pop();
                    self.vertex_indices[next_idx] = 0;
                }
                if self.curr_mapping.len() == self.o_qvertices.len()
                    || (self.vertex_indices[0] >= self.vertices_for_idx[0].len()
                        && self.curr_mapping.is_empty())
                {
                    break;
                }
            }
            self.is_next_computed = true;
        }
        if !self.curr_mapping.is_empty() {
            for i in 0..self.curr_mapping.len() {
                for j in (i + 1)..self.curr_mapping.len() {
                    let q_vertex = &self.curr_mapping[i];
                    let o_qvertex = &self.curr_mapping[j];
                    if !self.contains_query_edge(q_vertex, o_qvertex) {
                        continue;
                    }
                    let q_edge = &self.qvertex_to_qedges_map[q_vertex][o_qvertex][0];
                    let o_qedge = &self.o_qgraph.qvertex_to_qedges_map[&self.o_qvertices[i]]
                        [&self.o_qvertices[j]][0];
                    if q_edge.label == o_qedge.label {
                        continue;
                    }
                    self.is_next_computed = false;
                    return self.has_next();
                }
            }
        }

        !self.curr_mapping.is_empty()
    }

    pub fn next(&mut self) -> Option<HashMap<String, String>> {
        if !self.has_next() {
            return None;
        }
        self.is_next_computed = false;
        self.next.clear();
        for i in 0..self.o_qvertices.len() {
            self.next
                .insert(self.curr_mapping[i].clone(), self.o_qvertices[i].clone());
        }
        return Some(self.next.clone());
    }

    pub fn contains_query_edge(&self, v1: &String, v2: &String) -> bool {
        self.qvertex_to_qedges_map.contains_key(v1)
            && self.qvertex_to_qedges_map[v1].contains_key(v2)
    }
}
