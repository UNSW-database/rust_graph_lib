use graph_impl::multi_graph::planner::catalog::query_edge::QueryEdge;
use graph_impl::multi_graph::planner::catalog::subgraph_mapping_iterator::SubgraphMappingIterator;
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use std::iter::FromIterator;

#[derive(Clone)]
pub struct QueryGraph {
    pub qvertex_to_qedges_map: HashMap<String, HashMap<String, Vec<QueryEdge>>>,
    pub qvertex_to_type_map: HashMap<String, usize>,
    pub qvertex_to_deg_map: HashMap<String, Vec<usize>>,
    pub q_edges: Vec<QueryEdge>,
    pub it: Option<Box<SubgraphMappingIterator>>,
    pub encoding: Option<String>,
    pub limit: usize,
}

impl QueryGraph {
    pub fn empty() -> QueryGraph {
        QueryGraph {
            qvertex_to_qedges_map: HashMap::new(),
            qvertex_to_type_map: HashMap::new(),
            qvertex_to_deg_map: HashMap::new(),
            q_edges: vec![],
            it: None,
            encoding: None,
            limit: 0,
        }
    }

    pub fn get_num_qvertices(&self) -> usize {
        self.qvertex_to_qedges_map.len()
    }

    pub fn get_subgraph_mapping_iterator(
        &mut self,
        query_graph: &QueryGraph,
    ) -> &mut Box<SubgraphMappingIterator> {
        let mut it = self
            .it
            .take()
            .unwrap_or(Box::new(SubgraphMappingIterator::new(
                self.qvertex_to_qedges_map
                    .keys()
                    .map(|x| x.clone())
                    .sorted(),
            )));
        it.init(&self, query_graph);
        self.it.replace(it);
        self.it.as_mut().unwrap()
    }

    pub fn get_query_vertices(&self) -> Vec<String> {
        self.qvertex_to_qedges_map
            .keys()
            .map(|x| x.clone())
            .sorted()
    }

    pub fn get_query_vertices_as_set(&self) -> HashSet<String> {
        let mut set = HashSet::new();
        self.qvertex_to_qedges_map.keys().for_each(|key| {
            set.insert(key.clone());
        });
        set
    }

    pub fn get_query_vertex_type(&self, query_vertex: &str) -> usize {
        if let Some(vertex_type) = self.qvertex_to_type_map.get(query_vertex) {
            return vertex_type.clone();
        }
        0
    }

    pub fn set_query_vertex_type(&mut self, query_vertex: String, to_type: usize) {
        self.qvertex_to_type_map
            .insert(query_vertex.clone(), to_type);
        for edge in self.q_edges.iter_mut() {
            if edge.from_query_vertex == query_vertex {
                edge.from_type = to_type;
            } else if edge.to_query_vertex == query_vertex {
                edge.to_type = to_type;
            }
        }
    }

    pub fn contains_query_edge(&self, v1: &String, v2: &String) -> bool {
        if let Some(map) = self.qvertex_to_qedges_map.get(v1) {
            return map.contains_key(v2);
        }
        false
    }

    pub fn get_qedges(&self, variable: &String, neighbor_variable: &String) -> Vec<QueryEdge> {
        if !self.qvertex_to_qedges_map.contains_key(variable) {
            panic!("The variable '{}' is not present.", variable);
        }
        let contains_in_qedges = self
            .qvertex_to_qedges_map
            .get(variable)
            .map_or(false, |map| map.contains_key(neighbor_variable));
        if !contains_in_qedges {
            return vec![];
        }
        self.qvertex_to_qedges_map[variable][neighbor_variable].clone()
    }

    pub fn get_encoding(&mut self) -> String {
        if self.encoding.is_some() {
            return self.encoding.as_ref().unwrap().clone();
        }
        let mut query_vertices_encoded = vec![String::from(""); self.qvertex_to_qedges_map.len()];
        let mut vertex_idx = 0;
        for from_vertex in self.qvertex_to_qedges_map.keys() {
            let from_vertex = from_vertex.clone();
            let mut encoding_str = "".to_string();
            if let Some(edge_map) = self.qvertex_to_qedges_map.get(&from_vertex) {
                edge_map.keys().for_each(|to_vertex| {
                    if let Some(query_edges) = edge_map.get(to_vertex) {
                        for query_edge in query_edges {
                            if from_vertex == query_edge.from_query_vertex {
                                encoding_str += "F";
                            } else {
                                encoding_str += "B";
                            }
                        }
                    }
                });
            }
            let encoding_to_sort = String::from_iter(encoding_str.chars().into_iter().sorted());
            query_vertices_encoded[vertex_idx] = encoding_to_sort;
            vertex_idx += 1;
        }
        query_vertices_encoded.sort();
        self.encoding = Some(query_vertices_encoded.join("."));
        self.encoding.as_ref().unwrap().clone()
    }

    fn get_subgraph_mapping_if_any(
        &mut self,
        other_query_graph: &QueryGraph,
    ) -> Option<HashMap<String, String>> {
        let it = self.get_subgraph_mapping_iterator(other_query_graph);
        if !it.has_next() {
            return None;
        }
        it.next()
    }

    pub fn is_isomorphic_to(&mut self, other_query_graph: &mut QueryGraph) -> bool {
        other_query_graph.get_encoding() == self.get_encoding()
            && ((self.q_edges.len() == 0 && other_query_graph.q_edges.len() == 0)
                || self
                    .get_subgraph_mapping_if_any(other_query_graph)
                    .is_some())
    }

    pub fn get_isomorphic_mapping_if_any(
        &mut self,
        other_query_graph: &mut QueryGraph,
    ) -> Option<HashMap<String, String>> {
        if self.is_isomorphic_to(other_query_graph) {
            return self.get_subgraph_mapping_if_any(other_query_graph);
        }
        None
    }

    pub fn add_qedges(&mut self, query_edges: &Vec<QueryEdge>) {
        query_edges
            .iter()
            .for_each(|edge| self.add_qedge(edge.clone()));
    }

    pub fn add_qedge(&mut self, query_edge: QueryEdge) {
        // Get the vertex IDs.
        let from_qvertex = query_edge.from_query_vertex.clone();
        let to_qvertex = query_edge.to_query_vertex.clone();
        let from_type = query_edge.from_type.clone();
        let to_type = query_edge.to_type.clone();
        self.qvertex_to_type_map
            .entry(from_qvertex.clone())
            .or_insert(0);
        self.qvertex_to_type_map
            .entry(to_qvertex.clone())
            .or_insert(0);
        if 0 != from_type {
            self.qvertex_to_type_map
                .insert(from_qvertex.clone(), from_type);
        }
        if 0 != to_type {
            self.qvertex_to_type_map.insert(to_qvertex.clone(), to_type);
        }
        // Set the in and out degrees for each variable.
        if !self.qvertex_to_deg_map.contains_key(&from_qvertex) {
            self.qvertex_to_deg_map
                .insert(from_qvertex.clone(), vec![0; 2]);
        }
        *self
            .qvertex_to_deg_map
            .get_mut(&from_qvertex)
            .unwrap()
            .get_mut(0)
            .unwrap() += 1;
        if !self.qvertex_to_deg_map.contains_key(&to_qvertex) {
            self.qvertex_to_deg_map
                .insert(to_qvertex.clone(), vec![0; 2]);
        }
        self.qvertex_to_deg_map
            .get_mut(&to_qvertex)
            .map(|to| to[1] += 1);
        // Add fwd edge from_qvertex -> to_qvertex to the qVertexToQEdgesMap.
        self.add_qedge_to_qgraph(from_qvertex.clone(), to_qvertex.clone(), query_edge.clone());
        // Add bwd edge to_qvertex <- from_qvertex to the qVertexToQEdgesMap.
        self.add_qedge_to_qgraph(to_qvertex.clone(), from_qvertex.clone(), query_edge.clone());
        self.q_edges.push(query_edge);
    }

    fn add_qedge_to_qgraph(&mut self, from_qvertex: String, to_qvertex: String, q_edge: QueryEdge) {
        self.qvertex_to_qedges_map
            .entry(from_qvertex.clone())
            .or_insert(HashMap::new());
        self.qvertex_to_qedges_map
            .get_mut(&from_qvertex)
            .map(|qedge_map| qedge_map.entry(to_qvertex).or_insert(vec![q_edge]));
    }

    pub fn get_neighbors(&self, from_var: Vec<String>) -> HashSet<String> {
        let mut to_variables = HashSet::new();
        from_var.iter().for_each(|from| {
            if !self.qvertex_to_qedges_map.contains_key(from) {
                panic!("The variable '{}' is not present.", from);
            }
            self.get_neighbours_of_node(from).into_iter().for_each(|n| {
                to_variables.insert(n);
            });
        });
        from_var.iter().for_each(|from| {
            to_variables.remove(from);
        });
        to_variables
    }

    pub fn get_neighbours_of_node(&self, from: &String) -> Vec<String> {
        if !self.qvertex_to_qedges_map.contains_key(from) {
            panic!("The variable '{}' is not present.", from);
        }
        self.qvertex_to_qedges_map[from]
            .keys()
            .map(|key| key.clone())
            .collect()
    }

    pub fn copy(&self) -> QueryGraph {
        let mut q = QueryGraph::empty();
        q.add_qedges(&self.q_edges);
        q
    }
}

impl PartialEq for QueryGraph {
    fn eq(&self, other: &Self) -> bool {
        self.qvertex_to_qedges_map == other.qvertex_to_qedges_map
            && self.qvertex_to_type_map == other.qvertex_to_type_map
            && self.qvertex_to_deg_map == other.qvertex_to_deg_map
            && self.q_edges.eq(&other.q_edges)
            && self.encoding.as_ref().unwrap() == other.encoding.as_ref().unwrap()
            && self.limit == other.limit
    }
}
