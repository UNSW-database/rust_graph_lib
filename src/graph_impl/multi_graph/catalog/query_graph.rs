use graph_impl::multi_graph::catalog::query_edge::QueryEdge;
use graph_impl::multi_graph::catalog::subgraph_mapping_iterator::SubgraphMappingIterator;
use hashbrown::HashMap;
use itertools::Itertools;
use std::iter::FromIterator;

#[derive(Clone)]
pub struct QueryGraph {
    qvertex_to_qedges_map: HashMap<String, HashMap<String, Vec<QueryEdge>>>,
    qvertex_to_type_map: HashMap<String, usize>,
    qvertex_to_deg_map: HashMap<String, Vec<usize>>,
    q_edges: Vec<QueryEdge>,
    // Using `Box` here to enable clone.
    it: Option<Box<SubgraphMappingIterator>>,
    encoding: Option<String>,
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
        }
    }

    pub fn get_num_qvertices(&self) -> usize {
        self.qvertex_to_qedges_map.len()
    }

    pub fn get_subgraph_mapping_iterator(&mut self, query_graph: &QueryGraph) -> &mut Self {
        if self.it.is_none() {
            self.it.replace(Box::new(SubgraphMappingIterator::new(
                self.qvertex_to_qedges_map
                    .keys()
                    .map(|x| x.clone())
                    .collect(),
            )));
        }
        let mut it = self.it.take().unwrap();
        self.init_subgraph_iterator(&mut it, query_graph);
        self.it.replace(it);
        self
    }

    pub fn get_vertex_to_deg_map(&self) -> &HashMap<String, Vec<usize>> {
        &self.qvertex_to_deg_map
    }

    pub fn get_vertex_to_qedges_map(&self) -> &HashMap<String, HashMap<String, Vec<QueryEdge>>> {
        &self.qvertex_to_qedges_map
    }

    pub fn get_vertex_to_type_map(&self) -> &HashMap<String, usize> {
        &self.qvertex_to_type_map
    }

    pub fn get_query_vertices(&self) -> Vec<String> {
        let mut vec = vec![];
        for key in self.qvertex_to_qedges_map.keys() {
            vec.push(key.clone());
        }
        vec
    }

    pub fn get_query_vertex_type(&self, query_vertex: &str) -> usize {
        let vertex_type = self.qvertex_to_type_map.get(query_vertex);
        if let Some(vertex_type) = vertex_type {
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

    pub fn get_query_edges_by_neighbor(
        &self,
        variable: &String,
        neighbor_variable: &String,
    ) -> Option<&Vec<QueryEdge>> {
        if let Some(edges) = self.get_vertex_to_qedges_map().get(variable) {
            return edges.get(neighbor_variable);
        }
        None
    }

    pub fn get_query_edges(&self) -> &Vec<QueryEdge> {
        &self.q_edges
    }

    pub fn has_next(&mut self) -> bool {
        let mut iterator = self.it.take().unwrap();
        let res = iterator.has_next(&self);
        self.it.replace(iterator);
        res
    }

    pub fn next(&mut self) -> Option<&HashMap<String, String>> {
        if let Some(mut iterator) = self.it.take() {
            iterator.next(&self);
            self.it.replace(iterator);
            return Some(&self.it.as_ref().unwrap().next);
        }
        None
    }

    fn init_subgraph_iterator(
        &mut self,
        it: &mut SubgraphMappingIterator,
        o_query_graph: &QueryGraph,
    ) {
        it.o_qvertices = o_query_graph.get_query_vertices();
        it.current_idx = 0;
        it.vertex_indices = vec![0; it.o_qvertices.len()];
        it.curr_mapping.clear();
        for i in 0..it.o_qvertices.len() {
            if it.vertices_for_idx.len() <= i {
                it.vertices_for_idx.push(vec![]);
            } else {
                it.vertices_for_idx.get_mut(i).unwrap().clear();
            }
            let o_qvertex = it.o_qvertices.get(i).unwrap();
            let o_qvertex_deg = o_query_graph
                .get_vertex_to_deg_map()
                .get(o_qvertex)
                .unwrap();
            for j in 0..it.query_vertices.len() {
                let q_vertex = it.query_vertices.get(j).unwrap();
                let vertex_type = self.get_vertex_to_type_map().get(q_vertex).unwrap();
                let q_vertex_deg = self.get_vertex_to_deg_map().get(q_vertex).unwrap();
                if o_query_graph
                    .get_vertex_to_type_map()
                    .get(o_qvertex)
                    .unwrap()
                    == vertex_type
                    && o_qvertex_deg.eq(q_vertex_deg)
                    || (it.o_qvertices.len() < it.query_vertices.len()
                        && q_vertex_deg[0] >= o_qvertex_deg[0]
                        && q_vertex_deg[1] >= o_qvertex_deg[1])
                {
                    it.vertices_for_idx
                        .get_mut(i)
                        .unwrap()
                        .push(q_vertex.clone());
                }
            }
            if 0 == it.vertices_for_idx.get(i).unwrap().len() {
                it.is_next_computed = true;
                return;
            }
        }
        it.is_next_computed = false;
        it.has_next(self);
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
    ) -> Option<&HashMap<String, String>> {
        let it = self.get_subgraph_mapping_iterator(other_query_graph);
        if it.has_next() {
            return it.next();
        }
        None
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
    ) -> Option<&HashMap<String, String>> {
        if self.is_isomorphic_to(other_query_graph) {
            return self.get_subgraph_mapping_if_any(other_query_graph);
        }
        None
    }
}
