#[derive(Clone, Debug, PartialEq)]
pub struct QueryEdge {
    pub from_query_vertex: String,
    pub to_query_vertex: String,
    pub from_type: i32,
    pub to_type: i32,
    pub label: i32,
}

impl QueryEdge {
    pub fn new(
        from_qvertex: String,
        to_qvertex: String,
        from_type: i32,
        to_type: i32,
        label: i32,
    ) -> Self {
        Self {
            from_query_vertex: from_qvertex,
            to_query_vertex: to_qvertex,
            from_type,
            to_type,
            label,
        }
    }
    pub fn default(from_qvertex: String, to_qvertex: String) -> Self {
        Self {
            from_query_vertex: from_qvertex,
            to_query_vertex: to_qvertex,
            from_type: 0,
            to_type: 0,
            label: 0,
        }
    }
}
