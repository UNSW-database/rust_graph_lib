use serde::export::fmt::Error;
use serde::export::Formatter;
use std::fmt::Display;
#[derive(Clone)]
pub enum Direction {
    Fwd,
    Bwd,
}

pub struct AdjListDescriptor {
    pub from_query_vertex: String,
    vertex_idx: usize,
    pub direction: Direction,
    pub label: usize,
}

impl AdjListDescriptor {
    pub fn new(
        from_query_vertex: String,
        vertex_idx: usize,
        direction: Direction,
        label: usize,
    ) -> Self {
        Self {
            from_query_vertex,
            vertex_idx,
            direction,
            label,
        }
    }
}

impl Display for Direction {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Direction::Fwd => write!(f, "Fwd"),
            Direction::Bwd => write!(f, "Bwd"),
        }
    }
}
