pub mod general;
pub mod helper;
pub mod random;

pub use graph_gen::general::{
    complete_graph, complete_graph_unlabeled, empty_graph, empty_graph_unlabeled,
};
pub use graph_gen::random::{
    random_gnm_graph, random_gnm_graph_unlabeled, random_gnp_graph, random_gnp_graph_unlabeled,
};
