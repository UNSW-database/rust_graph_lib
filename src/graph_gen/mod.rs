pub mod general;
pub mod random;
pub mod helper;

pub use graph_gen::general::{complete_graph, complete_graph_unlabeled};
pub use graph_gen::general::{empty_graph, empty_graph_unlabeled};
pub use graph_gen::random::{random_gnm_graph, random_gnm_graph_unlabeled};
pub use graph_gen::random::{random_gnp_graph, random_gnp_graph_unlabeled};
