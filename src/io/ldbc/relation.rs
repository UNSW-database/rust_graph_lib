use std::collections::HashMap;
use std::path::PathBuf;

use csv::StringRecord;
use regex::Regex;

use generic::{GraphTrait, MutGraphTrait};
use generic::{GraphType, IdType};
use graph_impl::graph_map::TypedGraphMap;

#[derive(Debug)]
pub struct Relation {
    start_label: String,
    target_label: String,
    edge_label: String,
    start_index: usize,
    target_index: usize,
    file_name_start: Regex,
}

impl Relation {
    pub fn new(
        start_label: &str,
        target_label: &str,
        edge_label: &str,
        start_index: usize,
        target_index: usize,
        file_name_start: &str,
    ) -> Self {
        Relation {
            start_label: start_label.to_owned(),
            target_label: target_label.to_owned(),
            edge_label: edge_label.to_owned(),
            start_index,
            target_index,
            file_name_start: Regex::new(&format!("{}{}", file_name_start, r"[_\d]*.csv")[..])
                .unwrap(),
        }
    }

    pub fn is_match(&self, path: &PathBuf) -> bool {
        let filename = path.as_path().file_name().unwrap().to_str().unwrap();

        self.file_name_start.is_match(filename)
    }

    pub fn add_edge<Id: IdType, Ty: GraphType>(
        &self,
        record: StringRecord,
        g: &mut TypedGraphMap<Id, String, String, Ty>,
        node_id_map: &mut HashMap<String, Id>,
    ) {
        let start_str_id = self.start_label.clone() + &record[self.start_index];

        let start_id = *node_id_map.entry(start_str_id).or_insert({
            let i = if let Some(i) = g.max_seen_id() {
                i.increment()
            } else {
                Id::new(0)
            };
            g.add_node(i, Some(self.start_label.clone()));

            i
        });

        let target_str_id = self.target_label.clone() + &record[self.target_index];

        let target_id = *node_id_map.entry(target_str_id).or_insert({
            let i = if let Some(i) = g.max_seen_id() {
                i.increment()
            } else {
                Id::new(0)
            };
            g.add_node(i, Some(self.target_label.clone()));

            i
        });

        g.add_edge(start_id, target_id, Some(self.edge_label.clone()));
    }
}
