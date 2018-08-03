use std::collections::HashMap;
use std::path::PathBuf;

use csv::StringRecord;
use regex::Regex;

use generic::{GraphTrait, MutGraphTrait};
use generic::{GraphType, IdType};
use graph_impl::graph_map::TypedGraphMap;

#[derive(Debug)]
pub struct Node {
    name: String,
    id_index: usize,
    label_index: usize,
    file_name_start: Regex,
}

impl Node {
    pub fn new(name: &str, id_index: usize, label_index: usize, file_name_start: &str) -> Self {
        Node {
            name: name.to_owned(),
            id_index,
            label_index,
            file_name_start: Regex::new(&format!(r"^{}[_\d]*.csv", file_name_start)[..]).unwrap(),
        }
    }

    pub fn is_match(&self, path: &PathBuf) -> bool {
        let filename = path.as_path().file_name().unwrap().to_str().unwrap();

        self.file_name_start.is_match(filename)
    }

    pub fn add_node<Id: IdType, Ty: GraphType>(
        &self,
        record: StringRecord,
        g: &mut TypedGraphMap<Id, String, String, Ty>,
        node_id_map: &mut HashMap<String, Id>,
    ) {
        let str_id = self.name.clone() + &record[self.id_index];

        let id = *node_id_map.entry(str_id).or_insert_with(|| {
            if let Some(i) = g.max_seen_id() {
                i.increment()
            } else {
                Id::new(0)
            }
        });

        g.add_node(id, Some(record[self.label_index].to_owned()));
    }
}

//organisation_*.csv id | type("university", "company") | name | url
//place_*.csv id | name | url | type("city", "country", "continent")
