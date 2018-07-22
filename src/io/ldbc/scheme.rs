use std::collections::HashMap;
use std::fs;
use std::io::Result;
use std::path::Path;

use generic::{GraphType, IdType};
use graph_impl::graph_map::TypedGraphMap;
use io::ldbc::node::Node;
use io::ldbc::relation::Relation;

use csv::ReaderBuilder;

#[derive(Debug)]
pub struct Scheme {
    relations: Vec<Relation>,
    nodes: Vec<Node>,
    delimiter: u8,
}

impl Scheme {
    pub fn init() -> Self {
        let n1 = Node::new("organisation", 0, 1, "organisation_");
        let n2 = Node::new("place", 0, 3, "place_");

        let r1 = Relation::new(
            "comment",
            "person",
            "hasCreator",
            0,
            1,
            "comment_hasCreator_person_",
        );
        let r2 = Relation::new("comment", "tag", "hasTag", 0, 1, "comment_hasTag_tag_");
        let r3 = Relation::new(
            "comment",
            "place",
            "isLocatedIn",
            0,
            1,
            "comment_isLocatedIn_place_",
        );
        let r4 = Relation::new(
            "comment",
            "comment",
            "replyOf",
            0,
            1,
            "comment_replyOf_comment_",
        );
        let r5 = Relation::new("comment", "post", "replyOf", 0, 1, "comment_replyOf_post_");
        let r6 = Relation::new(
            "forum",
            "post",
            "containerOf",
            0,
            1,
            "forum_containerOf_post_",
        );
        let r7 = Relation::new(
            "forum",
            "post",
            "containerOf",
            0,
            1,
            "forum_containerOf_post_",
        );
        let r8 = Relation::new(
            "forum",
            "person",
            "hasMember",
            0,
            1,
            "forum_hasMember_person_",
        );
        let r9 = Relation::new(
            "forum",
            "person",
            "hasModerator",
            0,
            1,
            "forum_hasModerator_person_",
        );
        let r10 = Relation::new(
            "forum",
            "person",
            "hasModerator",
            0,
            1,
            "forum_hasModerator_person_",
        );
        let r11 = Relation::new("forum", "tag", "hasTag", 0, 1, "forum_hasTag_tag_");
        let r12 = Relation::new(
            "organisation",
            "place",
            "isLocatedIn",
            0,
            1,
            "organisation_isLocatedIn_place_",
        );
        let r13 = Relation::new(
            "person",
            "tag",
            "hasInterest",
            0,
            1,
            "person_hasInterest_tag_",
        );
        let r14 = Relation::new(
            "person",
            "place",
            "isLocatedIn",
            0,
            1,
            "person_isLocatedIn_place_",
        );
        let r15 = Relation::new("person", "person", "knows", 0, 1, "person_knows_person_");
        let r16 = Relation::new("person", "comment", "likes", 0, 1, "person_likes_comment_");
        let r17 = Relation::new("person", "post", "likes", 0, 1, "person_likes_post_");
        let r18 = Relation::new(
            "person",
            "organisation",
            "studyAt",
            0,
            1,
            "person_studyAt_organisation_",
        );
        let r19 = Relation::new(
            "person",
            "organisation",
            "workAt",
            0,
            1,
            "person_workAt_organisation_",
        );
        let r20 = Relation::new("place", "place", "isPartOf", 0, 1, "place_isPartOf_place_");
        let r21 = Relation::new(
            "post",
            "person",
            "hasCreator",
            0,
            1,
            "post_hasCreator_person_",
        );
        let r22 = Relation::new("post", "tag", "hasTag", 0, 1, "post_hasTag_tag_");
        let r23 = Relation::new(
            "post",
            "place",
            "isLocatedIn",
            0,
            1,
            "post_isLocatedIn_place_",
        );
        let r24 = Relation::new("tag", "tagclass", "hasType", 0, 1, "tag_hasType_tagclass_");
        let r25 = Relation::new(
            "tagclass",
            "tagclass",
            "isSubclassOf",
            0,
            1,
            "tagclass_isSubclassOf_tagclass_",
        );

        Scheme {
            relations: vec![
                r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18,
                r19, r20, r21, r22, r23, r24, r25,
            ],
            nodes: vec![n1, n2],
            delimiter: b'|',
        }
    }

    pub fn from_path<Id: IdType, Ty: GraphType, P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<TypedGraphMap<Id, String, String, Ty>> {
        if !path.as_ref().is_dir() {
            panic!("path must be a dir")
        }

        let mut files_in_dir = Vec::new();

        for entry in fs::read_dir(path)? {
            let dir = entry?;
            let path = dir.path();
            if path.is_file() {
                files_in_dir.push(path);
            }
        }

        let mut g = TypedGraphMap::new();
        let mut node_id_map = HashMap::<String, Id>::new();

        for node in self.nodes.iter() {
            for path in files_in_dir.iter().cloned() {
                if node.is_match(&path) {
                    println!("{:?} matches {:?}", &path, node);

                    let mut rdr = ReaderBuilder::new()
                        .delimiter(self.delimiter)
                        .from_path(path)?;

                    for result in rdr.records() {
                        let record = result?;

                        node.add_node::<Id, Ty>(record, &mut g, &mut node_id_map);
                    }
                }
            }
        }

        for relation in self.relations.iter() {
            for path in files_in_dir.iter().cloned() {
                if relation.is_match(&path) {
                    println!("{:?} matches {:?}", &path, relation);

                    let mut rdr = ReaderBuilder::new()
                        .delimiter(self.delimiter)
                        .from_path(path)?;

                    for result in rdr.records() {
                        let record = result?;

                        relation.add_edge::<Id, Ty>(record, &mut g, &mut node_id_map);
                    }
                }
            }
        }

        Ok(g)
    }
}
