/*
 * Copyright (c) 2018 UNSW Sydney, Data and Knowledge Group.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use property::filter::{Expression, Var};
use property::property_parser::recursive_parser;
use regex::Regex;
use std::marker::{Send, Sync};

#[derive(Clone)]
pub enum NodeElement {
    Star(usize),
    Exp(usize, Box<Expression>),
    Count(usize),
}

unsafe impl Send for NodeElement {}

unsafe impl Sync for NodeElement {}

pub struct ResultBlueprint {
    node_elements: Vec<NodeElement>,
}

impl Default for ResultBlueprint {
    fn default() -> Self {
        ResultBlueprint {
            node_elements: vec![
                NodeElement::Exp(0, Box::new(Var::new("name".to_owned()))),
                NodeElement::Exp(1, Box::new(Var::new("genre".to_owned()))),
                NodeElement::Star(2),
            ],
        }
    }
}

impl ResultBlueprint {
    pub fn new() -> Self {
        ResultBlueprint {
            node_elements: vec![],
        }
    }

    pub fn get_node_elements(&self) -> &Vec<NodeElement> {
        &self.node_elements
    }

    pub fn add_node_element(&mut self, node_element: NodeElement) {
        self.node_elements.push(node_element);
    }
}

unsafe impl Sync for ResultBlueprint {}

unsafe impl Send for ResultBlueprint {}

pub fn parse_result_blueprint(cypher_tree: Vec<String>) -> ResultBlueprint {
    let mut result_blueprint = ResultBlueprint::new();
    let cypher_tree: Vec<&str> = cypher_tree.iter().map(|s| &**s).collect();

    for i in 0..cypher_tree.len() {
        let line: &str = cypher_tree[i];
        if line.contains("RETURN") && line.contains("*") {
            let largest_node = get_largest_node(&cypher_tree);
            for i in 0..largest_node + 1 {
                result_blueprint.add_node_element(NodeElement::Star(i));
            }
            break;
        }
        if line.contains("> > projection") {
            let re = Regex::new(r"> projection\s+expression=@(?P<result_line>\w+)").unwrap();
            if let Some(caps) = re.captures(line) {
                let index: usize = caps["result_line"].parse::<usize>().unwrap();
                let var_string_option = collect_var(&cypher_tree, index);
                if var_string_option.is_none() {
                    break;
                }
                let var_string = var_string_option.unwrap();
                let current_var = var_string
                    .parse::<usize>()
                    .expect("Cypher tree contains non-integer as node id");

                if cypher_tree[index].contains("> property") {
                    result_blueprint.add_node_element(NodeElement::Exp(
                        current_var,
                        recursive_parser(&cypher_tree, index, &var_string)
                            .expect("Unable to parse result expression"),
                    ));
                } else if cypher_tree[index].contains("> apply") {
                    result_blueprint.add_node_element(NodeElement::Count(current_var));
                } else {
                    result_blueprint.add_node_element(NodeElement::Star(current_var));
                }
            }
        }
    }

    result_blueprint
}

fn collect_var(cypher_tree: &[&str], index: usize) -> Option<String> {
    let mut i = index;
    while i < cypher_tree.len() {
        let line: &str = cypher_tree[i];
        if line.contains("(*)") {
            return None;
        }

        if line.contains("> > identifier") {
            let re = Regex::new(r"> identifier\s+`(?P<var_name>\d+)`").unwrap();
            if let Some(caps) = re.captures(line) {
                let var_name = &caps["var_name"];
                return Some(var_name.to_owned());
            }
        }
        i += 1;
    }
    panic!("Cannot find valid identifier");
}

fn get_largest_node(cypher_tree: &[&str]) -> usize {
    let mut largest_node = 0usize;
    for i in 0..cypher_tree.len() {
        let line = cypher_tree[i];
        if line.contains("node pattern") {
            let re = Regex::new(r"> identifier\s+`(?P<node>\d+)`").unwrap();
            if let Some(caps) = re.captures(cypher_tree[i + 1]) {
                let node = &caps["node"]
                    .parse::<usize>()
                    .expect("Cypher tree contains non-integer as node id");;
                if *node > largest_node {
                    largest_node = *node;
                }
            }
        }
    }
    largest_node
}
