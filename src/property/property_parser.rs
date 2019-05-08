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

use property::filter::empty_expression;
use property::filter::PropertyResult;
use property::filter::{
    ArithmeticExpression, ArithmeticOperator, Const, Expression, PredicateExpression,
    PredicateOperator, Var,
};
use property::PropertyError;
use regex::Regex;
use serde_json::json;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::collections::HashSet;
use std::marker::{Send, Sync};

#[derive(Clone)]
pub struct ExpressionCache {
    node_expressions: HashMap<usize, Box<Expression>>,
    edge_expressions: HashMap<(usize, usize), Box<Expression>>,
}

impl ExpressionCache {
    pub fn new(
        node_expressions: HashMap<usize, Box<Expression>>,
        edge_expressions: HashMap<(usize, usize), Box<Expression>>,
    ) -> Self {
        ExpressionCache {
            node_expressions,
            edge_expressions,
        }
    }

    pub fn get_node_exp(&self, id: usize) -> Option<Box<Expression>> {
        if self.node_expressions.contains_key(&id) {
            Some(self.node_expressions[&id].box_clone())
        } else {
            None
        }
    }

    pub fn get_edge_exp(&self, src: usize, dst: usize) -> Option<Box<Expression>> {
        if self.edge_expressions.contains_key(&(src, dst)) {
            Some(self.edge_expressions[&(src, dst)].box_clone())
        } else {
            None
        }
    }
}

unsafe impl Sync for ExpressionCache {}
unsafe impl Send for ExpressionCache {}

pub fn parse_property_tree(cypher_tree: Vec<String>) -> ExpressionCache {
    // edge_id = (src_id + 1) * count("node pattern") + (dst_id)
    if cypher_tree.len() == 0 {
        panic!("The given cypher tree is empty");
    }
    let all_property = parse_property(cypher_tree.iter().map(|s| &**s).collect());
    let mut node_count = 0usize;
    for line in cypher_tree {
        if line.contains("node pattern") {
            node_count += 1;
        }
    }

    let mut node_property = HashMap::new();
    let mut edge_property = HashMap::new();

    for key in all_property.keys() {
        let id: usize = key.parse::<usize>().unwrap();
        if id < node_count {
            node_property.insert(id, all_property[key].clone());
        } else {
            let dst = id % node_count;
            let src = (id - dst) / node_count - 1;
            edge_property.insert((src, dst), all_property[key].clone());
        }
    }

    trace!("Node keys: {:?}", node_property.keys());
    trace!("Edge keys: {:?}", edge_property.keys());

    ExpressionCache::new(node_property, edge_property)
}

pub fn parse_property(cypher_tree: Vec<&str>) -> HashMap<String, Box<Expression>> {
    let mut root: usize = 0;
    let mut count: usize = 0;
    let mut result = HashMap::new();
    let mut found = false;

    for i in cypher_tree.clone() {
        if i.contains("> binary operator") || i.contains("> comparison") {
            root = count;
            found = true;
            break;
        }
        count += 1;
    }

    let var_list = get_all_vars(&cypher_tree);

    let mut candidate_vars = HashSet::new();

    for i in root..cypher_tree.len() - 1 {
        let line: &str = cypher_tree[i];
        if line.contains("identifier") {
            let re = Regex::new(r"> identifier\s+`(?P<var_name>\w+)`").unwrap();
            if let Some(caps) = re.captures(line) {
                let var_name = &caps["var_name"];
                candidate_vars.insert(var_name.to_string());
            }
        }
    }

    for var in var_list {
        if found && candidate_vars.contains(&var) {
            let expression = match recursive_parser(&cypher_tree, root, var.as_str()) {
                Ok(exp) => exp,
                _ => empty_expression(),
            };
            result.insert(var.clone(), expression);
        } else {
            result.insert(var.clone(), empty_expression());
        }
    }
    result
}

fn get_all_vars(cypher_tree: &Vec<&str>) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();

    for line in cypher_tree {
        let re = Regex::new(r"> identifier\s+`(?P<var_name>\w+)`").unwrap();
        if let Some(caps) = re.captures(line) {
            let var_name = &caps["var_name"];
            if result.contains(&var_name.to_owned()) {
                continue;
            }
            result.push(var_name.to_owned());
        }
    }

    result
}

fn recursive_parser(
    cypher_tree: &Vec<&str>,
    index: usize,
    var: &str,
) -> PropertyResult<Box<Expression>> {
    if let Some(result) = match_val(cypher_tree, index, var)? {
        return Ok(result);
    } else if let Some(result) = match_var(cypher_tree, index, var)? {
        return Ok(result);
    } else if let Some(result) = match_operator(cypher_tree, index, var)? {
        return Ok(result);
    } else {
        panic!("Invalid cypher tree: {:?}", cypher_tree[index]);
    }
}

fn match_operator(
    cypher_tree: &Vec<&str>,
    index: usize,
    var: &str,
) -> PropertyResult<Option<Box<Expression>>> {
    let syntax: &str = cypher_tree[index];
    let re =
        Regex::new(r">.+\s+@(?P<left_index>\w+) (?P<operator>\S+) @(?P<right_index>\w+)").unwrap();
    if let Some(caps) = re.captures(syntax) {
        let left_index: usize = *(&caps["left_index"].parse::<usize>().unwrap());
        let right_index: usize = *(&caps["right_index"].parse::<usize>().unwrap());
        let operator_name = &caps["operator"];

        if vec!["AND", "OR", "<", "<=", ">", ">=", "=", "<>", "CONTAINS"].contains(&operator_name) {
            let operator = match &caps["operator"] {
                "AND" => PredicateOperator::AND,
                "OR" => PredicateOperator::OR,
                "<" => PredicateOperator::LessThan,
                ">" => PredicateOperator::GreaterThan,
                "<=" => PredicateOperator::LessEqual,
                ">=" => PredicateOperator::GreaterEqual,
                "=" => PredicateOperator::Equal,
                "<>" => PredicateOperator::NotEqual,
                "CONTAINS" => PredicateOperator::Contains,
                _ => panic!("Unknown predicate operator"),
            };
            let left = recursive_parser(cypher_tree, left_index, var);
            let right = recursive_parser(cypher_tree, right_index, var);
            if left.is_err() || right.is_err() {
                Ok(Some(empty_expression()))
            } else {
                Ok(Some(Box::new(PredicateExpression::new(
                    left.unwrap(),
                    right.unwrap(),
                    operator,
                ))))
            }
        } else {
            let operator = match &caps["operator"] {
                "+" => ArithmeticOperator::Add,
                "-" => ArithmeticOperator::Subtract,
                "*" => ArithmeticOperator::Multiply,
                "/" => ArithmeticOperator::Divide,
                "%" => ArithmeticOperator::Modulo,
                "^" => ArithmeticOperator::Power,
                _ => panic!("Unknown predicate operator"),
            };
            let left = recursive_parser(cypher_tree, left_index, var);
            let right = recursive_parser(cypher_tree, right_index, var);
            if left.is_err() || right.is_err() {
                Err(PropertyError::CrossComparisonError)
            } else {
                Ok(Some(Box::new(ArithmeticExpression::new(
                    left.unwrap(),
                    right.unwrap(),
                    operator,
                ))))
            }
        }
    } else {
        Ok(None)
    }
}

fn match_val(
    cypher_tree: &Vec<&str>,
    index: usize,
    _var: &str,
) -> PropertyResult<Option<Box<Expression>>> {
    let syntax: &str = cypher_tree[index];
    let re = Regex::new(r"> (?P<type_name>\w+)\W+?(?P<value>[\w.]+)").unwrap();
    if let Some(caps) = re.captures(syntax) {
        let type_name = &caps["type_name"];
        let value = &caps["value"];

        if type_name == "integer" {
            Ok(Some(Box::new(Const::new(json!(value
                .parse::<i32>()
                .unwrap())))))
        } else if type_name == "float" {
            Ok(Some(Box::new(Const::new(json!(value
                .parse::<f64>()
                .unwrap())))))
        } else if type_name == "string" {
            Ok(Some(Box::new(Const::new(JsonValue::String(
                value.to_string(),
            )))))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn match_var(
    cypher_tree: &Vec<&str>,
    index: usize,
    var: &str,
) -> PropertyResult<Option<Box<Expression>>> {
    let syntax: &str = cypher_tree[index];
    let re = Regex::new(r"> property\s+@(?P<var_index>\w+)\.@(?P<attribute_index>\w+)").unwrap();
    if let Some(caps) = re.captures(syntax) {
        let name = get_var_name(cypher_tree, index);
        if name != var.to_string() {
            return Err(PropertyError::CrossComparisonError);
        }

        let attribute_index: usize = *(&caps["attribute_index"].parse::<usize>().unwrap());
        let attribute_line = cypher_tree[attribute_index];
        let re = Regex::new(r"> prop name\s+`(?P<attribute_name>\w+)`").unwrap();

        if let Some(caps) = re.captures(attribute_line) {
            let attribute_name = &caps["attribute_name"];
            Ok(Some(Box::new(Var::new(attribute_name.to_string()))))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn get_var_name(cypher_tree: &Vec<&str>, index: usize) -> String {
    let syntax: &str = cypher_tree[index];
    let re = Regex::new(r"> property\s+@(?P<var_index>\w+)\.@(?P<attribute_index>\w+)").unwrap();
    if let Some(caps) = re.captures(syntax) {
        let var_index: usize = *(&caps["var_index"].parse::<usize>().unwrap());
        let var_line = cypher_tree[var_index];
        let re = Regex::new(r"> identifier\s+`(?P<var_name>\w+)`").unwrap();
        if let Some(caps) = re.captures(var_line) {
            let var_name = &caps["var_name"];
            var_name.to_owned()
        } else {
            "".to_owned()
        }
    } else {
        "".to_owned()
    }
}
