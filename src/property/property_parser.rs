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
use json::JsonValue;
use json::number::Number;
use property::filter::{Expression, Var, PredicateExpression, ArithmeticExpression, Const, PredicateOperator, ArithmeticOperator};
use regex::Regex;
use std::collections::HashMap;
use property::filter::empty_expression;
use std::time::Instant;

pub fn parse_property_tree(cypher_tree: Vec<String>) -> (HashMap<usize, Box<Expression>>, HashMap<(usize, usize), Box<Expression>>) {
    // edge_id = (dst_id + 1) * count("node pattern") + (src_id)
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
        println!("Current key is: {}", key);
        let id: usize = key.parse::<usize>().unwrap();
        if id < node_count {
            node_property.insert(id, all_property[key].clone());
        } else {
            let dst = id % node_count;
            let src = (id - dst) / node_count - 1;
            edge_property.insert((src, dst), all_property[key].clone());
        }
    }

    println!("Node keys: {:?}", node_property.keys());
    println!("Edge keys: {:?}", edge_property.keys());


    (node_property, edge_property)

}


pub fn parse_property(cypher_tree: Vec<&str>) -> HashMap<String, Box<Expression>> {
    let mut root: usize = 0;
    let mut count: usize = 0;
    let mut result = HashMap::new();
    let mut found = false;

    let instant = Instant::now();
    for i in cypher_tree.clone() {
        if i.contains("> binary operator") || i.contains("> comparison") {
            root = count;
            found = true;
            break;
        }
        count += 1;
    }
    println!("Determine start: {:?}", instant.elapsed());

    let instant = Instant::now();
    let var_list = get_all_vars(&cypher_tree);
    println!("Collect all vars: {:?}", instant.elapsed());

    let instant = Instant::now();
    for var in var_list {
        if found {
            result.insert(var.clone(), recursive_parser(&cypher_tree, root, var.as_str()));
        } else {
            result.insert(var.clone(), empty_expression());
        }
    }
    println!("Build parser: {:?}", instant.elapsed());

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

fn is_other_var(cypher_tree: &Vec<&str>, index: usize, var: &str) -> bool {
    let mut result = true;
    let syntax: &str = cypher_tree[index];

    let re = Regex::new(r">.+\s+@(?P<left_index>\w+) (?P<operator>\S+) @(?P<right_index>\w+)").unwrap();
    if let Some(caps) = re.captures(syntax) {

        let left_index: usize = *(&caps["left_index"].parse::<usize>().unwrap());
        let right_index: usize = *(&caps["right_index"].parse::<usize>().unwrap());

        let l_result = is_other_var(cypher_tree, left_index, var);
        let r_result = is_other_var(cypher_tree, right_index, var);
        let operator_name = &caps["operator"];


        if vec!["AND", "OR"].contains(&operator_name) {
            return l_result && r_result;
        } else if l_result || r_result {
            return true;
        } else {
            return false;
        }
    }

    let re = Regex::new(r"> property\s+@(?P<var_index>\w+)\.@(?P<attribute_index>\w+)").unwrap();
    if let Some(caps) = re.captures(syntax) {

        let name = get_var_name(cypher_tree, index);

        if var == name.as_str() {
            return false;
        } else {
            return true;
        }
    }

    let re = Regex::new(r"> (?P<type_name>\w+)\W+?(?P<value>[\w.]+)").unwrap();
    if let Some(caps) = re.captures(syntax) {

        let type_name = &caps["type_name"];
        let value = &caps["value"];

        return false;
    }

    panic!("Invalid cypher tree");
    return true;
}


fn recursive_parser(cypher_tree: &Vec<&str>, index: usize, var: &str) -> Box<Expression> {
    if is_other_var(cypher_tree, index, var) {
        Box::new(Const::new(JsonValue::Boolean(true)))
    } else {
        if let Some(result) = match_val(cypher_tree, index, var) {
            return result;
        } else if let Some(result) = match_var(cypher_tree, index, var) {
            return result;
        } else if let Some(result) = match_operator(cypher_tree, index, var) {
            return result;
        } else {
            println!("Invalid: {:?}", cypher_tree[index]);
            panic!("Invalid cypher tree");
        }
    }
}


fn match_operator(cypher_tree: &Vec<&str>, index: usize, var: &str) -> Option<Box<Expression>> {
    let syntax: &str = cypher_tree[index];
    let re = Regex::new(r">.+\s+@(?P<left_index>\w+) (?P<operator>\S+) @(?P<right_index>\w+)").unwrap();
    if let Some(caps) = re.captures(syntax) {
        let left_index: usize = *(&caps["left_index"].parse::<usize>().unwrap());
        let right_index: usize = *(&caps["right_index"].parse::<usize>().unwrap());
        let operator_name = &caps["operator"];

        if vec!["AND", "OR", "<", "<=", ">", ">=", "=", "<>", "CONTAINS", ].contains(&operator_name) {
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
                _ => panic!("Unknown predicate operator")
            };
            Some(Box::new(PredicateExpression::new(recursive_parser(cypher_tree, left_index, var), recursive_parser(cypher_tree, right_index, var), operator)))
        } else {
            let operator = match &caps["operator"] {
                "+" => ArithmeticOperator::Add,
                "-" => ArithmeticOperator::Subtract,
                "*" => ArithmeticOperator::Multiply,
                "/" => ArithmeticOperator::Divide,
                "%" => ArithmeticOperator::Modulo,
                "^" => ArithmeticOperator::Power,
                _ => panic!("Unknown predicate operator")
            };
            Some(Box::new(ArithmeticExpression::new(recursive_parser(cypher_tree, left_index, var), recursive_parser(cypher_tree, right_index, var), operator)))
        }
    } else {
        None
    }
}

fn match_val(cypher_tree: &Vec<&str>, index: usize, var: &str) -> Option<Box<Expression>> {
    let syntax: &str = cypher_tree[index];
    let re = Regex::new(r"> (?P<type_name>\w+)\W+?(?P<value>[\w.]+)").unwrap();
    if let Some(caps) = re.captures(syntax) {
        let type_name = &caps["type_name"];
        let value = &caps["value"];

        if type_name == "integer" {
            Some(Box::new(Const::new(JsonValue::Number(Number::from(value.parse::<i32>().unwrap())))))
        } else if type_name == "float" {
            Some(Box::new(Const::new(JsonValue::Number(Number::from(value.parse::<f64>().unwrap())))))
        } else if type_name == "string" {
            Some(Box::new(Const::new(JsonValue::String(value.to_string()))))
        } else {
            None
        }
    } else {
        None
    }
}

fn match_var(cypher_tree: &Vec<&str>, index: usize, var: &str) -> Option<Box<Expression>> {
    let syntax: &str = cypher_tree[index];
    let re = Regex::new(r"> property\s+@(?P<var_index>\w+)\.@(?P<attribute_index>\w+)").unwrap();
    if let Some(caps) = re.captures(syntax) {
        let var_index: usize = *(&caps["var_index"].parse::<usize>().unwrap());
        let attribute_index: usize = *(&caps["attribute_index"].parse::<usize>().unwrap());
        let attribute_line = cypher_tree[attribute_index];
        let re = Regex::new(r"> prop name\s+`(?P<attribute_name>\w+)`").unwrap();

        if let Some(caps) = re.captures(attribute_line) {
            let attribute_name = &caps["attribute_name"];
            Some(Box::new(Var::new(attribute_name.to_string())))
        } else {
            None
        }
    } else {
        None
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