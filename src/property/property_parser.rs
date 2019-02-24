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

fn parse_property(cypher_tree: Vec<&str>) -> Box<Expression> {
    let mut root: usize = 0;
    let mut count: usize = 0;
    for i in cypher_tree.clone() {
        if i.contains("> operator") || i.contains("> comparison") {
            root = count;
            break;
        }
        count += 1;
    }

    recursive_parser(&cypher_tree, root)
}


fn recursive_parser(cypher_tree: &Vec<&str>, index: usize) -> Box<Expression> {
    if let Some(result) = match_val(cypher_tree, index) {
        result
    } else if let Some(result) = match_var(cypher_tree, index) {
        result
    } else if let Some(result) = match_operator(cypher_tree, index) {
        result
    } else {
        panic!("Invalid cypher tree");
    }
}


fn match_operator(cypher_tree: &Vec<&str>, index: usize) -> Option<Box<Expression>> {
    let syntax: &str = cypher_tree[index];
    let re = Regex::new(r">.+\s+\@(?P<left_index>\w+) (?P<operator>\S+) \@(?P<right_index>\w+)").unwrap();
    if let Some(caps) = re.captures(syntax) {
        let left_index: usize = *(&caps["left_index"].parse::<usize>().unwrap());
        let right_index: usize = *(&caps["right_index"].parse::<usize>().unwrap());
        let operator_name = &caps["operator"];

        if vec!["AND", "OR", "LessThan"].contains(&operator_name) {
            let operator = match &caps["operator"] {
                "AND" => PredicateOperator::AND,
                "OR" => PredicateOperator::OR,
                "<" => PredicateOperator::LessThan,
                _ => panic!("Unknown predicate operator")
            };
            Some(Box::new(PredicateExpression::new(recursive_parser(cypher_tree, left_index).as_ref(), recursive_parser(cypher_tree, right_index).as_ref(), operator)))
        } else {
            let operator = match &caps["operator"] {
                "+" => ArithmeticOperator::Add,
                "-" => ArithmeticOperator::Subtract,
                _ => panic!("Unknown predicate operator")
            };
            Some(Box::new(ArithmeticExpression::new(recursive_parser(cypher_tree, left_index).as_ref(), recursive_parser(cypher_tree, right_index).as_ref(), operator)))
        }
    } else {
        None
    }
}

fn match_val(cypher_tree: &Vec<&str>, index: usize) -> Option<Box<Expression>> {
    let syntax: &str = cypher_tree[index];
    let re = Regex::new(r"> (?P<type_name>\w+)\s+(?P<value>\w+)").unwrap();
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
            Some(Box::new(Const::new(JsonValue::Null)))
        }
    } else {
        None
    }
}

fn match_var(cypher_tree: &Vec<&str>, index: usize) -> Option<Box<Expression>> {
    let syntax: &str = cypher_tree[index];
    let re = Regex::new(r"> property\s+\@(?P<var_index>\w+)\.\@(?P<attribute_index>\w+)").unwrap();
    if let Some(caps) = re.captures(syntax) {
        let var_index: usize = *(&caps["var_index"].parse::<usize>().unwrap());
        let attribute_index: usize = *(&caps["attribute_index"].parse::<usize>().unwrap());
        let attribute_line = cypher_tree[attribute_index];
        let re = Regex::new(r"> prop name\s+\`(?P<attribute_name>\w+)\`").unwrap();

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