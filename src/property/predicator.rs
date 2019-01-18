///*
// * Copyright (c) 2018 UNSW Sydney, Data and Knowledge Group.
// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//use json::JsonValue;
//
//use property::{PropertyGraph, SledProperty};
//use generic::IdType;
//
//use std::collections::HashMap;
//
//
//pub trait StringExpression<Id: IdType> {
//    fn get_string_result(&self, id_map: HashMap<usize, Id>, property_graph: &PropertyGraph<Id>) -> String;
//}
//
//
//pub trait BoolExpression<Id: IdType> {
//    fn get_bool_result(&self, id_map: HashMap<usize, Id>, property_graph: &PropertyGraph<Id>) -> bool;
//}
//
//
//pub trait NumExpression<Id: IdType> {
//    fn get_num_result(&self, id_map: HashMap<usize, Id>, property_graph: &PropertyGraph<Id>) -> f64;
//}
//
//
//pub struct Var {
//    id: usize,
//    attribute: String
//}
//
//
//pub struct Const {
//    value: JsonValue
//}
//
//
//// BoolExpression
//pub struct NumCompare<Id: IdType> {
//    left: NumExpression<Id>,
//    right: NumExpression<Id>,
//    operator: String
//}
//
//
//pub struct StringCompare<Id: IdType> {
//    left: StringExpression<Id>,
//    right: StringExpression<Id>,
//    operator: String
//}
//
//
//pub struct MathOperation<Id: IdType> {
//
//
//}
//
//
//pub struct StringOperation<Id: IdType> {
//
//
//}
//
//
//
//
//
//
//
//
////
////pub struct SingleNumPredicator<Id: IdType> {
////
////    field: String,
////    // <, =, >, <=, >=, !=
////    operator: String,
////
////    value: f64
////}
////
////
////impl<Id: IdType> SingleNumPredicator<Id> {
////
////    pub fn new(field: String, operator: String, value: f64) -> Self {
////        SingleNumPredicator {
////            field,
////            operator,
////            value
////        }
////    }
////
////    fn filter(&self, value: f64) {
////        match self.operator {
////            "<" => value < self.value,
////
////            "=" => value == self.value,
////
////            ">" => value > self.value,
////
////            "<=" => value <= self.value,
////
////            ">=" => value >= self.value,
////
////            "!=" => value != self.value,
////
////            _ => panic!("Invalid operator")
////        }
////    }
////
////}
////
////
//impl<Id: IdType> Predicator<Id> for SingleNumPredicator<Id> {
//    fn filter_nodes(&self, nodes: Vec<Id>, property_graph: &PropertyGraph<Id>) -> Vec<Id> {
//        let mut filtered_nodes: Vec<Id> = Vec::new();
//        for node in nodes {
//            if Ok(Some(result)) = property_graph.get_node_property(node, vec![self.field.to_owned()]) {
//                if self.filter(result[self.field]) {
//                    filtered_nodes.push(node);
//                }
//            }
//        }
//        filtered_nodes
//    }
//
//    fn get_field(&self) -> String {
//        self.field.clone()
//    }
////}
////
////
////pub struct RangeNumPredicator<Id: IdType> {
////
////    field: String,
////
////    operator: (String, String),
////
////    value: (f64, f64)
////}
////
////
////impl<Id: IdType> RangeNumPredicator<Id> {
////
////    pub fn new(field: String, operator: (String, String), value: (f64, f64)) -> Self {
////        RangeNumPredicator {
////            field,
////            operator,
////            value
////        }
////    }
////
////    fn filter(&self, value: f64) {
////        match self.operator {
////            ("<", "<") => value < self.value.1 && value > self.value.0,
////
////            ("<", "<=") => value <= self.value.1 && value > self.value.0,
////
////            ("<=", "<") => value < self.value.1 && value >= self.value.0,
////
////            ("<=", "<=") => value <= self.value.1 && value >= self.value.0,
////
////            _ => panic!("Invalid operator")
////        }
////    }
////
////}
////
////
////impl<Id: IdType> Predicator<Id> for RangeNumPredicator<Id> {
////    fn filter_nodes(&self, nodes: Vec<Id>, property_graph: &PropertyGraph<Id>) -> Vec<Id> {
////        let mut filtered_nodes: Vec<Id> = Vec::new();
////        for node in nodes {
////            if Ok(Some(result)) = property_graph.get_node_property(node, vec![self.field.to_owned()]) {
////                if self.filter(result[self.field]) {
////                    filtered_nodes.push(node);
////                }
////            }
////        }
////        filtered_nodes
////    }
////
////    fn get_field(&self) -> String {
////        self.field.clone()
////    }
////}
////
////
////#[cfg(test)]
////mod test {
////    use super::*;
////    use json::{array, object};
////    use std::collections::HashMap;
////
////    #[test]
////    fn test_single_num_operator() {
////        let mut node_property = HashMap::new();
////        let mut edge_property = HashMap::new();
////
////        node_property.insert(
////            0u32,
////            object!(
////            "name"=>"John",
////            "age"=>12,
////            "is_member"=>true,
////            "scores"=>array![9,8,10],
////            ),
////        );
////
////        node_property.insert(
////            1,
////            object!(
////            "name"=>"Marry",
////            "age"=>13,
////            "is_member"=>false,
////            "scores"=>array![10,10,9],
////            ),
////        );
////
////        edge_property.insert(
////            (0, 1),
////            object!(
////            "friend_since"=>"2018-11-15",
////            ),
////        );
////
////        let path = Path::new("/home/wangran/RustProjects/PatMatch/PropertyGraph/test_data/undirected");
////        let graph = SledProperty::with_data(path,node_property.into_iter(),
////                                            edge_property.into_iter(), false).unwrap();
////
////        let single_num_predicator = SingleNumPredicator::new("age", ">", 12);
////        let mut ids = vec![0, 1];
////        let result = single_num_predicator.filter_nodes(ids, graph);
////        assert_eq!(result, vec![1]);
////    }
////
////    fn test_range_num_operator() {
////        let mut node_property = HashMap::new();
////        let mut edge_property = HashMap::new();
////
////        node_property.insert(
////            0u32,
////            object!(
////            "name"=>"John",
////            "age"=>12,
////            "is_member"=>true,
////            "scores"=>array![9,8,10],
////            ),
////        );
////
////        node_property.insert(
////            1,
////            object!(
////            "name"=>"Marry",
////            "age"=>13,
////            "is_member"=>false,
////            "scores"=>array![10,10,9],
////            ),
////        );
////
////        edge_property.insert(
////            (0, 1),
////            object!(
////            "friend_since"=>"2018-11-15",
////            ),
////        );
////
////        let path = Path::new("/home/wangran/RustProjects/PatMatch/PropertyGraph/test_data/undirected");
////        let graph = SledProperty::with_data(path,node_property.into_iter(),
////                                            edge_property.into_iter(), false).unwrap();
////
////        let range_num_predicator = RangeNumPredicator::new("age", ("<", "<="), (5, 12));
////        let mut ids = vec![0, 1];
////        let result = range_num_predicator.filter_nodes(ids, graph);
////        assert_eq!(result, vec![1]);
////    }
////
////
////}