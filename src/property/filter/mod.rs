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

// 1. Change to refcell
// 2. Add error types
// 3. Change cache to store json object (all attributes)
// 4. Add comments
// 5. exp ma ager prefetch

// 1. edge p f/ n p f
// 2. possible errors listed

pub mod edge_property_filter;
pub mod node_property_filter;
pub mod predicate_expression;
pub mod value_expression;
pub mod arithmetic_expression;
pub mod expression_operator;
pub mod hash_property_cache;
pub mod filter_errors;

use generic::IdType;
use serde_json::Value;

use property::PropertyError;

pub use property::filter::expression_operator::{ArithmeticOperator, PredicateOperator};
pub use property::filter::predicate_expression::PredicateExpression;
pub use property::filter::value_expression::{Var, Const};
pub use property::filter::arithmetic_expression::ArithmeticExpression;
pub use property::filter::node_property_filter::NodeFilter;
pub use property::filter::edge_property_filter::EdgeFilter;
pub use property::filter::hash_property_cache::{HashNodeCache, HashEdgeCache};



type PropertyResult<T> = Result<T, PropertyError>;


pub trait Expression {
    // Get the result of expression as a Json Value.
    fn get_value(&self, var: &Value) -> PropertyResult<Value>;
}


pub trait NodeCache<Id: IdType> {

    fn get(&self, id:Id) -> PropertyResult<Value>;

    fn set(&mut self, id:Id, value: Value) -> bool;
}


pub trait EdgeCache<Id: IdType> {

    fn get(&self, src: Id, dst: Id) -> PropertyResult<Value>;

    fn set(&mut self, src: Id, dst: Id, value: Value) -> bool;
}

