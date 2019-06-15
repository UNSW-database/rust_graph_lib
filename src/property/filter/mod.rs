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

pub mod arithmetic_expression;
pub mod edge_property_filter;
pub mod expression_operator;
pub mod hash_property_cache;
pub mod node_property_filter;
pub mod predicate_expression;
pub mod property_cache;
pub mod value_expression;
pub mod lru_cache;

use generic::IdType;
use serde_json::json;
use serde_json::Value as JsonValue;
use std::borrow::Cow;

use property::PropertyError;

pub use property::filter::arithmetic_expression::ArithmeticExpression;
pub use property::filter::edge_property_filter::filter_edge;
pub use property::filter::expression_operator::{ArithmeticOperator, PredicateOperator};
pub use property::filter::hash_property_cache::{HashEdgeCache, HashNodeCache};
pub use property::filter::node_property_filter::filter_node;
pub use property::filter::predicate_expression::PredicateExpression;
pub use property::filter::property_cache::PropertyCache;
pub use property::filter::value_expression::{Const, Var};

pub type PropertyResult<T> = Result<T, PropertyError>;

pub fn empty_expression() -> Box<Expression> {
    Box::new(Const::new(json!(true)))
}

pub trait Expression {
    // Get the result of expression as a Json Value.
    fn get_value<'a>(&'a self, var: &'a JsonValue) -> PropertyResult<Cow<'a, JsonValue>>;

    fn box_clone(&self) -> Box<Expression>;

    fn is_empty(&self) -> bool;
}

impl Clone for Box<Expression> {
    fn clone(&self) -> Box<Expression> {
        self.box_clone()
    }
}

impl PartialEq for Box<Expression> {
    fn eq(&self, _other: &Box<Expression>) -> bool {
        true
    }
}

impl Eq for Box<Expression> {}

pub trait NodeCache<Id: IdType> {
    fn get(&self, id: Id) -> PropertyResult<&JsonValue>;

    fn set(&mut self, id: Id, value: JsonValue) -> bool;

    fn get_mut(&mut self, id: Id) -> PropertyResult<&mut JsonValue>;
}

pub trait EdgeCache<Id: IdType> {
    fn get(&self, src: Id, dst: Id) -> PropertyResult<&JsonValue>;

    fn set(&mut self, src: Id, dst: Id, value: JsonValue) -> bool;

    fn get_mut(&mut self, src: Id, dst: Id) -> PropertyResult<&mut JsonValue>;
}
