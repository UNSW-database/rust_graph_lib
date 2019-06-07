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
use std::borrow::Cow;
use property::filter::{Expression, PropertyResult};
use property::PropertyError;

use serde_json::Value as JsonValue;
use serde_json::json;

pub struct Var {
    // queried attribute
    attribute: String,
}

impl Var {
    pub fn new(attribute: String) -> Self {
        Var { attribute }
    }
}

impl Expression for Var {
    // Get value of queried attribute of node
    fn get_value<'a>(&'a self, var: &'a JsonValue) -> PropertyResult<Cow<'a,JsonValue>> {
        let result_option = var.get(&self.attribute);
        if let Some(result) = result_option {
            Ok(Cow::Borrowed(result))
        } else {
            Err(PropertyError::JsonObjectFieldError)
        }


    }

    fn box_clone(&self) -> Box<Expression> {
        Box::new(Var::new(self.attribute.clone()))
    }

    fn is_empty(&self) -> bool {
        false
    }
}

pub struct Const {
    // value of constant defined in query
    value: JsonValue,
}

impl Const {
    pub fn new(value: JsonValue) -> Self {
        Const { value }
    }
}

impl Expression for Const {
    // get the value of constant
    fn get_value<'a>(&'a self, _var: &'a JsonValue) -> PropertyResult<Cow<'a, JsonValue>> {
        Ok(Cow::Borrowed(&self.value))
    }

    fn box_clone(&self) -> Box<Expression> {
        Box::new(Const::new(self.value.clone()))
    }

    fn is_empty(&self) -> bool {
        if self.value == json!(true) {
            true
        } else {
            false
        }
    }
}
