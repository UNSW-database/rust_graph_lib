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

use generic::IdType;
use property::expression::Expression;

use json::JsonValue;


pub struct Var {
    // queried attribute
    attribute: String
}

impl Var {
    pub fn new(attribute: String) -> Self {
        Var {
            attribute
        }
    }
}

impl Expression for Var {
    // Get value of queried attribute of node
    fn get_value<Id: IdType>(&self, var: JsonValue) -> Result<JsonValue, &'static str> {
        Ok(var)
    }

    fn get_attribute(&self) -> String {
        self.attribute
    }
}


pub struct Const {
    // value of constant defined in query
    value: JsonValue
}

impl Const {
    pub fn new(value: JsonValue) -> Self {
        Const {
            value
        }
    }
}

impl Expression for Const {
    // get the value of constant
    fn get_value<Id: IdType>(&self, var: JsonValue) -> Result<JsonValue, &'static str> {
        Ok(self.value)
    }

    fn get_attribute(&self) -> String {
        ""
    }
}
