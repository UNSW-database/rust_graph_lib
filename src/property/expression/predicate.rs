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

use property::expression::Expression;
use property::expression::operators::*;

use json::JsonValue;


pub struct Predicate {
    // expression on the LHS of operator
    left: Expression,
    // expression on the RHS of operator
    right: Expression,
    // operator used in predicator
    operator: Operator
}

impl Predicate {

    pub fn new(left: Expression, right: Expression, operator: Operator) -> Self {
        Predicate {
            left,
            right,
            operator
        }
    }
}

impl Expression for Predicate {
    // Return the resulting value of expression.
    // Firstly get the values of expressions on both sides.
    // Then calculate the result based on operator.
    fn get_value(&self, var: JsonValue) -> Result<JsonValue, &'static str> {
        // Get values of left and right expressions.
        let exp1 = self.left.get_value(var)?;
        let exp2 = self.right.get_value(var)?;

        // Perform operator on left and right values.
        match self.operator {
            // Logical
            Operator::AND           =>     and(exp1, exp2),
            Operator::OR            =>     or(exp1, exp2),

            // Numeric Comparison
            Operator::LessThan      =>     less_than(exp1, exp2),
            Operator::LessEqual     =>     less_equal(exp1, exp2),
            Operator::GreaterThan   =>     greater_than(exp1, exp2),
            Operator::GreaterEqual  =>     greater_equal(exp1, exp2),
            Operator::Equal         =>     equal(exp1, exp2),
            Operator::NotEqual      =>     not_equal(exp1, exp2),

            // String Comparison
            Operator::Contains      =>     contains(exp1, exp2),
            Operator::StartsWith    =>     starts_with(exp1, exp2),
            Operator::EndsWith      =>     ends_with(exp1, exp2),

            // Mathematical Operation
            Operator::Add           =>     add(exp1, exp2),
            Operator::Subtract      =>     subtract(exp1, exp2),
            Operator::Multiply      =>     multiply(exp1, exp2),
            Operator::Divide        =>     divide(exp1, exp2),
            Operator::Modulo        =>     modulo(exp1, exp2),
            Operator::Power         =>     power(exp1, exp2),

            // String Operation
            Operator::Concat        =>     concat(exp1, exp2)
        }
    }

    fn get_attribute(&self) -> String {
        let mut result = self.left.get_attribute();
        result.push_str(&self.right.get_attribute());
        result
    }
}