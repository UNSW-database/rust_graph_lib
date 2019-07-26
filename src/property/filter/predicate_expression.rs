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
use crate::property::filter::expression_operator::*;
use crate::property::filter::{Expression, PropertyResult};
use std::borrow::Cow;

use serde_json::Value as JsonValue;

pub struct PredicateExpression {
    // expression on the LHS of operator
    left: Box<dyn Expression>,
    // expression on the RHS of operator
    right: Box<dyn Expression>,
    // operator used in predicator
    operator: PredicateOperator,
}

impl PredicateExpression {
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>, operator: PredicateOperator) -> Self {
        PredicateExpression {
            left,
            right,
            operator,
        }
    }
}

impl Expression for PredicateExpression {
    // Return the resulting value of expression.
    // Firstly get the values of expressions on both sides.
    // Then calculate the result based on operator.
    fn get_value<'a>(&'a self, var: &'a JsonValue) -> PropertyResult<Cow<'a, JsonValue>> {
        // Get values of left and right expressions.
        let exp1_cow = self.left.get_value(&var)?;
        let exp2_cow = self.right.get_value(&var)?;
        let exp1 = exp1_cow.as_ref();
        let exp2 = exp2_cow.as_ref();

        // Perform operator on left and right values.
        let result = match self.operator {
            // Logical
            PredicateOperator::AND => and(exp1, exp2),
            PredicateOperator::OR => or(exp1, exp2),

            // Numeric Comparison
            PredicateOperator::LessThan => less_than(exp1, exp2),
            PredicateOperator::LessEqual => less_equal(exp1, exp2),
            PredicateOperator::GreaterThan => greater_than(exp1, exp2),
            PredicateOperator::GreaterEqual => greater_equal(exp1, exp2),
            PredicateOperator::Equal => equal(exp1, exp2),
            PredicateOperator::NotEqual => not_equal(exp1, exp2),

            // String Comparison
            PredicateOperator::Contains => contains(exp1, exp2),
            PredicateOperator::StartsWith => starts_with(exp1, exp2),
            PredicateOperator::EndsWith => ends_with(exp1, exp2),
            PredicateOperator::Range => range(exp1, exp2),

            // Temporary place holder
            _ => ends_with(exp1, exp2),
        }?;
        Ok(Cow::Owned(result))
    }

    fn box_clone(&self) -> Box<dyn Expression> {
        Box::new(PredicateExpression::new(
            self.left.clone(),
            self.right.clone(),
            self.operator.clone(),
        ))
    }
    fn is_empty(&self) -> bool {
        false
    }
}
