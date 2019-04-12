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

use property::filter::expression_operator::*;
use property::filter::Expression;
use property::filter::PropertyResult;

use json::JsonValue;

pub struct ArithmeticExpression {
    // expression on the LHS of operator
    left: Box<Expression>,
    // expression on the RHS of operator
    right: Box<Expression>,
    // operator used in predicator
    operator: ArithmeticOperator,
}

impl ArithmeticExpression {
    pub fn new(
        left: Box<Expression>,
        right: Box<Expression>,
        operator: ArithmeticOperator,
    ) -> Self {
        ArithmeticExpression {
            left,
            right,
            operator,
        }
    }
}

impl Expression for ArithmeticExpression {
    // Return the resulting value of expression.
    // Firstly get the values of expressions on both sides.
    // Then calculate the result based on operator.
    fn get_value(&self, var: &JsonValue) -> PropertyResult<JsonValue> {
        // Get values of left and right expressions.
        let exp1 = self.left.get_value(var)?;
        let exp2 = self.right.get_value(var)?;

        // Perform operator on left and right values.
        match self.operator {
            // Mathematical Operation
            ArithmeticOperator::Add => add(exp1, exp2),
            ArithmeticOperator::Subtract => subtract(exp1, exp2),
            ArithmeticOperator::Multiply => multiply(exp1, exp2),
            ArithmeticOperator::Divide => divide(exp1, exp2),
            ArithmeticOperator::Modulo => modulo(exp1, exp2),
            ArithmeticOperator::Power => power(exp1, exp2),

            // String Operation
            ArithmeticOperator::Concat => concat(exp1, exp2),
        }
    }

    fn box_clone(&self) -> Box<Expression> {
        Box::new(ArithmeticExpression::new(
            self.left.clone(),
            self.right.clone(),
            self.operator.clone(),
        ))
    }
}
