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

use property::filter::PropertyResult;
use property::PropertyError;

use serde_json::json;
use serde_json::Value as JsonValue;

#[derive(Clone, Copy)]
pub enum PredicateOperator {
    // Logical
    AND,
    OR,
    XOR,
    // Not Supported
    NOT,
    // Not Supported

    // Numeric Comparison
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
    Equal,
    NotEqual,
    Range,

    // String Comparison
    Contains,
    StartsWith,
    EndsWith,
    Regex,
    // Not Supported

    // Other
    IsNull,
    // Not Supported
    IsNotNull,
    // Not Supported
    Exists,
    // Not Supported
}

#[derive(Clone, Copy)]
pub enum ArithmeticOperator {
    // Mathematical Operation
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,

    // String Operation
    Concat,
}

// Logical

pub fn and(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_bool().is_some() && exp2.as_bool().is_some() {
        Ok(json!(exp1.as_bool().unwrap() && exp2.as_bool().unwrap()))
    } else {
        Err(PropertyError::BooleanExpressionError)
    }
}

pub fn or(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_bool().is_some() && exp2.as_bool().is_some() {
        Ok(json!(exp1.as_bool().unwrap() || exp2.as_bool().unwrap()))
    } else {
        Err(PropertyError::BooleanExpressionError)
    }
}

// Numeric Comparison

pub fn less_than(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        Ok(json!(exp1.as_u64().unwrap() < exp2.as_u64().unwrap()))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(json!(exp1.as_f64().unwrap() < exp2.as_f64().unwrap()))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

pub fn less_equal(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        Ok(json!(exp1.as_u64().unwrap() <= exp2.as_u64().unwrap()))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(json!(exp1.as_f64().unwrap() <= exp2.as_f64().unwrap()))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

pub fn greater_than(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        Ok(json!(exp1.as_u64().unwrap() > exp2.as_u64().unwrap()))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(json!(exp1.as_f64().unwrap() > exp2.as_f64().unwrap()))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

pub fn greater_equal(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        Ok(json!(exp1.as_u64().unwrap() >= exp2.as_u64().unwrap()))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(json!(exp1.as_f64().unwrap() >= exp2.as_f64().unwrap()))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

pub fn equal(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    Ok(json!(exp1 == exp2))
}

pub fn not_equal(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    Ok(json!(exp1 != exp2))
}

pub fn range(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.is_array() && exp2.as_f64().is_some() {
        let value = exp2.as_f64().unwrap();
        let lower = exp1[0].as_f64();
        let upper = exp1[1].as_f64();
        if lower.is_some() && upper.is_some() {
            Ok(json!(value >= lower.unwrap() && value <= upper.unwrap()))
        } else {
            Err(PropertyError::NumberExpressionError)
        }
    } else if exp2.is_array() && exp1.as_f64().is_some() {
        let value = exp1.as_f64().unwrap();
        let lower = exp2[0].as_f64();
        let upper = exp2[1].as_f64();
        if lower.is_some() && upper.is_some() {
            Ok(json!(value >= lower.unwrap() && value <= upper.unwrap()))
        } else {
            Err(PropertyError::NumberExpressionError)
        }
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

// String Comparison

pub fn contains(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_str().is_some() && exp2.as_str().is_some() {
        Ok(json!(exp1
            .as_str()
            .unwrap()
            .contains(exp2.as_str().unwrap())))
    } else {
        Err(PropertyError::StringExpressionError)
    }
}

pub fn starts_with(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_str().is_some() && exp2.as_str().is_some() {
        Ok(json!(exp1
            .as_str()
            .unwrap()
            .starts_with(exp2.as_str().unwrap())))
    } else {
        Err(PropertyError::StringExpressionError)
    }
}

pub fn ends_with(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_str().is_some() && exp2.as_str().is_some() {
        Ok(json!(exp1
            .as_str()
            .unwrap()
            .ends_with(exp2.as_str().unwrap())))
    } else {
        Err(PropertyError::StringExpressionError)
    }
}

// Mathematical Operation

pub fn add(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        let number = exp1.as_u64().unwrap() + exp2.as_u64().unwrap();
        Ok(json!(number))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number = exp1.as_f64().unwrap() + exp2.as_f64().unwrap();
        Ok(json!(number))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

pub fn subtract(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        let number = exp1.as_u64().unwrap() - exp2.as_u64().unwrap();
        Ok(json!(number))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number = exp1.as_f64().unwrap() - exp2.as_f64().unwrap();
        Ok(json!(number))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

pub fn multiply(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        let number = exp1.as_u64().unwrap() * exp2.as_u64().unwrap();
        Ok(json!(number))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number = exp1.as_f64().unwrap() * exp2.as_f64().unwrap();
        Ok(json!(number))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

pub fn divide(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        let number = exp1.as_u64().unwrap() / exp2.as_u64().unwrap();
        Ok(json!(number))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number = exp1.as_f64().unwrap() / exp2.as_f64().unwrap();
        Ok(json!(number))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

pub fn modulo(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        let number = exp1.as_u64().unwrap() % exp2.as_u64().unwrap();
        Ok(json!(number))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number = exp1.as_f64().unwrap() % exp2.as_f64().unwrap();
        Ok(json!(number))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

pub fn power(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_u64().is_some() && exp2.as_u64().is_some() {
        let number = exp1.as_u64().unwrap().pow(exp2.as_u64().unwrap() as u32);
        Ok(json!(number))
    } else if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number = exp1.as_f64().unwrap().powf(exp2.as_f64().unwrap());
        Ok(json!(number))
    } else {
        Err(PropertyError::NumberExpressionError)
    }
}

// String Operation

pub fn concat(exp1: JsonValue, exp2: JsonValue) -> PropertyResult<JsonValue> {
    if exp1.as_str().is_some() && exp2.as_str().is_some() {
        let mut result = exp1.as_str().unwrap().to_owned();
        result.push_str(exp2.as_str().unwrap());
        Ok(JsonValue::String(result))
    } else {
        Err(PropertyError::StringExpressionError)
    }
}
