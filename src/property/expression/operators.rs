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


pub enum Operator {
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

    // String Comparison
    Contains,
    StartsWith,
    EndsWith,
    Regex,
    // Not Supported

    // Mathematical Operation
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,

    // String Operation
    Concat,

    // Other
    IsNull,
    // Not Supported
    IsNotNull,
    // Not Supported
}


// Logical

pub fn and(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_bool().is_some() && exp2.as_bool().is_some() {
        Ok(JsonValue::Boolean(exp1.as_bool().unwrap() && exp2.as_bool().unwrap()))
    } else {
        Err("Invalid Json value, bool expected.")
    }
}

pub fn or(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_bool().is_some() && exp2.as_bool().is_some() {
        Ok(JsonValue::Boolean(exp1.as_bool().unwrap() || exp2.as_bool().unwrap()))
    } else {
        Err("Invalid Json value, bool expected.")
    }
}


// Numeric Comparison

pub fn less_than(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(JsonValue::Boolean(exp1.as_f64().unwrap() < exp2.as_f64().unwrap()))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn less_equal(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(JsonValue::Boolean(exp1.as_f64().unwrap() <= exp2.as_f64().unwrap()))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn greater_than(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(JsonValue::Boolean(exp1.as_f64().unwrap() > exp2.as_f64().unwrap()))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn greater_equal(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(JsonValue::Boolean(exp1.as_f64().unwrap() >= exp2.as_f64().unwrap()))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn equal(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(JsonValue::Boolean(exp1.as_f64().unwrap() == exp2.as_f64().unwrap()))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn not_equal(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        Ok(JsonValue::Boolean(exp1.as_f64().unwrap() != exp2.as_f64().unwrap()))
    } else {
        Err("Invalid Json value, number expected.")
    }
}


// String Comparison

pub fn contains(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_str().is_some() && exp2.as_str().is_some() {
        Ok(JsonValue::Boolean(exp1.as_str().unwrap().contains(exp2.as_str().unwrap())))
    } else {
        Err("Invalid Json value, string expected.")
    }
}

pub fn starts_with(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_str().is_some() && exp2.as_str().is_some() {
        Ok(JsonValue::Boolean(exp1.as_str().unwrap().starts_with(exp2.as_str().unwrap())))
    } else {
        Err("Invalid Json value, string expected.")
    }
}

pub fn ends_with(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_str().is_some() && exp2.as_str().is_some() {
        Ok(JsonValue::Boolean(exp1.as_str().unwrap().ends_with(exp2.as_str().unwrap())))
    } else {
        Err("Invalid Json value, string expected.")
    }
}


// Mathematical Operation

pub fn add(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number: Number = (exp1.as_f64().unwrap() + exp2.as_f64().unwrap()).into();
        Ok(JsonValue::Number(number))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn subtract(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number: Number = (exp1.as_f64().unwrap() - exp2.as_f64().unwrap()).into();
        Ok(JsonValue::Number(number))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn multiply(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number: Number = (exp1.as_f64().unwrap() * exp2.as_f64().unwrap()).into();
        Ok(JsonValue::Number(number))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn divide(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number: Number = (exp1.as_f64().unwrap() / exp2.as_f64().unwrap()).into();
        Ok(JsonValue::Number(number))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn modulo(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number: Number = (exp1.as_f64().unwrap() % exp2.as_f64().unwrap()).into();
        Ok(JsonValue::Number(number))
    } else {
        Err("Invalid Json value, number expected.")
    }
}

pub fn power(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_f64().is_some() && exp2.as_f64().is_some() {
        let number: Number = (exp1.as_f64().unwrap().powf(exp2.as_f64().unwrap())).into();
        Ok(JsonValue::Number(number))
    } else {
        Err("Invalid Json value, number expected.")
    }
}


// String Operation

pub fn concat(exp1: JsonValue, exp2: JsonValue) -> Result<JsonValue, &'static str> {
    if exp1.as_str().is_some() && exp2.as_str().is_some() {
        let mut result = exp1.as_str().unwrap().to_owned();
        result.push_str(exp2.as_str().unwrap());
        Ok(JsonValue::String(result))
    } else {
        Err("Invalid Json value, string expected.")
    }
}
