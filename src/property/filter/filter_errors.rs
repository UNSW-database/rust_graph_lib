// *
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
//use std::{fmt, error};
//
//
//#[derive(Debug, Clone)]
//pub struct JsonObjectFieldError;
//
//impl fmt::Display for JsonObjectFieldError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        write!(f, "Json object has no such field")
//    }
//}
//
//impl error::Error for JsonObjectFieldError {
//    fn description(&self) -> &str {
//        "Json object has no such field"
//    }
//
//    fn cause(&self) -> Option<&error::Error> {
//        None
//    }
//}
//
//
//#[derive(Debug, Clone)]
//pub struct NodeNotFoundError;
//
//impl fmt::Display for NodeNotFoundError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        write!(f, "Node is found found in cache")
//    }
//}
//
//impl error::Error for NodeNotFoundError {
//    fn description(&self) -> &str {
//        "Node is found found in cache"
//    }
//
//    fn cause(&self) -> Option<&error::Error> {
//        None
//    }
//}
//
//
//#[derive(Debug, Clone)]
//pub struct EdgeNotFoundError;
//
//impl fmt::Display for EdgeNotFoundError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        write!(f, "Edge is found found in cache")
//    }
//}
//
//impl error::Error for EdgeNotFoundError {
//    fn description(&self) -> &str {
//        "Edge is found found in cache"
//    }
//
//    fn cause(&self) -> Option<&error::Error> {
//        None
//    }
//}
//
//
//#[derive(Debug, Clone)]
//pub struct BooleanExpressionError;
//
//impl fmt::Display for BooleanExpressionError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        write!(f, "Boolean expression is expected")
//    }
//}
//
//impl error::Error for BooleanExpressionError {
//    fn description(&self) -> &str {
//        "Boolean expression is expected"
//    }
//
//    fn cause(&self) -> Option<&error::Error> {
//        None
//    }
//}
//
//
//#[derive(Debug, Clone)]
//pub struct NumberExpressionError;
//
//impl fmt::Display for NumberExpressionError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        write!(f, "Number expression is expected")
//    }
//}
//
//impl error::Error for NumberExpressionError {
//    fn description(&self) -> &str {
//        "Number expression is expected"
//    }
//
//    fn cause(&self) -> Option<&error::Error> {
//        None
//    }
//}
//
//
//#[derive(Debug, Clone)]
//pub struct StringExpressionError;
//
//impl fmt::Display for StringExpressionError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        write!(f, "String expression is expected")
//    }
//}
//
//impl error::Error for StringExpressionError {
//    fn description(&self) -> &str {
//        "String expression is expected"
//    }
//
//    fn cause(&self) -> Option<&error::Error> {
//        None
//    }
//}
