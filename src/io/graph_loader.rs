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

use crate::generic::IdType;
use crate::io::ReadGraph;
use serde::{Deserialize, Serialize};
use std::hash::Hash;

pub trait GraphLoader<'a, Id: IdType, NL: Hash + Eq, EL: Hash + Eq>
where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> NL: Deserialize<'de> + Serialize,
    for<'de> EL: Deserialize<'de> + Serialize,
{
    fn load(
        &self,
        reader: &'a (dyn ReadGraph<Id, NL, EL> + Sync),
        thread_cnt: usize,
        batch_size: usize,
    );
}