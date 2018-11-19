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
use generic::{IdType, NodeTrait};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticNode<Id: IdType, L: IdType = Id> {
    id: Id,
    label: Option<L>,
}

impl<Id: IdType, L: IdType> StaticNode<Id, L> {
    #[inline(always)]
    pub fn new(id: Id, label: Option<L>) -> Self {
        StaticNode { id, label }
    }

    #[inline(always)]
    pub fn new_static(id: Id, label: L) -> Self {
        StaticNode {
            id,
            label: if label == L::max_value() {
                None
            } else {
                Some(label)
            },
        }
    }
}

impl<Id: IdType, L: IdType> NodeTrait<Id, L> for StaticNode<Id, L> {
    #[inline(always)]
    fn get_id(&self) -> Id {
        self.id
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<L> {
        self.label
    }
}
