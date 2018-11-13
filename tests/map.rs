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
#[macro_use]
extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::map::{SetMap, VecMap};

#[test]
fn test_set_map() {
    let mut label_map = SetMap::<&str>::new();

    assert_eq!(label_map.len(), 0);

    assert_eq!(label_map.add_item("zero"), 0);
    assert_eq!(label_map.add_item("first"), 1);
    assert_eq!(label_map.add_item("zero"), 0);
    assert_eq!(label_map.add_item("first"), 1);

    assert_eq!(label_map.len(), 2);
    assert_eq!(label_map.get_item(0), Some(&"zero"));
    assert_eq!(label_map.get_item(1), Some(&"first"));

    assert_eq!(label_map.get_item(2), None);

    assert!(label_map.contains(&"zero"));
    assert!(!label_map.contains(&"five"));

    assert_eq!(label_map.find_index(&"zero"), Some(0));
    assert_eq!(label_map.find_index(&"first"), Some(1));
    assert_eq!(label_map.find_index(&"five"), None);

    let items: Vec<_> = label_map.items().collect();
    assert_eq!(items, vec![&"zero", &"first"]);
}

#[test]
fn test_vec_map() {
    let mut label_map = VecMap::<&str>::new();

    assert_eq!(label_map.len(), 0);

    assert_eq!(label_map.add_item("zero"), 0);
    assert_eq!(label_map.add_item("first"), 1);
    assert_eq!(label_map.add_item("zero"), 0);
    assert_eq!(label_map.add_item("first"), 1);

    assert_eq!(label_map.len(), 2);
    assert_eq!(label_map.get_item(0), Some(&"zero"));
    assert_eq!(label_map.get_item(1), Some(&"first"));

    assert_eq!(label_map.get_item(2), None);

    assert!(label_map.contains(&"zero"));
    assert!(!label_map.contains(&"five"));

    assert_eq!(label_map.find_index(&"zero"), Some(0));
    assert_eq!(label_map.find_index(&"first"), Some(1));
    assert_eq!(label_map.find_index(&"five"), None);

    let items: Vec<_> = label_map.items().collect();
    assert_eq!(items, vec![&"zero", &"first"]);
}

#[test]
fn test_macro() {
    let setmap1 = setmap![1u32, 2, 3];
    assert_eq!(setmap1.clone().items_vec(), vec![1u32, 2, 3]);
    let vecmap1 = vecmap![1u32, 2, 3];
    assert_eq!(vecmap1.clone().items_vec(), vec![1u32, 2, 3]);

    let setmap2: SetMap<_> = (1u32..4).collect();
    assert_eq!(setmap2, setmap1);
    let vecmap2: VecMap<_> = (1u32..4).collect();
    assert_eq!(vecmap2, vecmap1);
}
