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

use rust_graph::cache::Cache;

#[test]
fn test_cache() {
    let mut cache = Cache::<u32>::new(10);
    assert_eq!(cache.capacity(), 10);

    cache.reserve(0);
    cache.insert(0, vec![1, 2, 3, 4, 5]);
    assert_eq!(cache.size(), 5);
    assert_eq!(cache.len(), 1);

    cache.free_all();
    cache.reserve(1);
    cache.insert(1, vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(cache.size(), 6);
    assert_eq!(cache.len(), 1);

    cache.reserve(2);
    cache.insert(2, vec![1, 2, 3, 4]);
    assert_eq!(cache.size(), 10);
    assert_eq!(cache.len(), 2);

    cache.reserve(3);
    cache.insert(3, vec![1]);
    assert_eq!(cache.size(), 11);
    assert_eq!(cache.len(), 3);

    assert!(!cache.contains_key(&0));
    assert!(cache.contains_key(&1));
    assert!(cache.contains_key(&2));
    assert!(cache.contains_key(&3));
    assert!(!cache.contains_key(&4));

    assert_eq!(cache.get(&0), None);
    assert_eq!(cache.get(&1), Some(&vec![1, 2, 3, 4, 5, 6]));
    assert_eq!(cache.get(&2), Some(&vec![1, 2, 3, 4]));
    assert_eq!(cache.get(&3), Some(&vec![1]));
    assert_eq!(cache.get(&4), None);
}
