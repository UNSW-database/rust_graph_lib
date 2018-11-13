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
use std::fmt::Debug;
use std::hash::Hash;

/// The default data type for graph indices is `u32`.
#[cfg(not(feature = "usize_id"))]
pub type DefaultId = u32;

/// The default data type for graph indices can be set to `usize` by setting `feature="usize_id"`.
#[cfg(feature = "usize_id")]
pub type DefaultId = usize;

pub type DefaultTy = Directed;

pub type Void = ();

pub trait GraphType: Debug + Eq + Clone {
    fn is_directed() -> bool;
}

/// Marker for directed graph
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub enum Directed {}

/// Marker for undirected graph
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub enum Undirected {}

impl GraphType for Directed {
    #[inline(always)]
    fn is_directed() -> bool {
        true
    }
}

impl GraphType for Undirected {
    #[inline(always)]
    fn is_directed() -> bool {
        false
    }
}

pub unsafe trait IdType: Copy + Clone + Default + Hash + Debug + Ord {
    fn new(x: usize) -> Self;
    fn id(&self) -> usize;
    fn max_value() -> Self;
    fn max_usize() -> usize;
    fn increment(&self) -> Self;
}

unsafe impl IdType for () {
    #[inline(always)]
    fn new(_: usize) -> Self {
        ()
    }
    #[inline(always)]
    fn id(&self) -> usize {
        0
    }
    #[inline(always)]
    fn max_value() -> Self {
        ()
    }
    #[inline(always)]
    fn max_usize() -> usize {
        0
    }
    #[inline(always)]
    fn increment(&self) -> Self {
        ()
    }
}

unsafe impl IdType for u8 {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x as u8
    }
    #[inline(always)]
    fn id(&self) -> usize {
        *self as usize
    }
    #[inline(always)]
    fn max_value() -> Self {
        ::std::u8::MAX
    }
    #[inline(always)]
    fn max_usize() -> usize {
        ::std::u8::MAX as usize
    }
    #[inline(always)]
    fn increment(&self) -> Self {
        *self + 1
    }
}

unsafe impl IdType for u16 {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x as u16
    }
    #[inline(always)]
    fn id(&self) -> usize {
        *self as usize
    }
    #[inline(always)]
    fn max_value() -> Self {
        ::std::u16::MAX
    }
    #[inline(always)]
    fn max_usize() -> usize {
        ::std::u16::MAX as usize
    }
    #[inline(always)]
    fn increment(&self) -> Self {
        *self + 1
    }
}

unsafe impl IdType for u32 {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x as u32
    }
    #[inline(always)]
    fn id(&self) -> usize {
        *self as usize
    }
    #[inline(always)]
    fn max_value() -> Self {
        ::std::u32::MAX
    }
    #[inline(always)]
    fn max_usize() -> usize {
        ::std::u32::MAX as usize
    }
    #[inline(always)]
    fn increment(&self) -> Self {
        *self + 1
    }
}

unsafe impl IdType for usize {
    #[inline(always)]
    fn new(x: usize) -> Self {
        x
    }
    #[inline(always)]
    fn id(&self) -> usize {
        *self
    }
    #[inline(always)]
    fn max_value() -> Self {
        ::std::usize::MAX
    }
    #[inline(always)]
    fn max_usize() -> usize {
        ::std::u64::MAX as usize
    }
    #[inline(always)]
    fn increment(&self) -> Self {
        *self + 1
    }
}
