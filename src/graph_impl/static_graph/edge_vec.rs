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
use generic::IdType;
use io::mmap::dump;
use itertools::Itertools;
use std::fs::File;
use std::io::Result;
use std::ops::Add;

/// With the node indexed from 0 .. num_nodes - 1, we can maintain the edges in a compact way,
/// using `offset` and `edges`, in which `offset[node]` maintain the start index of the given
/// node's neighbors in `edges`. Thus, the node's neighbors is maintained in:
/// `edges[offsets[node]]` (included) to `edges[offsets[node+1]]` (excluded),
///
/// *Note*: The edges must be sorted according to the starting node, that is,
/// The sub-vector from `edges[offsets[node]]` (included) to `edges[offsets[node + 1]]` (excluded)
/// for any `node` should be sorted.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EdgeVec<Id: IdType, L: IdType = Id> {
    offsets: Vec<usize>,
    edges: Vec<Id>,
    labels: Option<Vec<L>>,
}

pub trait EdgeVecTrait<Id: IdType, L: IdType> {
    fn get_offsets(&self) -> &[usize];
    fn get_edges(&self) -> &[Id];
    fn get_labels(&self) -> &[L];

    #[inline]
    fn num_nodes(&self) -> usize {
        self.get_offsets().len() - 1
    }

    #[inline]
    fn num_edges(&self) -> usize {
        self.get_edges().len()
    }

    #[inline]
    fn neighbors(&self, node: Id) -> &[Id] {
        assert!(self.has_node(node));
        let start = self.get_offsets()[node.id()].id();
        let end = self.get_offsets()[node.id() + 1].id();

        &self.get_edges()[start..end]
    }

    #[inline]
    fn degree(&self, node: Id) -> usize {
        assert!(self.has_node(node));
        let start = self.get_offsets()[node.id()].id();
        let end = self.get_offsets()[node.id() + 1].id();

        end - start
    }

    #[inline]
    fn has_node(&self, node: Id) -> bool {
        node.id() < self.num_nodes()
    }

    #[inline]
    fn find_edge_index(&self, start: Id, target: Id) -> Option<usize> {
        if !(self.has_node(start) && self.has_node(target)) {
            None
        } else {
            let neighbors = self.neighbors(start);
            let found = neighbors.binary_search(&target);
            match found {
                Err(_) => None,
                Ok(idx) => Some(self.get_offsets()[start.id()].id() + idx),
            }
        }
    }

    #[inline]
    fn find_edge_label_id(&self, start: Id, target: Id) -> Option<&L> {
        let labels = self.get_labels();

        if labels.is_empty() {
            return None;
        }

        let idx_opt = self.find_edge_index(start, target);
        match idx_opt {
            None => None,
            Some(idx) => labels.get(idx),
        }
    }

    #[inline]
    fn has_edge(&self, start: Id, target: Id) -> bool {
        self.find_edge_index(start, target).is_some()
    }
}

impl<Id: IdType, L: IdType> EdgeVec<Id, L> {
    pub fn new(offsets: Vec<usize>, edges: Vec<Id>) -> Self {
        EdgeVec {
            offsets,
            edges,
            labels: None,
        }
    }

    pub fn with_labels(offsets: Vec<usize>, edges: Vec<Id>, labels: Vec<L>) -> Self {
        if edges.len() != labels.len() {
            panic!(
                "Unequal length: there are {} edges, but {} labels",
                edges.len(),
                labels.len()
            );
        }
        EdgeVec {
            offsets,
            edges,
            labels: Some(labels),
        }
    }

    pub fn from_raw(offsets: Vec<usize>, edges: Vec<Id>, labels: Option<Vec<L>>) -> Self {
        match labels {
            Some(labels) => EdgeVec::with_labels(offsets, edges, labels),
            None => EdgeVec::new(offsets, edges),
        }
    }

    pub fn remove_labels(&mut self) {
        self.labels = None;
    }

    pub fn clear(&mut self) {
        self.offsets.clear();
        self.edges.clear();
        if let Some(ref mut labels) = self.labels {
            labels.clear();
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
        self.edges.shrink_to_fit();
        if let Some(ref mut labels) = self.labels {
            labels.shrink_to_fit();
        }
    }

    /// Dump self to bytearray in order to be deserialised as `EdgeVecMmap`.
    pub fn dump_mmap(&self, prefix: &str) -> Result<()> {
        let offsets_file = format!("{}.offsets", prefix);
        let edges_file = format!("{}.edges", prefix);
        let labels_file = format!("{}.labels", prefix);

        unsafe {
            dump(self.get_offsets(), File::create(offsets_file)?)?;
            dump(self.get_edges(), File::create(edges_file)?)?;

            if !self.get_labels().is_empty() {
                dump(self.get_labels(), File::create(labels_file)?)
            } else {
                Ok(())
            }
        }
    }
}

impl<Id: IdType, L: IdType> EdgeVecTrait<Id, L> for EdgeVec<Id, L> {
    #[inline]
    fn get_offsets(&self) -> &[usize] {
        &self.offsets
    }

    #[inline]
    fn get_edges(&self) -> &[Id] {
        &self.edges
    }

    #[inline]
    fn get_labels(&self) -> &[L] {
        match self.labels {
            Some(ref labels) => &labels[..],
            None => &[],
        }
    }
}

impl<Id: IdType, L: IdType> Default for EdgeVec<Id, L> {
    fn default() -> Self {
        EdgeVec::new(Vec::new(), Vec::new())
    }
}

/// Add two `EdgeVec`s following the rules:
/// * The `edges` will the merged vector, duplication will be removed.
/// * The `labels` if some, will be the merged vector. We assume that the label is the same
///   for two same edges (same `src` and `dst`) is the same, hence the label will be randomly
///   picked up in either `EdgeVec`. If they contain different labels, it will end with indefinite
///   results.
/// * The `offsets` will be of the length of the longer one, and reshifted according to the
///   merged `edges`.
///
/// # Panic
///
/// One `EdgeVec` has `Some(labels)`, but the other has `None`.
impl<Id: IdType, L: IdType> Add for EdgeVec<Id, L> {
    type Output = EdgeVec<Id, L>;

    fn add(self, other: EdgeVec<Id, L>) -> Self::Output {
        assert_eq!(self.labels.is_some(), other.labels.is_some());
        println!("self: offset len: {}, edges len: {}", self.offsets.len(), self.edges.len());
        println!("self: offset len: {}, edges len: {}", other.offsets.len(), other.edges.len());

        let (smaller, larger) = if self.offsets.len() <= other.offsets.len() {
            (self, other)
        } else {
            (other, self)
        };

        let len = smaller.edges.len() + larger.edges.len();
        let s_num_nodes = smaller.offsets.len() - 1;
        let num_nodes = larger.offsets.len() - 1;

        let mut edges = Vec::with_capacity(len);
        let mut labels = Vec::with_capacity(len);
        let mut offsets = Vec::with_capacity(num_nodes + 1);
        offsets.push(0);

        for node in 0..s_num_nodes {
            let (s1, e1) = (smaller.offsets[node], smaller.offsets[node + 1]);
            let (s2, e2) = (larger.offsets[node], larger.offsets[node + 1]);
            let mut num_nbrs = 0;

            if smaller.labels.is_none() {
                let merged_nbrs = smaller
                    .edges
                    .iter()
                    .skip(s1)
                    .take(e1 - s1)
                    .merge(larger.edges.iter().skip(s2).take(e2 - s2))
                    .dedup();

                for &nbr in merged_nbrs {
                    edges.push(nbr);
                    num_nbrs += 1;
                }
            } else {
                let merged_nbrs = smaller
                    .edges
                    .iter()
                    .skip(s1)
                    .take(e1 - s1)
                    .zip(
                        smaller
                            .labels
                            .as_ref()
                            .unwrap()
                            .iter()
                            .skip(s1)
                            .take(e1 - s1),
                    ).merge(
                        larger.edges.iter().skip(s2).take(e2 - s2).zip(
                            larger
                                .labels
                                .as_ref()
                                .unwrap()
                                .iter()
                                .skip(s2)
                                .take(e2 - s2),
                        )
                    ).unique_by(|x| x.0);

                for (&nbr, &lab) in merged_nbrs {
                    edges.push(nbr);
                    labels.push(lab);
                    num_nbrs += 1;
                }
            }

            let offset = offsets.last().unwrap() + num_nbrs;
            offsets.push(offset);
        }

        if s_num_nodes < num_nodes {
            let last_off = larger.offsets[s_num_nodes];
            edges.extend(larger.edges.iter().skip(last_off));
            if larger.labels.is_some() {
                labels.extend(larger.labels.as_ref().unwrap().iter().skip(last_off));
            }

            let extra_off = *offsets.last().unwrap() - larger.offsets[s_num_nodes];
            offsets.extend(
                larger
                    .offsets
                    .iter()
                    .skip(s_num_nodes + 1)
                    .map(|x| x + extra_off),
            );
        }

        EdgeVec {
            offsets,
            edges,
            labels: if smaller.labels.is_none() {
                None
            } else {
                Some(labels)
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_merge() {
        let edges1 = EdgeVec::<u32>::new(vec![0, 2, 4, 6, 8], vec![1_u32, 3, 0, 2, 1, 3, 0, 2]);

        let edges2 = EdgeVec::<u32>::new(vec![0, 1, 2, 3, 4], vec![2_u32, 3, 0, 1]);

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 3, 6, 9, 12]);

        assert_eq!(edges.edges, vec![1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2]);

        assert!(edges.labels.is_none());
    }

    #[test]
    fn test_merge_with_label() {
        let edges1 = EdgeVec::<u32>::with_labels(
            vec![0, 2, 4, 6, 8],
            vec![1_u32, 3, 0, 2, 1, 3, 0, 2],
            vec![1, 3, 1, 3, 3, 5, 3, 5],
        );

        let edges2 = EdgeVec::<u32>::with_labels(
            vec![0, 1, 2, 3, 4],
            vec![2_u32, 3, 0, 1],
            vec![2, 4, 2, 4],
        );

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 3, 6, 9, 12]);

        assert_eq!(edges.edges, vec![1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2]);

        assert_eq!(edges.labels, Some(vec![1, 2, 3, 1, 3, 4, 2, 3, 5, 3, 4, 5]));
    }

    #[test]
    fn test_merge_with_comm_edges() {
        let edges1 = EdgeVec::<u32>::with_labels(
            vec![0, 2, 4, 6, 8],
            vec![1_u32, 3, 0, 2, 1, 3, 0, 2],
            vec![1, 3, 1, 3, 3, 5, 3, 5],
        );

        let edges2 = EdgeVec::<u32>::with_labels(
            vec![0, 2, 5, 7, 8],
            vec![1_u32, 2, 0, 2, 3, 0, 1, 1],
            vec![1, 2, 1, 3, 4, 2, 3, 4],
        );

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 3, 6, 9, 12]);

        assert_eq!(edges.edges, vec![1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2]);

        assert_eq!(edges.labels, Some(vec![1, 2, 3, 1, 3, 4, 2, 3, 5, 3, 4, 5]));
    }

    #[test]
    fn test_merge_with_more_nodes() {
        let edges1 = EdgeVec::<u32>::with_labels(
            vec![0, 2, 4, 6],
            vec![1_u32, 2, 0, 2, 0, 1],
            vec![1, 2, 1, 3, 2, 3],
        );

        let edges2 = EdgeVec::<u32>::with_labels(
            vec![0, 1, 2, 3, 6, 7],
            vec![3_u32, 3, 3, 0, 1, 2, 2],
            vec![3, 4, 5, 3, 4, 5, 6],
        );

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 3, 6, 9, 12, 13]);

        assert_eq!(edges.edges, vec![1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2, 2]);

        assert_eq!(
            edges.labels,
            Some(vec![1, 2, 3, 1, 3, 4, 2, 3, 5, 3, 4, 5, 6])
        );
    }

    #[test]
    fn test_merge_with_empty_edges() {
        let edges1 = EdgeVec::<u32>::new(
            vec![0, 0],
            vec![],
        );

        let edges2 = EdgeVec::<u32>::new(
            vec![0, 1, 2, 3, 6, 7],
            vec![3_u32, 3, 3, 0, 1, 2, 2],
        );

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 1, 2, 3, 6, 7]);

        assert_eq!(edges.edges, vec![3_u32, 3, 3, 0, 1, 2, 2]);
    }

}
