/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

use std::iter::empty;

pub struct Iter<'a, T> {
    inner: Box<dyn Iterator<Item = T> + 'a>,
}

impl<'a, T> Iter<'a, T> {
    pub fn new(iter: Box<dyn Iterator<Item = T> + 'a>) -> Self {
        Iter { inner: iter }
    }
}

impl<'a, T: 'a> Iter<'a, T> {
    pub fn empty() -> Self {
        Iter {
            inner: Box::new(empty()),
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline(always)]
    fn count(self) -> usize {
        self.inner.count()
    }
}
