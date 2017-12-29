//use std::iter::Iterator;

pub struct IndexIter<'a> {
    inner: Box<Iterator<Item=usize> + 'a>
}


impl<'a> IndexIter<'a> {
    pub fn new(iter: Box<Iterator<Item=usize> + 'a>) -> Self {
        IndexIter {
            inner: iter
        }
    }
}

impl<'a> Iterator for IndexIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}