pub struct Iter<'a, T> {
    inner: Box<Iterator<Item = T> + 'a>,
}

impl<'a, T> Iter<'a, T> {
    pub fn new(iter: Box<Iterator<Item = T> + 'a>) -> Self {
        Iter { inner: iter }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    fn count(self) -> usize {
        self.inner.count()
    }
}
