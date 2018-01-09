//use std::fmt::{Debug, Formatter, Error};

pub type IndexIter<'a> = Iter<'a, &'a usize>;

pub struct Iter<'a, T> {
    inner: Box<Iterator<Item=T> + 'a>
}

impl<'a, T> Iter<'a, T> {
    pub fn new(iter: Box<Iterator<Item=T> + 'a>) -> Self {
        Iter {
            inner: iter
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

//impl<'a, T: Debug> Debug for Iter<'a, T> {
//    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
//        let mut result = String::new();
//        while let Some(item) = self.next() {
//            result.push_str(&format!("{:?}, ", item));
//        };
//        write!(f, "Iter({})", result)
//    }
//}
