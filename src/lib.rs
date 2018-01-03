extern crate ordermap;

mod generic;
pub mod prelude;
pub mod implementation;

pub use implementation::{DiGraphMap, UnGraphMap};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
