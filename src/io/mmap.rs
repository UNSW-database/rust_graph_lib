//! This implements some `Trait` for dumping and loading memory-mapped (mmap) file.
//! Memory-mapped file is a mechanism in Unix-like system to reduce memory cost.
//! More details can be found here: https://en.wikipedia.org/wiki/Memory-mapped_file.

extern crate memmap;

use std::fs::File;
use std::io::{BufWriter, Result, Write};
use std::marker::PhantomData;
use std::mem;
use std::ops;
use std::path::Path;
use std::slice;

pub struct TypedMemoryMap<T: Copy> {
    pub map: memmap::Mmap,
    pub len: usize,
    type_len: usize,
    // in bytes (needed because map extends to full block)
    phn: PhantomData<T>,
}

impl<T: Copy> TypedMemoryMap<T> {
    pub fn new<P: AsRef<Path>>(filename: P) -> Self {
        let file = File::open(filename).expect("error opening file");
        let size = file.metadata().expect("error reading metadata").len() as usize;
        let type_len = mem::size_of::<T>();
        TypedMemoryMap {
            map: unsafe { memmap::Mmap::map(&file).unwrap() },
            len: size / type_len,
            type_len,
            phn: PhantomData,
        }
    }

    pub fn with_mmap(map: memmap::Mmap) -> Self {
        let size = map.len();
        let type_len = mem::size_of::<T>();
        Self {
            map,
            len: size / type_len,
            type_len,
            phn: PhantomData,
        }
    }
}

impl<T: Copy> ops::Index<ops::RangeFull> for TypedMemoryMap<T> {
    type Output = [T];
    #[inline]
    fn index(&self, _index: ops::RangeFull) -> &[T] {
        unsafe { slice::from_raw_parts(self.map.as_ptr() as *const T, self.len) }
    }
}

impl<T: Copy> ops::Index<ops::RangeFrom<usize>> for TypedMemoryMap<T> {
    type Output = [T];
    #[inline]
    fn index(&self, _index: ops::RangeFrom<usize>) -> &[T] {
        let index = _index.start;
        unsafe {
            slice::from_raw_parts(
                self.map.as_ptr().offset((index * self.type_len) as isize) as *const T,
                self.len - index,
            )
        }
    }
}

impl<T: Copy> ops::Index<ops::RangeTo<usize>> for TypedMemoryMap<T> {
    type Output = [T];
    #[inline]
    fn index(&self, _index: ops::RangeTo<usize>) -> &[T] {
        unsafe { slice::from_raw_parts(self.map.as_ptr() as *const T, _index.end) }
    }
}

#[inline]
pub fn typed_as_byte_slice<T: Copy>(slice: &[T]) -> &[u8] {
    unsafe {
        slice::from_raw_parts(
            slice.as_ptr() as *const u8,
            slice.len() * mem::size_of::<T>(),
        )
    }
}

pub unsafe fn dump<W: Write, T: Copy>(data: &[T], write: W) -> Result<()> {
    let mut writer = BufWriter::new(write);
    let mut slice = typed_as_byte_slice(data);

    while !slice.is_empty() {
        let to_write = if slice.len() < 10000 {
            slice.len()
        } else {
            10000
        };
        writer.write_all(&slice[..to_write])?;
        slice = &slice[to_write..];
    }

    writer.flush()
}
