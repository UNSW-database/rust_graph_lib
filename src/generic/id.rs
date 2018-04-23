pub unsafe trait IdType {
    fn new(x: usize) -> Self;
    fn id(&self) -> usize;
    fn max() -> Self;
}

pub type DefaultId = u32;

unsafe impl IdType for u32 {
    fn new(x: usize) -> Self {
        x as u32
    }

    fn id(&self) -> usize {
        *self as usize
    }

    fn max() -> Self {
        ::std::u32::MAX
    }
}

unsafe impl IdType for u64 {
    fn new(x: usize) -> Self {
        x as u64
    }

    fn id(&self) -> usize {
        *self as usize
    }

    fn max() -> Self {
        ::std::u64::MAX
    }
}

unsafe impl IdType for usize {
    fn new(x: usize) -> Self {
        x
    }

    fn id(&self) -> usize {
        *self
    }

    fn max() -> Self {
        ::std::usize::MAX
    }
}
