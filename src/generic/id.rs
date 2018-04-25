pub unsafe trait IdType {
    fn new(x: usize) -> Self;
    fn id(&self) -> usize;
    fn max_value() -> Self;
    fn max_usize() -> usize;
}

pub type DefaultId = u32;

unsafe impl IdType for u8 {
    fn new(x: usize) -> Self {
        x as u8
    }

    fn id(&self) -> usize {
        *self as usize
    }

    fn max_value() -> Self {
        ::std::u8::MAX
    }

    fn max_usize() -> usize {
        Self::max_value() as usize
    }
}

unsafe impl IdType for u16 {
    fn new(x: usize) -> Self {
        x as u16
    }

    fn id(&self) -> usize {
        *self as usize
    }

    fn max_value() -> Self {
        ::std::u16::MAX
    }

    fn max_usize() -> usize {
        Self::max_value() as usize
    }
}

unsafe impl IdType for u32 {
    fn new(x: usize) -> Self {
        x as u32
    }

    fn id(&self) -> usize {
        *self as usize
    }

    fn max_value() -> Self {
        ::std::u32::MAX
    }

    fn max_usize() -> usize {
        Self::max_value() as usize
    }
}

unsafe impl IdType for usize {
    fn new(x: usize) -> Self {
        x
    }

    fn id(&self) -> usize {
        *self
    }

    fn max_value() -> Self {
        ::std::usize::MAX
    }

    fn max_usize() -> usize {
        Self::max_value() as usize
    }
}
