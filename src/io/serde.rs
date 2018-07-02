use std::fs::File;

use serde::{de, ser};
use bincode::{deserialize_from, serialize_into,Infinite};
use bincode::Result;

pub struct Serializer;

pub struct Deserializer;

pub trait Serialize {
    fn export<T>(obj: &T, path: &str) -> Result<()>
        where
            T: ser::Serialize;
}

pub trait Deserialize {
    fn import<T>(path: &str) -> Result<T>
        where
            T: de::DeserializeOwned;
}

impl Serialize for Serializer {
    fn export<T>(obj: &T, path: &str) -> Result<()>
        where
            T: ser::Serialize,
    {
        let mut file = File::create(path)?;
        serialize_into(&mut file, &obj,Infinite)
    }
}

impl Deserialize for Deserializer {
    fn import<T>(path: &str) -> Result<T>
        where
            T: de::DeserializeOwned,
    {
        let mut file = File::open(path)?;
        deserialize_from(&mut file,Infinite)
    }
}