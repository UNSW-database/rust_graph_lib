use std::fs::File;

use lib::serde::{de, ser};
use lib::bincode::{deserialize_from, serialize_into};
use lib::bincode::Result;

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
        let file = File::create(path)?;
        serialize_into(file, &obj)
    }
}

impl Deserialize for Deserializer {
    fn import<T>(path: &str) -> Result<T>
        where
            T: de::DeserializeOwned,
    {
        let file = File::open(path)?;
        deserialize_from(file)
    }
}