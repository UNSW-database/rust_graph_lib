use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use serde::{de, ser};

use bincode::Result;
use bincode::{deserialize_from, serialize_into, Infinite};

pub struct Serializer;
pub struct Deserializer;

pub trait Serialize {
    fn export<T, P>(obj: &T, path: P) -> Result<()>
    where
        T: ser::Serialize,
        P: AsRef<Path>;
}

pub trait Deserialize {
    fn import<T, P>(path: P) -> Result<T>
    where
        T: de::DeserializeOwned,
        P: AsRef<Path>;
}

impl Serialize for Serializer {
    fn export<T, P>(obj: &T, path: P) -> Result<()>
    where
        T: ser::Serialize,
        P: AsRef<Path>,
    {
        let mut writer = BufWriter::new(File::create(path)?);

        serialize_into(&mut writer, &obj, Infinite)
    }
}

impl Deserialize for Deserializer {
    fn import<T, P>(path: P) -> Result<T>
    where
        T: de::DeserializeOwned,
        P: AsRef<Path>,
    {
        let mut reader = BufReader::new(File::open(path)?);

        deserialize_from(&mut reader, Infinite)
    }
}
