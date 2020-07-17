/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use serde::{de, ser};

pub use bincode::Result;
use bincode::{deserialize_from, serialize_into};

pub struct Serializer;
pub struct Deserializer;

pub trait Serialize: ser::Serialize {
    #[inline(always)]
    fn export<P>(&self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        Serializer::export(&self, path)
    }
}

pub trait Deserialize: de::DeserializeOwned {
    #[inline(always)]
    fn import<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        Deserializer::import(path)
    }
}

impl Serializer {
    #[inline(always)]
    pub fn export<T, P>(obj: &T, path: P) -> Result<()>
    where
        T: ser::Serialize,
        P: AsRef<Path>,
    {
        let mut writer = BufWriter::new(File::create(path)?);

        serialize_into(&mut writer, &obj)
    }
}

impl Deserializer {
    #[inline(always)]
    pub fn import<T, P>(path: P) -> Result<T>
    where
        T: de::DeserializeOwned,
        P: AsRef<Path>,
    {
        let mut reader = BufReader::new(File::open(path)?);

        deserialize_from(&mut reader)
    }
}
