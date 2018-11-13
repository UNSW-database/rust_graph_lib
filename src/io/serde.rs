/*
 * Copyright (c) 2018 UNSW Sydney, Data and Knowledge Group.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    fn export<P>(&self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        Serializer::export(&self, path)
    }
}

pub trait Deserialize: de::DeserializeOwned {
    fn import<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        Deserializer::import(path)
    }
}

impl Serializer {
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
    pub fn import<T, P>(path: P) -> Result<T>
    where
        T: de::DeserializeOwned,
        P: AsRef<Path>,
    {
        let mut reader = BufReader::new(File::open(path)?);

        deserialize_from(&mut reader)
    }
}
