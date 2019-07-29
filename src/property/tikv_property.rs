use std::collections::BTreeMap;
use std::mem::swap;

use tikv_client::raw::{Client, Connect};
use tikv_client::Config;
use tikv_client::Value;

use crate::serde_json::Value as JsonValue;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{from_slice, to_value};

use crate::generic::{IdType, Iter};
use crate::property::{PropertyError, PropertyGraph};

pub struct TikvProperty {
    tikv_config: Config,
    is_directed: bool,
}

impl TikvProperty {
    /// New tikv-client with destroying all kv-pairs first if any
    pub fn new(tikv_config: Config, is_directed: bool) -> Result<Self, PropertyError> {
        futures::executor::block_on(async {
            let connection = Client::connect(tikv_config.clone());
            let client = connection.await.expect("Connect to pd-server failed!");
            client
                .delete_range("".to_owned()..)
                .await
                .expect("Delete all kv-pairs failed!");
        });

        Ok(TikvProperty {
            tikv_config,
            is_directed,
        })
    }

    pub fn open(tikv_config: Config, is_directed: bool) -> Result<Self, PropertyError> {
        Ok(TikvProperty {
            tikv_config,
            is_directed,
        })
    }

    pub fn with_data<Id: IdType + Serialize + DeserializeOwned, N, E>(
        tikv_config: Config,
        node_property: N,
        edge_property: E,
        is_directed: bool,
    ) -> Result<Self, PropertyError>
    where
        N: Iterator<Item = (Id, JsonValue)>,
        E: Iterator<Item = ((Id, Id), JsonValue)>,
    {
        let mut prop = Self::open(tikv_config, is_directed)?;
        prop.extend_node_property(node_property)?;
        prop.extend_edge_property(edge_property)?;

        Ok(prop)
    }

    #[inline(always)]
    fn swap_edge<Id: IdType>(&self, a: &mut Id, b: &mut Id) {
        if !self.is_directed && a > b {
            swap(a, b)
        }
    }

    fn get_property(
        &self,
        key: Vec<u8>,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        futures::executor::block_on(async {
            let connection = Client::connect(self.tikv_config.clone());
            let client = connection.await?;
            let _value = client.get(key).await?;
            match _value {
                Some(value_bytes) => {
                    let value_parsed: JsonValue = from_slice((&value_bytes).into())?;
                    let mut result = BTreeMap::<String, JsonValue>::new();
                    for name in names {
                        if value_parsed.get(&name).is_some() {
                            result.insert(name.clone(), value_parsed[&name].clone());
                        }
                    }
                    Ok(Some(to_value(result)?))
                }
                None => Ok(None),
            }
        })
    }

    fn get_property_all(&self, key: Vec<u8>) -> Result<Option<JsonValue>, PropertyError> {
        futures::executor::block_on(async {
            let connection = Client::connect(self.tikv_config.clone());
            let client = connection.await?;
            let _value = client.get(key).await?;
            match _value {
                Some(value_bytes) => {
                    let value_parsed: JsonValue = from_slice((&value_bytes).into())?;
                    Ok(Some(value_parsed))
                }
                None => Ok(None),
            }
        })
    }
}

impl<Id: IdType + Serialize + DeserializeOwned> PropertyGraph<Id> for TikvProperty {
    #[inline]
    fn get_node_property(
        &self,
        id: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let id_bytes = bincode::serialize(&id)?;
        self.get_property(id_bytes, names)
    }

    #[inline]
    fn get_edge_property(
        &self,
        mut src: Id,
        mut dst: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        let id_bytes = bincode::serialize(&(src, dst))?;
        self.get_property(id_bytes, names)
    }

    fn get_node_property_all(&self, id: Id) -> Result<Option<JsonValue>, PropertyError> {
        let id_bytes = bincode::serialize(&id)?;
        self.get_property_all(id_bytes)
    }

    fn get_edge_property_all(
        &self,
        mut src: Id,
        mut dst: Id,
    ) -> Result<Option<JsonValue>, PropertyError> {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        let id_bytes = bincode::serialize(&(src, dst))?;
        self.get_property_all(id_bytes)
    }

    fn insert_node_property(
        &mut self,
        id: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        unimplemented!()
    }

    fn insert_edge_property(
        &mut self,
        src: Id,
        dst: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        unimplemented!()
    }

    fn extend_node_property<I: IntoIterator<Item = (Id, JsonValue)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        unimplemented!()
    }

    fn extend_edge_property<I: IntoIterator<Item = ((Id, Id), JsonValue)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        unimplemented!()
    }

    fn insert_node_raw(
        &mut self,
        id: Id,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        unimplemented!()
    }

    fn insert_edge_raw(
        &mut self,
        src: Id,
        dst: Id,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        unimplemented!()
    }

    fn extend_node_raw<I: IntoIterator<Item = (Id, Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        unimplemented!()
    }

    fn extend_edge_raw<I: IntoIterator<Item = ((Id, Id), Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        unimplemented!()
    }

    fn scan_node_property_all(&self) -> Iter<Result<(Id, JsonValue), PropertyError>> {
        unimplemented!()
    }

    fn scan_edge_property_all(&self) -> Iter<Result<((Id, Id), JsonValue), PropertyError>> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    extern crate tikv_client;

    use tikv_client::raw::Client;
    use tikv_client::*;

    #[test]
    fn test_tikv_put_get() {
        futures::executor::block_on(async {
            let connect = Client::connect(Config::new(vec!["192.168.2.2"]));
            let client = connect.await.expect("Connect to pd-server failed");
            const KEY: &str = "Tikv";
            const VALUE: &str = "Rust";
            client
                .put(KEY.to_owned(), VALUE.to_owned())
                .await
                .expect("Put kv-pair failed");
            println!("Put key {:?}, value {:?}.", KEY, VALUE);

            let value: Option<Value> = client.get(KEY.to_owned()).await.expect("get key failed");
            assert_eq!(value, Some(Value::from(VALUE.to_owned())));
            println!("Get key `{}` returned value {:?}.", KEY, value);
        });
    }
}
