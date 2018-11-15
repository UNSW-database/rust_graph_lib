use json::{parse, stringify, JsonValue};
use std::fmt;

use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::ser::{Serialize, Serializer};

use generic::IdType;
use property::NaiveProperty;

struct SerdeJsonValue {
    pub json: JsonValue,
}

impl SerdeJsonValue {
    pub fn new(json: &JsonValue) -> Self {
        SerdeJsonValue { json: json.clone() }
    }

    pub fn unwrap(self) -> JsonValue {
        self.json
    }
}

impl Serialize for SerdeJsonValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&stringify(self.json.clone()))
    }
}

struct SerdeJsonValueVisitor;

impl<'de> Visitor<'de> for SerdeJsonValueVisitor {
    type Value = SerdeJsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON string")
    }

    fn visit_str<E>(self, valve: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match parse(valve) {
            Ok(json) => Ok(SerdeJsonValue { json }),
            Err(e) => Err(E::custom(format!("{:?}", e))),
        }
    }
}

impl<'de> Deserialize<'de> for SerdeJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<SerdeJsonValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SerdeJsonValueVisitor)
    }
}

#[derive(Serialize, Deserialize)]
struct SerdeNaiveProperty<Id: IdType> {
    node_property: Vec<(Id, SerdeJsonValue)>,
    edge_property: Vec<((Id, Id), SerdeJsonValue)>,
    is_directed: bool,
}

impl<Id: IdType> SerdeNaiveProperty<Id> {
    pub fn new(property: &NaiveProperty<Id>) -> Self {
        SerdeNaiveProperty {
            node_property: property
                .node_property
                .iter()
                .map(|(i, j)| (*i, SerdeJsonValue::new(j)))
                .collect(),
            edge_property: property
                .edge_property
                .iter()
                .map(|(i, j)| (*i, SerdeJsonValue::new(j)))
                .collect(),
            is_directed: property.is_directed,
        }
    }

    pub fn unwrap(self) -> NaiveProperty<Id> {
        NaiveProperty {
            node_property: self
                .node_property
                .into_iter()
                .map(|(i, j)| (i, j.unwrap()))
                .collect(),
            edge_property: self
                .edge_property
                .into_iter()
                .map(|(i, j)| (i, j.unwrap()))
                .collect(),
            is_directed: self.is_directed,
        }
    }
}

impl<Id: IdType> Serialize for NaiveProperty<Id>
where
    Id: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let property = SerdeNaiveProperty::new(&self);
        property.serialize(serializer)
    }
}

impl<'de, Id: IdType> Deserialize<'de> for NaiveProperty<Id>
where
    Id: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<NaiveProperty<Id>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let property = SerdeNaiveProperty::deserialize(deserializer)?;
        Ok(property.unwrap())
    }
}
