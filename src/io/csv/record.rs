use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use serde;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

use generic::IdType;
use generic::MutGraphTrait;

#[derive(Debug, Serialize)]
pub struct NodeRecord<Id: IdType, N: Hash + Eq> {
    #[serde(rename = "nodeId:ID")]
    id: Id,
    #[serde(rename = ":LABEL")]
    label: Option<N>,
}

#[derive(Debug, Serialize)]
pub struct EdgeRecord<Id: IdType, E: Hash + Eq> {
    #[serde(rename = ":START_ID")]
    start: Id,
    #[serde(rename = ":END_ID")]
    target: Id,
    #[serde(rename = ":TYPE")]
    label: Option<E>,
}

impl<Id: IdType, N: Hash + Eq> NodeRecord<Id, N> {
    pub fn new(id: Id, label: Option<N>) -> Self {
        NodeRecord { id, label }
    }

    pub fn add_to_graph<E: Hash + Eq, G: MutGraphTrait<Id, N, E>>(self, g: &mut G) {
        g.add_node(self.id, self.label);
    }
}

impl<Id: IdType, E: Hash + Eq> EdgeRecord<Id, E> {
    pub fn new(start: Id, target: Id, label: Option<E>) -> Self {
        EdgeRecord {
            start,
            target,
            label,
        }
    }

    pub fn add_to_graph<N: Hash + Eq, G: MutGraphTrait<Id, N, E>>(self, g: &mut G) {
        g.add_edge(self.start, self.target, self.label);
    }
}

impl<'de, Id: IdType, N: Hash + Eq> Deserialize<'de> for NodeRecord<Id, N>
where
    Id: serde::Deserialize<'de>,
    N: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier)]
        enum Field {
            #[serde(rename = "nodeId:ID")]
            Id,
            #[serde(rename = ":LABEL")]
            Label,
        }

        struct NodeRecordVisitor<Id, N> {
            _id: PhantomData<Id>,
            _n: PhantomData<N>,
        };

        impl<'de, Id: IdType, N: Hash + Eq> Visitor<'de> for NodeRecordVisitor<Id, N>
        where
            Id: serde::Deserialize<'de>,
            N: serde::Deserialize<'de>,
        {
            type Value = NodeRecord<Id, N>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct NodeRecord")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<NodeRecord<Id, N>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let id = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let label = seq.next_element().unwrap_or(None);

                Ok(NodeRecord::new(id, label))
            }

            fn visit_map<V>(self, mut map: V) -> Result<NodeRecord<Id, N>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut id = None;
                let mut label = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Id => {
                            if id.is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }
                            id = Some(map.next_value()?);
                        }
                        Field::Label => {
                            if label.is_some() {
                                return Err(de::Error::duplicate_field("label"));
                            }
                            label = Some(map.next_value().unwrap_or(None));
                        }
                    }
                }
                let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
                let label = label.unwrap_or(None);
                Ok(NodeRecord::new(id, label))
            }
        }

        const FIELDS: &'static [&'static str] = &["id", "label"];
        deserializer.deserialize_struct(
            "NodeRecord",
            FIELDS,
            NodeRecordVisitor {
                _id: PhantomData,
                _n: PhantomData,
            },
        )
    }
}

impl<'de, Id: IdType, E: Hash + Eq> Deserialize<'de> for EdgeRecord<Id, E>
where
    Id: serde::Deserialize<'de>,
    E: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier)]
        enum Field {
            #[serde(rename = ":START_ID")]
            Start,
            #[serde(rename = ":END_ID")]
            Target,
            #[serde(rename = ":TYPE")]
            Label,
        }

        struct EdgeRecordVisitor<Id, E> {
            _id: PhantomData<Id>,
            _e: PhantomData<E>,
        };

        impl<'de, Id: IdType, E: Hash + Eq> Visitor<'de> for EdgeRecordVisitor<Id, E>
        where
            Id: serde::Deserialize<'de>,
            E: serde::Deserialize<'de>,
        {
            type Value = EdgeRecord<Id, E>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct EdgeRecord")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<EdgeRecord<Id, E>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let start = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let target = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let label = seq.next_element().unwrap_or(None);

                Ok(EdgeRecord::new(start, target, label))
            }

            fn visit_map<V>(self, mut map: V) -> Result<EdgeRecord<Id, E>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut start = None;
                let mut target = None;
                let mut label = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Start => {
                            if start.is_some() {
                                return Err(de::Error::duplicate_field("start"));
                            }
                            start = Some(map.next_value()?);
                        }
                        Field::Target => {
                            if target.is_some() {
                                return Err(de::Error::duplicate_field("target"));
                            }
                            target = Some(map.next_value()?);
                        }
                        Field::Label => {
                            if label.is_some() {
                                return Err(de::Error::duplicate_field("label"));
                            }
                            label = Some(map.next_value().unwrap_or(None));
                        }
                    }
                }
                let start = start.ok_or_else(|| de::Error::missing_field("start"))?;
                let target = target.ok_or_else(|| de::Error::missing_field("target"))?;
                let label = label.unwrap_or(None);
                Ok(EdgeRecord::new(start, target, label))
            }
        }

        const FIELDS: &'static [&'static str] = &["start", "target", "label"];
        deserializer.deserialize_struct(
            "EdgeRecord",
            FIELDS,
            EdgeRecordVisitor {
                _id: PhantomData,
                _e: PhantomData,
            },
        )
    }
}
