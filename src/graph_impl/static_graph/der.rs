/// Compatibility fix
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

use generic::GraphType;
use generic::IdType;

use graph_impl::EdgeVec;
use graph_impl::TypedStaticGraph;

use map::SetMap;

impl<'de, Id, NL, EL, Ty> Deserialize<'de> for TypedStaticGraph<Id, NL, EL, Ty>
where
    Id: IdType + Deserialize<'de>,
    NL: Hash + Eq + Deserialize<'de>,
    EL: Hash + Eq + Deserialize<'de>,
    Ty: GraphType,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            NumNodes,
            NumEdges,
            EdgeVec,
            InEdgeVec,
            Labels,
            GraphType,
            NodeLabelMap,
            EdgeLabelMap,
        }

        struct GraphVisitor<'de, Id, NL, EL, Ty>
        where
            Id: IdType + Deserialize<'de>,
            NL: Hash + Eq + Deserialize<'de>,
            EL: Hash + Eq + Deserialize<'de>,
            Ty: GraphType,
        {
            marker: PhantomData<TypedStaticGraph<Id, NL, EL, Ty>>,
            lifetime: PhantomData<&'de ()>,
        }

        impl<'de, Id, NL, EL, Ty> Visitor<'de> for GraphVisitor<'de, Id, NL, EL, Ty>
        where
            Id: IdType + Deserialize<'de>,
            NL: Hash + Eq + Deserialize<'de>,
            EL: Hash + Eq + Deserialize<'de>,
            Ty: GraphType,
        {
            type Value = TypedStaticGraph<Id, NL, EL, Ty>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct TypedStaticGraph")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<TypedStaticGraph<Id, NL, EL, Ty>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let num_nodes = seq.next_element::<usize>()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let num_edges = seq.next_element::<usize>()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let edge_vec = seq.next_element::<EdgeVec<Id>>()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let in_edge_vec = seq.next_element::<Option<EdgeVec<Id>>>()?
                    .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                let labels = seq.next_element::<Option<Vec<Id>>>()?
                    .ok_or_else(|| de::Error::invalid_length(4, &self))?;
                let _graph_type = seq.next_element::<PhantomData<Ty>>()?
                    .ok_or_else(|| de::Error::invalid_length(5, &self))?;
                let node_label_map = seq.next_element::<SetMap<NL>>()?
                    .unwrap_or_else(|| SetMap::new());
                let edge_label_map = seq.next_element::<SetMap<EL>>()?
                    .unwrap_or_else(|| SetMap::new());

                Ok(TypedStaticGraph::from_raw(
                    num_nodes,
                    num_edges,
                    edge_vec,
                    in_edge_vec,
                    labels,
                    node_label_map,
                    edge_label_map,
                ))
            }

            fn visit_map<V>(self, mut map: V) -> Result<TypedStaticGraph<Id, NL, EL, Ty>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut num_nodes = None;
                let mut num_edges = None;
                let mut edge_vec = None;
                let mut in_edge_vec = None;
                let mut labels = None;
                let mut _graph_type = None;
                let mut node_label_map = None;
                let mut edge_label_map = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::NumNodes => {
                            if num_nodes.is_some() {
                                return Err(de::Error::duplicate_field("num_nodes"));
                            }
                            num_nodes = Some(map.next_value::<usize>()?);
                        }
                        Field::NumEdges => {
                            if num_edges.is_some() {
                                return Err(de::Error::duplicate_field("num_edges"));
                            }
                            num_edges = Some(map.next_value::<usize>()?);
                        }
                        Field::EdgeVec => {
                            if edge_vec.is_some() {
                                return Err(de::Error::duplicate_field("edge_vec"));
                            }
                            edge_vec = Some(map.next_value::<EdgeVec<Id>>()?);
                        }
                        Field::InEdgeVec => {
                            if in_edge_vec.is_some() {
                                return Err(de::Error::duplicate_field("in_edge_vec"));
                            }
                            in_edge_vec = Some(map.next_value::<Option<EdgeVec<Id>>>()?);
                        }
                        Field::Labels => {
                            if labels.is_some() {
                                return Err(de::Error::duplicate_field("labels"));
                            }
                            labels = Some(map.next_value::<Option<Vec<Id>>>()?);
                        }
                        Field::GraphType => {
                            if _graph_type.is_some() {
                                return Err(de::Error::duplicate_field("graph_type"));
                            }
                            _graph_type = Some(map.next_value::<PhantomData<Ty>>()?);
                        }
                        Field::NodeLabelMap => {
                            if node_label_map.is_some() {
                                return Err(de::Error::duplicate_field("node_label_map"));
                            }
                            node_label_map = Some(map.next_value::<SetMap<NL>>()?);
                        }
                        Field::EdgeLabelMap => {
                            if edge_label_map.is_some() {
                                return Err(de::Error::duplicate_field("edge_label_map"));
                            }
                            edge_label_map = Some(map.next_value::<SetMap<EL>>()?);
                        }
                    }
                }

                let num_nodes = num_nodes.ok_or_else(|| de::Error::missing_field("num_nodes"))?;
                let num_edges = num_edges.ok_or_else(|| de::Error::missing_field("num_edges"))?;
                let edge_vec = edge_vec.ok_or_else(|| de::Error::missing_field("edge_vec"))?;
                let in_edge_vec =
                    in_edge_vec.ok_or_else(|| de::Error::missing_field("in_edge_vec"))?;
                let labels = labels.ok_or_else(|| de::Error::missing_field("labels"))?;
                let _graph_type =
                    _graph_type.ok_or_else(|| de::Error::missing_field("graph_type"))?;
                let node_label_map = node_label_map.unwrap_or_else(|| SetMap::new());
                let edge_label_map = edge_label_map.unwrap_or_else(|| SetMap::new());

                Ok(TypedStaticGraph::from_raw(
                    num_nodes,
                    num_edges,
                    edge_vec,
                    in_edge_vec,
                    labels,
                    node_label_map,
                    edge_label_map,
                ))
            }
        }

        const FIELDS: &'static [&'static str] = &[
            "num_nodes",
            "num_edges",
            "edge_vec",
            "in_edge_vec",
            "labels",
            "graph_type",
            "node_label_map",
            "edge_label_map",
        ];

        deserializer.deserialize_struct(
            "TypedStaticGraph",
            FIELDS,
            GraphVisitor {
                marker: PhantomData::<TypedStaticGraph<Id, NL, EL, Ty>>,
                lifetime: PhantomData,
            },
        )
    }
}
