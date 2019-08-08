//use std::borrow::Cow;
//use std::cell::RefCell;
//use std::hash::Hash;
//use std::sync::Arc;
//use std::net::SocketAddr;
//
//use lru::LruCache;
//use tarpc::{client, context};
//use fxhash::FxBuildHasher;
//
//use crate::generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType};
//use crate::graph_impl::GraphImpl;
//use crate::map::SetMap;
//use crate::graph_impl::UnStaticGraph;
//use crate::generic::{DefaultId, Void};
//use std::path::Path;
//
//type DefaultGraph = UnStaticGraph<Void>;
//type FxLruCache<K, V> = LruCache<K, V, FxBuildHasher>;
//
//pub struct GraphRPCClient{
//    graph:Arc<DefaultGraph>,
//    nodes_addr: Vec<SocketAddr>,
//    cache: RefCell<FxLruCache<DefaultId, Vec<DefaultId>>>,
//    workers: usize,
//    machines: usize,
//    processor: usize,
////    hits: RefCell<usize>,
////    requests: RefCell<usize>,
//}
//
//impl GraphRPCClient{
//    pub fn new<P:AsRef<Path>>(graph:Arc<DefaultGraph>,
//               cache_size: usize,
//               workers: usize,
//               machines: usize,
//               processor: usize, path_to_hosts:P)->Self{
//        unimplemented!()
//    }
//}
