use std::borrow::Cow;
use std::cell::{RefCell, RefMut};
use std::fs;
use std::hash::Hash;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;

use parking_lot::Mutex;
use fxhash::FxBuildHasher;
use lru::LruCache;
use tarpc::{
    client::{self, NewClient},
    context,
};
use tarpc_bincode_transport as bincode_transport;
use tokio::runtime::current_thread::Runtime as CurrentRuntime;

use crate::generic::{DefaultId, Void};
use crate::generic::{EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType};
use crate::graph_impl::rpc_graph::server::{GraphRPC, GraphRPCClient};
use crate::graph_impl::GraphImpl;
use crate::graph_impl::UnStaticGraph;
use crate::map::SetMap;
use std::path::Path;
use std::time::{Duration, Instant};

type FxLruCache<K, V> = LruCache<K, V, FxBuildHasher>;

pub struct Messenge{
    server_addrs: Vec<SocketAddr>,
//    runtime: RefCell<CurrentRuntime>,
    clients: Vec<Option<GraphRPCClient>>,
    cache: Mutex<FxLruCache<DefaultId, Vec<DefaultId>>>,
    workers: usize,
    peers: usize,
    processor: usize,
//    cache_hits: RefCell<usize>,
}

impl Messenge{


}