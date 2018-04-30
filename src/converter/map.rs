use std::hash::Hash;

use generic::MapTrait;
use map::{SetMap, VecMap};

impl<L: Hash + Eq> From<SetMap<L>> for VecMap<L> {
    fn from(set_map: SetMap<L>) -> Self {
        let data = set_map.items_vec();
        VecMap::with_data(data)
    }
}

impl<L: Hash + Eq> From<VecMap<L>> for SetMap<L> {
    fn from(vec_map: VecMap<L>) -> Self {
        let data = vec_map.items_vec();
        SetMap::from_vec(data)
    }
}
