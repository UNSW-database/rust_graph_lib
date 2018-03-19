use std::hash::Hash;

use generic::{MapTrait, MutMapTrait};
use graph_impl::map::{SetMap, VecMap};

impl<L: Hash + Eq> From<SetMap<L>> for VecMap<L> {
    fn from(setmap: SetMap<L>) -> Self {
        let data = setmap.items_vec();
        VecMap::with_data(data)
    }
}

impl<L: Hash + Eq> From<VecMap<L>> for SetMap<L> {
    fn from(vecmap: VecMap<L>) -> Self {
        let mut setmap = SetMap::with_capacity(vecmap.len());
        for item in vecmap.items_vec() {
            setmap.add_item(item);
        }
        setmap
    }
}
