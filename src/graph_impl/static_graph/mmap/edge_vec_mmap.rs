use std::fs::metadata;

use generic::IdType;
use graph_impl::static_graph::EdgeVecTrait;
use io::mmap::TypedMemoryMap;

/// A mmap version of `EdgeVec`.
pub struct EdgeVecMmap<Id: IdType> {
    offsets: TypedMemoryMap<usize>,
    edges: TypedMemoryMap<Id>,
    labels: Option<TypedMemoryMap<Id>>,
}

impl<Id: IdType> EdgeVecMmap<Id> {
    pub fn new(prefix: &str) -> Self {
        let offsets_file = format!("{}.offsets", prefix);
        let edges_file = format!("{}.edges", prefix);
        let labels_file = format!("{}.labels", prefix);

        if metadata(&labels_file).is_ok() {
            EdgeVecMmap {
                offsets: TypedMemoryMap::new(&offsets_file),
                edges: TypedMemoryMap::new(&edges_file),
                labels: Some(TypedMemoryMap::new(&labels_file)),
            }
        } else {
            EdgeVecMmap {
                offsets: TypedMemoryMap::new(&offsets_file),
                edges: TypedMemoryMap::new(&edges_file),
                labels: None,
            }
        }
    }
}

impl<Id: IdType> EdgeVecTrait<Id> for EdgeVecMmap<Id> {
    #[inline(always)]
    fn get_offsets(&self) -> &[usize] {
        &self.offsets[..]
    }

    #[inline(always)]
    fn get_edges(&self) -> &[Id] {
        &self.edges[..]
    }

    #[inline(always)]
    fn get_labels(&self) -> &[Id] {
        match self.labels {
            Some(ref labels) => &labels[..],
            None => &[],
        }
    }
}
