use generic::IdType;
use itertools::Itertools;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone)]
pub struct BlockInfo<Id: IdType> {
    pub block: Vec<Id>,
    pub start_offset: usize,
    pub end_offset: usize,
}

impl<Id: IdType> BlockInfo<Id> {
    pub fn empty() -> BlockInfo<Id> {
        BlockInfo {
            block: vec![],
            start_offset: 0,
            end_offset: 0,
        }
    }
}

#[derive(Clone)]
pub struct HashTable<Id: IdType> {
    blocks: Vec<Vec<Id>>,
    extra_blocks: Vec<Vec<Id>>,
    block_ids_and_chunk_offsets: Vec<Vec<usize>>,
    pub num_chunks: Vec<usize>,
    initial_num_blocks: usize,
    num_tuples_per_chunk: usize,
    num_chunks_per_block: usize,
    initial_num_chunks_per_vertex: usize,
    block_sz: usize,
    chunk_sz: usize,
    next_global_block_id: usize,
    next_global_chunk_offset: usize,
    build_hash_idx: usize,
    build_tuple_len: usize,
    hashed_tuple_len: usize,
}

impl<Id: IdType> HashTable<Id> {
    pub fn new(build_hash_idx: usize, hashed_tuple_len: usize) -> HashTable<Id> {
        let mut hash_table = HashTable {
            blocks: vec![],
            extra_blocks: vec![],
            block_ids_and_chunk_offsets: vec![],
            num_chunks: vec![],
            initial_num_blocks: 1000,
            num_tuples_per_chunk: 64,
            num_chunks_per_block: 8000,
            initial_num_chunks_per_vertex: 6,
            block_sz: 0,
            chunk_sz: 0,
            next_global_block_id: 0,
            next_global_chunk_offset: 0,
            build_hash_idx,
            build_tuple_len: hashed_tuple_len + 1,
            hashed_tuple_len,
        };
        hash_table.chunk_sz = hash_table.num_tuples_per_chunk * hash_table.hashed_tuple_len;
        hash_table.block_sz = hash_table.chunk_sz * hash_table.num_chunks_per_block;
        hash_table
    }

    pub fn allocate_initial_memory(&mut self, highest_vertex_id: usize) {
        self.blocks = vec![vec![Id::new(0); self.block_sz]; self.initial_num_blocks];
        self.extra_blocks = vec![vec![]; self.initial_num_blocks];
        self.block_ids_and_chunk_offsets =
            vec![vec![0; self.initial_num_chunks_per_vertex * 3]; highest_vertex_id + 1];
        self.num_chunks = vec![0; highest_vertex_id + 1];
    }
    pub fn insert_tuple(&mut self, build_tuple: Rc<RefCell<Vec<Id>>>) {
        let hash_vertex = build_tuple.borrow()[self.build_hash_idx].id();
        let mut last_chunk_idx = self.num_chunks[hash_vertex];
        if 0 == last_chunk_idx {
            self.num_chunks[hash_vertex] += 1;
            self.update_block_ids_and_global_and_chunk_offset(hash_vertex);
        }
        last_chunk_idx = 3 * (self.num_chunks[hash_vertex] - 1);
        let block_id = self.block_ids_and_chunk_offsets[hash_vertex][last_chunk_idx];
        let start_offset = self.block_ids_and_chunk_offsets[hash_vertex][last_chunk_idx + 1];
        let mut end_offset = self.block_ids_and_chunk_offsets[hash_vertex][last_chunk_idx + 2];
        let block = if block_id < self.initial_num_blocks {
            &mut self.blocks[block_id]
        } else {
            &mut self.extra_blocks[block_id - self.initial_num_blocks]
        };
        for i in 0..self.build_tuple_len {
            if i != self.build_hash_idx {
                block[end_offset] = build_tuple.borrow()[i];
                end_offset += 1;
            }
        }
        self.block_ids_and_chunk_offsets[hash_vertex][last_chunk_idx + 2] = end_offset;
        if self.chunk_sz <= end_offset - start_offset + self.hashed_tuple_len {
            self.num_chunks[hash_vertex] += 1;
            self.resize_block_ids_and_global_and_chunk_offset(hash_vertex);
            self.update_block_ids_and_global_and_chunk_offset(hash_vertex);
        }
    }

    pub fn get_block_and_offsets(
        &self,
        hash_vertex: usize,
        chunk_idx: usize,
        block_info: &mut BlockInfo<Id>,
    ) {
        let block_id = self.block_ids_and_chunk_offsets[hash_vertex][chunk_idx * 3];
        block_info.start_offset = self.block_ids_and_chunk_offsets[hash_vertex][chunk_idx * 3 + 1];
        block_info.end_offset = self.block_ids_and_chunk_offsets[hash_vertex][chunk_idx * 3 + 2];
        block_info.block = if block_id < self.initial_num_blocks {
            self.blocks[block_id].clone()
        } else {
            self.extra_blocks[block_id - self.initial_num_blocks].clone()
        };
    }

    pub fn resize_block_ids_and_global_and_chunk_offset(&mut self, hash_vertex: usize) {
        if self.num_chunks[hash_vertex] + 1
            > (self.block_ids_and_chunk_offsets[hash_vertex].len() / 3)
        {
            let mut new_chunk_block_id_offset_array =
                vec![0; (self.num_chunks[hash_vertex] + 2) * 3];
            self.block_ids_and_chunk_offsets[hash_vertex]
                .iter()
                .enumerate()
                .foreach(|(i, x)| new_chunk_block_id_offset_array[i] = x.clone());
            self.block_ids_and_chunk_offsets[hash_vertex] = new_chunk_block_id_offset_array;
        }
    }
    pub fn update_block_ids_and_global_and_chunk_offset(&mut self, hash_vertex: usize) {
        let last_chunk_idx = (self.num_chunks[hash_vertex] - 1) * 3;
        self.block_ids_and_chunk_offsets[hash_vertex][last_chunk_idx] = self.next_global_block_id;
        self.block_ids_and_chunk_offsets[hash_vertex][last_chunk_idx + 1] =
            self.next_global_chunk_offset;
        self.block_ids_and_chunk_offsets[hash_vertex][last_chunk_idx + 2] =
            self.next_global_chunk_offset;
        self.next_global_chunk_offset += self.chunk_sz;
        if self.next_global_chunk_offset == self.block_sz {
            self.next_global_block_id += 1;
            if self.next_global_block_id >= self.initial_num_blocks {
                self.extra_blocks.push(vec![Id::new(0); self.block_sz]);
            }
            self.next_global_chunk_offset = 0;
        }
    }
}
