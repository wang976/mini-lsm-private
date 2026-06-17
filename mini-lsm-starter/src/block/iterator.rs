// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec, // 迭代器应从块中复制 key 并将其存储在迭代器内部
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize), // 对于值，你只需在迭代器中存储起始/结束偏移量，无需复制它们。
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

// 对 encoder 后的块实现迭代器, 以便用户在块中查找/扫描键.
impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    // 抽象出同一个函数 id -> key-value.  fix: Arc<Block> -> &Block
    fn get_key_value(block: &Block, idx: usize) -> (KeyVec, (usize, usize)) {
        // 先根据 offset 读取在 data section 中的起始位置.
        let entry_begin = block.offsets[idx] as usize;

        // 读取 entry 中前两字节, 获取 key 的长度.
        let key_len =
            u16::from_le_bytes(block.data[entry_begin..entry_begin + 2].try_into().unwrap())
                as usize;
        let key = KeyVec::for_testing_from_vec_no_ts(
            block.data[entry_begin + 2..entry_begin + 2 + key_len].to_vec(),
        );

        // 再继续读 key + key_len 后的两字节, 获取 value 的长度.
        let value_len = u16::from_le_bytes(
            block.data[entry_begin + 2 + key_len..entry_begin + 2 + key_len + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        let value_range = (
            entry_begin + 2 + key_len + 2,
            entry_begin + 2 + key_len + 2 + value_len,
        );

        (key, value_range)
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        // unimplemented!()
        // 创建迭代器, 迭代器将定位到块中的第一个键.

        // 先读前两字节. 得到第一个条目的 key 的长度.
        // let one_key_len = u16::from_le_bytes(block.data[0..2].try_into().unwrap()) as usize;
        // let one_key = KeyVec::for_testing_from_vec_no_ts(block.data[2..2 + one_key_len].to_vec());

        // 再读接下来的2字节.
        // let first_value_len = u16::from_le_bytes(block.data[2 + one_key_len..2 + one_key_len + 2].try_into().unwrap()) as usize;
        let (one_key, (value_start, value_end)) = Self::get_key_value(&block, 0);

        Self {
            block,
            key: one_key.clone(),
            value_range: (value_start, value_end),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        // unimplemented!()
        // 迭代器将定位到第一个 >= 所提供键的键.

        // 这里可以用二分法, 因为 key 有序.  需要从 offsets 数组入手.
        let offset_start = 0;
        let offset_end = block.offsets.len();
        let mut left = offset_start;
        let mut right = offset_end;

        while left < right {
            let mid = left + (right - left) / 2;
            let mid_offset = block.offsets[mid] as usize;
            let mid_key_len =
                u16::from_le_bytes(block.data[mid_offset..mid_offset + 2].try_into().unwrap())
                    as usize;
            let mid_key = KeySlice::for_testing_from_slice_no_ts(
                &block.data[mid_offset + 2..mid_offset + 2 + mid_key_len],
            );
            if mid_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // 最后 left 就是第一个 >= key 的位置. 需要判断一下是否越界.
        if left >= block.offsets.len() {
            // 越界了, 说明没有 >= key 的键. 迭代器无效.
            return Self {
                block,
                key: KeyVec::new(),
                value_range: (0, 0),
                idx: left,
                first_key: KeyVec::new(),
            };
        }

        // 没有越界, 说明找到了第一个 >= key 的键. 需要读取这个键的值.
        let (idx_key, (value_start, value_end)) = Self::get_key_value(&block, left);

        Self {
            block,
            key: idx_key,
            value_range: (value_start, value_end),
            idx: left,
            first_key: KeyVec::new(),
        }
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        // unimplemented!()
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        // unimplemented!()
        // 取出数据
        let (start, end) = self.value_range;
        &self.block.data[start..end]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        // unimplemented!()
        // 看 key 是否为空.
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        // unimplemented!()
        // 这是对应 new() 的 api?  处理同 first_key 的情况.
        let (idx_key, (value_start, value_end)) = Self::get_key_value(&self.block, 0);

        // 修改字段
        self.key = idx_key;
        self.value_range = (value_start, value_end);
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        // unimplemented!()
        // 迭代器将移动到下一个位置。如果到达块的末尾，我们可以将 key 设为空，并从 is_valid 返回 false ，以便调用方在可能的情况下切换到另一个块.

        // 查看 idx 是否已经是末尾
        if self.idx + 1 >= self.block.offsets.len() {
            // 已经是末尾了, 将 key 设为空, 迭代器无效.
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            self.idx += 1; // idx 越界了, 以便 is_valid 返回 false.
            return;
        }

        // 不是末尾, 读取下一个条目. 需要根据 offsets.
        self.idx += 1;
        let (idx_key, (value_start, value_end)) = Self::get_key_value(&self.block, self.idx);

        self.key = idx_key;
        self.value_range = (value_start, value_end);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // unimplemented!()

        // 首先, 必须判断 是否大于等于当前 key.
        // if self.is_valid() && self.key.as_key_slice() >= key {
        //     // 已经满足条件了, 不需要移动.
        //     return;
        // }

        // 同 create_and_seek_to_key 的逻辑
        let offset_start = 0;
        let offset_end = self.block.offsets.len();
        let mut left = offset_start;
        let mut right = offset_end;
        while left < right {
            let mid = left + (right - left) / 2;
            let mid_offset = self.block.offsets[mid] as usize;
            let mid_key_len = u16::from_le_bytes(
                self.block.data[mid_offset..mid_offset + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let mid_key = KeySlice::for_testing_from_slice_no_ts(
                &self.block.data[mid_offset + 2..mid_offset + 2 + mid_key_len],
            );
            if mid_key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // 最后 left 就是第一个 >= key 的位置. 需要判断一下是否越界.
        if left >= self.block.offsets.len() {
            // 越界了, 说明没有 >= key 的键. 迭代器无效.
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            self.idx = left;
            return;
        }

        // 没有越界, 获取
        let (idx_key, (value_start, value_end)) = Self::get_key_value(&self.block, left);

        // 修改
        self.key = idx_key;
        self.value_range = (value_start, value_end);
        self.idx = left;
    }
}
