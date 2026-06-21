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

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        // unimplemented!()
        // 找第 0 个块.  调用现存的api即可.
        let block = table.read_block_cached(0)?;

        Ok(Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(block),
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        // unimplemented!()
        self.blk_idx = 0;
        let block = self.table.read_block_cached(self.blk_idx)?; // TODO: 修改为cache版本
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);

        Ok(())
    }

    // 添加辅助函数
    fn seek_to_key_inner(table: &SsTable, key: KeySlice) -> usize {
        // unimplemented!()
        // 找到第一个 >= key 的块.  使用二分法
        let mut left = 0;
        let mut right = table.block_meta.len();

        while left < right {
            let mid = (left + right) / 2;
            let mid_first_key = &table.block_meta[mid].first_key;
            let mid_last_key = &table.block_meta[mid].last_key;

            if mid_first_key.as_key_slice() > key {
                right = mid;
            } else if mid_last_key.as_key_slice() < key {
                left = mid + 1;
            } else {
                return mid; // 找到了一个块, 其 first_key <= key <= last_key.
            }
        }

        left
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        // unimplemented!()
        // 找到符合条件的第一个块.  这也有对应 api.
        let idx = Self::seek_to_key_inner(&table, key);

        // 处理 idx 是否超出范围的情况.
        if idx >= table.block_meta.len() {
            let block = table.read_block_cached(0)?; // 不必要的读磁盘.
            return Ok(Self {
                table,
                blk_iter: BlockIterator::create_and_seek_to_first(block),
                blk_idx: usize::MAX, // 迭代器无效.
            });
        }

        let block = table.read_block_cached(idx)?;

        Ok(Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_key(block, key),
            blk_idx: idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    // 需要对块元数据进行二分查找, 以确定哪个块可能包含该键. 该键可能不存在于 LSM 树中, 因此块迭代器在执行 seek 后可能会立即失效.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        // unimplemented!()
        let idx = Self::seek_to_key_inner(&self.table, key); // key是判断在不在, 而当前是判断第一个 >= key 的块.

        // 如果当前块已经满足条件了, 就不需要移动了.
        // if self.blk_idx == idx {
        //     return Ok(());
        // }

        // 查看 idx 是否超出范围
        if idx >= self.table.block_meta.len() {
            self.blk_idx = usize::MAX;
            return Ok(());
        }

        // 否则需要移动到新的块.
        self.blk_idx = idx;
        let block = self.table.read_block_cached(self.blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice<'_> {
        // unimplemented!()
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        // unimplemented!()
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        // unimplemented!()
        if self.blk_idx == usize::MAX {
            return false; // 迭代器处于无效状态.
        }
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        // 先调用迭代器的 next(), 若失效, 则移动到下一个块.
        self.blk_iter.next();

        // 查看当前迭代器是否失效
        if self.blk_iter.is_valid() {
            return Ok(());
        }

        // 失效了, 移动到一下个块.
        self.blk_idx += 1;

        // 查看是否已经超出块的范围了.
        if self.blk_idx < self.table.block_meta.len() {
            let block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        }

        Ok(())
    }
}
