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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize, // 区块大小限制.
    /// The first key in the block
    first_key: KeyVec, // 为后续 key 压缩/恢复 key 做准备的字段.
}

// BlockBuilder是正在写入中的 Block.
impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        // unimplemented!()
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    // 控制大小: 如果加入新的kv超过block_size, 返回 false. 表示当前 block 满了, 调用方应先 build 当前 block, 再创建一个 BlockBuilder.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // unimplemented!()
        // fix: 若当前是第一个kv, 即使其长度超过 block_size. 否则会陷入死循环.
        let cursize = self.data.len() + self.offsets.len() * 2; // 当前 block 的大小. 注意 offsets 中每个元素占 2 字节.
        let newsize = cursize + key.len() + value.len() + 2 * 2 + 2 + 2; // 加入新的 kv 后的大小. 注意每个条目还需额外的 4 字节来存储 key 和 value 的长度.
        // fix: 增加 offset和num_of_elements各2字节

        if newsize > self.block_size && !self.data.is_empty() {
            return false; // 加入新的 kv 会超过 block 大小限制, 返回 false.
        }

        // 添加key, value 和 offset.

        // 先计算偏移量.  记录开始位置.
        self.offsets.push(self.data.len() as u16); // 记录当前条目的结束位置, 以便后续解码时使用.

        // 再添加数据.
        self.data
            .extend_from_slice(&(key.len() as u16).to_le_bytes()); // 先写入key的长度
        self.data.extend_from_slice(key.raw_ref()); // 再写入key的内容

        self.data
            .extend_from_slice(&(value.len() as u16).to_le_bytes()); // 先写入value的长度
        self.data.extend_from_slice(value); // 再写入value的内容

        true // 成功加入新的 kv, 返回 true.
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        // unimplemented!()
        let size = self.data.len() + self.offsets.len() * 2; // 当前 block 的大小.
        size == 0
    }

    /// Finalize the block.
    // 生成一个不可继续追加的 Block.
    pub fn build(self) -> Block {
        // unimplemented!()
        // 生成数据部分和未编码的条目偏移量. 存储在 Block 结构中.
        // 你只需将原始区块数据复制到 data 向量中, 并每 2 字节解码一次条目偏移量.

        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
