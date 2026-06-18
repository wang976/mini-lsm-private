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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
// 表示一个正在构建的 SST 文件对象.
// 作用: 内部会先攒当前 block, block 满了就编码到 data. 同时记录一条 BlockMeta.
pub struct SsTableBuilder {
    builder: BlockBuilder, // 当前正在写入的 block. KV会先加到这里, 当其满了, 就将其 build() 成一个 Block.
    first_key: Vec<u8>,    // 当前 block 的第一个key, 非 SST.
    last_key: Vec<u8>, // 当前 block 的最后一个 key. 每次添加kv后需更新. 更新后会变为 BlockMeta 中的 last_key.
    data: Vec<u8>,     // 已经编码好的 data blocks. 即 SST: block0 | block1 | ... | blockN.
    pub(crate) meta: Vec<BlockMeta>, // 已经编码好的 block 的 meta 信息.
    block_size: usize, // 目标 block 大小.  非 SST, 是传给 BlockBuilder 的单个 block 目标大小.
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        // unimplemented!()
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // unimplemented!()
        // 调用 blockbuilder 中的 add(), 若返回错误则重新 new() 一个.
        if self.builder.add(key, value) {
            // fix: 第一个 block 的第一个 key 会空
            if self.first_key.is_empty() {
                self.first_key = key.raw_ref().to_vec();
            }
            // 更新 last_key.
            self.last_key = key.raw_ref().to_vec();
            return;
        }

        // 底层 blockbuilder 调用失败, 说明当前 block 满了.  调用方应先 build 当前 block, 再创建一个 BlockBuilder.
        // 先 build.
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size)); // 执行完后, self.builder 是一个新的 BlockBuilder, old_builder 是之前的那个满了的 BlockBuilder.

        let block = old_builder.build();
        let block_data = block.encode(); // 将 block 编码成二进制格式, 准备写入 SST 中.
        self.data.extend_from_slice(&block_data); // 将 block 的数据追加到 SST 的数据中.

        // 同时记录一条 BlockMeta 信息.
        let meta_offset = self.data.len() - block_data.len();
        self.meta.push(BlockMeta {
            offset: meta_offset,
            // Vec<u8> -> Key<bytest::Bytes>
            first_key: KeyVec::from_vec(self.first_key.clone()).into_key_bytes(),
            last_key: KeyVec::from_vec(self.last_key.clone()).into_key_bytes(),
        });

        // 新建block并添加当前kv.
        // self.builder = BlockBuilder::new(self.block_size);
        let ret = self.builder.add(key, value); // 将当前kv加入新的 block 中.

        // 更新 first_key 和 last_key.
        self.first_key = key.raw_ref().to_vec();
        self.last_key = key.raw_ref().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        // unimplemented!()
        // 以便调用者能够知道何时可以开始一个新的 SST 来写入数据.
        // 假设数据块包含的数据远多于元数据块，我们可以简单地将数据块的大小作为 estimated_size 的返回值.
        self.data.len() + self.block_size // + 当前正在写入的 block 的预留空间.
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // unimplemented!()
        // build 函数将对 SST 进行编码，使用 FileObject::create 将所有内容写入磁盘，并返回一个 SsTable 对象.
        // fix: 此时最后一个正在写的block没有添加到 self.data.
        if !self.builder.is_empty() {
            let block = self.builder.build();
            let block_data = block.encode(); // 将 block 编码成二进制格式, 准备写入 SST 中.
            self.data.extend_from_slice(&block_data); // 将 block 的数据追加到 SST 的数据中.

            // 同时记录一条 BlockMeta 信息.
            let meta_offset = self.data.len() - block_data.len();
            self.meta.push(BlockMeta {
                offset: meta_offset,
                // Vec<u8> -> Key<bytest::Bytes>
                first_key: KeyVec::from_vec(self.first_key.clone()).into_key_bytes(),
                last_key: KeyVec::from_vec(self.last_key.clone()).into_key_bytes(),
            });
        }
        // 将 self.meta 编码到 self.data 中?
        // 得到 meta_offset 再编码 self.meta.  4字节
        let meta_block_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);

        // 将 meta_block_offset 编码追加到 self.data 中.
        self.data
            .extend_from_slice(&(meta_block_offset as u32).to_le_bytes());

        // 先获取 fileobject 编码 self.data
        let obj = FileObject::create(path.as_ref(), self.data.clone())?;

        // 这里的 first_key 和 last_key 是整个 SST 的边界 key.
        // first_key 需要读取, last_ley 可以使用 self.last_key.
        // 读取前两个字节, 获取第一个key的长度, 再根据长度读第一个key.
        let first_key_len = u16::from_le_bytes(self.data[0..2].try_into().unwrap()) as usize;
        let sst_first_key = self.data[2..2 + first_key_len].to_vec();

        // 其他参数来自 形参 和 SsTableBuilder 的字段.
        Ok(SsTable {
            file: obj,
            block_meta: self.meta,
            block_meta_offset: meta_block_offset,
            id,
            block_cache,
            first_key: KeyVec::from_vec(sst_first_key).into_key_bytes(),
            last_key: KeyVec::from_vec(self.last_key.clone()).into_key_bytes(),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
