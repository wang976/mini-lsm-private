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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
pub use builder::SsTableBuilder;
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice, KeyVec};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize, // 多少字节?  4字节即可
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}
// 其中包含每个块的首尾键以及每个块的偏移量

impl BlockMeta {
    /// 将块元数据编码到缓冲区。
    /// 你可以向缓冲区添加额外字段，
    /// 以便在将来从相同缓冲区解码时帮助跟踪 `first_key`。
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        // unimplemented!()
        // 将 Vec<BlockMeta> 序列化追加到一个内存 buffer 里. 这个 buffer 后面会被 FileObject::create() 一次性写入 SST 文件.
        // 此时 buf 当前内容: block0 | block1 | ... | blockN |.  添加 meta

        // 循环处理每个 meta, 将其 offset, first_key 和 last_key 编码成二进制格式追加到 buf 中.
        // 注意: first_key 和 last_key 都是不定长的, 所以需要和 block 的 entry 有类似格式: first_key_len | first_key | last_key_len | last_key. len 同样保持 2 字节.
        for meta in block_meta {
            // 先写 offset.
            buf.extend_from_slice(&(meta.offset as u32).to_le_bytes());

            // 得到 first_key_len, 再写入 first_key_len.
            let first_key_len = meta.first_key.len() as u16;
            buf.extend_from_slice(&first_key_len.to_le_bytes());
            // 再写 first_key.
            buf.extend_from_slice(meta.first_key.raw_ref());

            // 同上
            let last_key_len = meta.last_key.len() as u16;
            buf.extend_from_slice(&last_key_len.to_le_bytes());
            // 最后写 last_key.
            buf.extend_from_slice(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    // 传进来的参数只有 SsTable 的 meta section 部分.  该函数仅应该看到此部分, open才能看到全部 sstable.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        // unimplemented!()

        // 开始循环读取, buf 是 meta secton.
        let mut metas = Vec::new();
        let mut cur = 0;
        while cur < buf.chunk().len() {
            // 先取出 4B 的 offset.
            let offset = u32::from_le_bytes(buf.chunk()[cur..cur + 4].try_into().unwrap()) as usize;
            cur += 4;

            // 再取 2B 的 first_key_len, 再取 first_key.
            let first_key_len =
                u16::from_le_bytes(buf.chunk()[cur..cur + 2].try_into().unwrap()) as usize;
            cur += 2;
            let first_key = buf.chunk()[cur..cur + first_key_len].to_vec();
            cur += first_key_len;

            // 同上, 取 last_key_len 和 last_key.
            let last_key_len =
                u16::from_le_bytes(buf.chunk()[cur..cur + 2].try_into().unwrap()) as usize;
            cur += 2;
            let last_key = buf.chunk()[cur..cur + last_key_len].to_vec();
            cur += last_key_len;

            // 存入到 metas 中.
            metas.push(BlockMeta {
                offset,
                first_key: KeyVec::from_vec(first_key).into_key_bytes(),
                last_key: KeyVec::from_vec(last_key).into_key_bytes(),
            });
        }

        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    // 从文件的 offset 位置开始, 读取 len 字节, 返回一个 Vec<u8>.
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    // 把一段已经编码好的 SST 字节写入磁盘, 并返回一个可读的 FileObject.
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    // 打开一个已经存在的 SST 文件: 1. 以只读方式打开文件, 2. 通过 metadata 获取文件大小.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
// 表示一个已经构建好的 SST 文件对象.
// 作用: 读路径上使用的数据结构, 保存文件句柄, block元信息, 缓存入口, 以及整个 SST 的边界 key.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject, // 真正的 SST 文件对象. 封装了磁盘文件句柄和文件大小. read_block使用.
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>, // 所有 data block 的 meta 信息.
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize, // metadate section 在文件中的起始偏移.
    id: usize,                            // SST 的唯一编号.
    block_cache: Option<Arc<BlockCache>>, // 这个 SST 可能持有一个共享的 block cache, 也可能没有 cache.
    first_key: KeyBytes,                  // 整个 SST 中最小的key.
    last_key: KeyBytes, // 整个 SST 中最大的key.  用于快速判断一个 key 是否可能在这个 SST 中.
    pub(crate) bloom: Option<Bloom>, // 布隆过滤器.  用于快速判断 "某个 key 一定不存在 / 可能存在"
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64, // 这个 SST 中最大的 timestamp.
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // unimplemented!()
        // 先获取总长度
        let file_size = file.size() as usize;
        let file_data = file.read(0, file.size())?;

        // 读取最后 4字节获取 meta_block_offset, 以便后续读取 block 数据时使用.
        let meta_block_offset =
            u32::from_le_bytes(file_data[file_data.len() - 4..].try_into().unwrap()) as usize;

        // 传入 meta section 数据, 解码出 block_meta 信息.
        let block_meta =
            BlockMeta::decode_block_meta(&file_data[meta_block_offset..file_data.len() - 4]);

        // 从 meta_block_offset 处读取第一个 block 的 first_key, 以便设置 SST 的 first_key.
        let first_block_meta = &block_meta[0];
        let first_key = first_block_meta.first_key.clone();

        // 从 meta_block_offset 处读取最后一个 block 的 last_key, 以便设置 SST 的 last_key.
        let last_block_meta = &block_meta[block_meta.len() - 1];
        let last_key = last_block_meta.last_key.clone();

        // 截断 file_data, 只保留 block 数据部分, 以节省内存.
        // let data = file_data[..meta_block_offset].to_vec();

        Ok(SsTable {
            file,
            block_meta,
            block_meta_offset: meta_block_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        // unimplemented!()
        // 先查 block_meta_offset 获取 metadata section 起始位置.
        let meta_sec_offset = self.block_meta_offset as u64;

        // 再查 block_idx 处 block 的 offset.
        let block_idx_meta = &self.block_meta[block_idx];
        let block_idx_offset = block_idx_meta.offset as u64;

        // 该 block 的起始位置有了, 结束位置为下一次 block 的起始位置.
        let block_end_offset = if block_idx + 1 < self.block_meta.len() {
            self.block_meta[block_idx + 1].offset as u64
        } else {
            meta_sec_offset // 最后一个 block 的结束位置就是 metadata section 的起始位置.
        };

        // 调用 FileObject::read 从磁盘读取 block 数据
        let block_data = self
            .file
            .read(block_idx_offset, block_end_offset - block_idx_offset)?;

        // 再调用 encoder 即可
        Ok(Arc::new(Block::decode(&block_data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    // 对 read_block 的使用可以全部使用 read_block_cached.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        // unimplemented!()
        // block_cached 变量表示为可能有也可能没有.
        // 理解: 此为 read_block 的缓存版本.
        match self.block_cache.clone() {
            Some(cache) => {
                let cache_key = (self.id, block_idx);
                let val = cache.get(&cache_key);
                match val {
                    Some(block) => Ok(block),
                    None => {
                        let block = self.read_block(block_idx)?; // 不执行 cache.insert吗
                        cache.insert(cache_key, block.clone());
                        Ok(block)
                    }
                }
            }
            None => {
                // 没有 cache, 直接调用 read_block 从磁盘读取.
                self.read_block(block_idx)
            }
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // unimplemented!()
        // 还是可以使用二分法
        // 先判断是否在该 SST 中
        if key < self.first_key.as_key_slice() || key > self.last_key.as_key_slice() {
            return usize::MAX; // 不在该 SST 中, 返回一个不合法的 block index.
        }

        // 执行二分法
        let mut left = 0;
        let mut right = self.block_meta.len() - 1;
        while left < right {
            let mid = left + (right - left) / 2;

            // 读取 meta 信息中的 first_key 和 last_key
            let mid_first_key = &self.block_meta[mid].first_key;
            let mid_last_key = &self.block_meta[mid].last_key;

            // 判断
            if key < mid_first_key.as_key_slice() {
                right = mid - 1;
            } else if key > mid_last_key.as_key_slice() {
                left = mid + 1;
            } else {
                return mid; // key 在 mid block 中, 返回 mid index.
            }
        }

        // 最终 left == right. 判断 key 是否在 left block 中.
        let left_first_key = &self.block_meta[left].first_key;
        let left_last_key = &self.block_meta[left].last_key;
        if key >= left_first_key.as_key_slice() && key <= left_last_key.as_key_slice() {
            return left;
        }

        usize::MAX // key 不在该 SST 中, 返回一个不合法的 block index.
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
