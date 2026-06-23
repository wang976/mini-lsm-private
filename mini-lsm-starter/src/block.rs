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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    // 将内部数据编码为课程中示例的数据布局
    // 注意：请检查输出中是否缺少任何预期的字段
    // 真正写入 SST 文件的二进制格式.
    pub fn encode(&self) -> Bytes {
        // unimplemented!()

        // 计算 num_of_elements 数量
        let num_of_elements = self.offsets.len() as u16;

        // 定义返回数据变量.
        let mut data = Vec::new();

        // 先写date, 再写 offsets, 最后写 num_of_elements.
        data.extend_from_slice(&self.data); // 将 data 中的 key-value 数据追加到 data 中.
        // data.extend_from_slice(bytemuck::cast_slice(&self.offsets)); // 将 offsets 转换为字节并追加到 data 中.
        for offset in &self.offsets {
            data.extend_from_slice(&offset.to_le_bytes()); // 将每个 offset 转换为小端字节并追加到 data 中.
        }
        data.extend_from_slice(&num_of_elements.to_le_bytes()); // 将 num_of_elements 转换为小端字节并追加到 data 中.
        Bytes::from(data) // 将 Vec<u8> 转换为 Bytes 并返回.
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // unimplemented!()

        // 添加布隆过滤器后, 其 encode 逻辑需要修改.
        // 真正恢复 key 的工作交给 BlockIterator 来做.  Block 只负责拆分出 date section,  offsets 和 num_of_elements 部分, 不需要解析 entry 内容.
        // 读取最后 2 字节, 得到 num_of_elements.
        let num_of_elements =
            u16::from_le_bytes(data[data.len() - 2..].try_into().unwrap()) as usize;

        // 再继续读取 num_of_elements * 2 字节, 得到 offsets.
        let offsets_start = data.len() - 2 - num_of_elements * 2;
        let offsets_end = data.len() - 2;

        let mut offsets = Vec::with_capacity(num_of_elements);
        for chunk in data[offsets_start..offsets_end].chunks_exact(2) {
            let offset = u16::from_le_bytes(chunk.try_into().unwrap());
            offsets.push(offset);
        }

        // 读取 0 到 offsets_start 的数据, 得到 data 部分.
        let data_section = data[..offsets_start].to_vec();

        // 最后构造 Block.
        Block {
            data: data_section,
            offsets,
        }
    }
}
