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
        // 这个才是最难的.
        // 思路: 从先末尾读取 num_of_elements, 再根据 num_of_elements 读取 offsets, 最后根据 offsets 读取 data 中的 key-value 数据.

        // 读取最后 2 字节, 得到 num_of_elements.
        let num_of_elements =
            u16::from_le_bytes(data[data.len() - 2..].try_into().unwrap()) as usize;

        // 再继续读取 num_of_elements * 2 字节, 得到 offsets.
        let offsets_start = data.len() - 2 - num_of_elements * 2;
        let offsets_end = data.len() - 2;
        // let offsets = bytemuck::cast_slice(&data[offsets_start..offsets_end]).to_vec();
        let mut offsets = Vec::with_capacity(num_of_elements);
        for chunk in data[offsets_start..offsets_end].chunks_exact(2) {
            let offset = u16::from_le_bytes(chunk.try_into().unwrap());
            offsets.push(offset);
        }

        // 循环遍历 offsets 数组, 根据 offsets 中的偏移量读取 data 中的 key-value 数据.
        let mut kv_data = Vec::new();
        // for i in 0..num_of_elements {
        for &raw_offset in &offsets {
            // let offset = offsets[i] as usize; // 当前条目的结束位置
            let offset = raw_offset as usize; // 当前条目的结束位置
            let key_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize; // 读取前2字节获取 key 的长度
            let key_start = offset + 2; // key 的起始位置
            let key_end = key_start + key_len; // key 的结束位置
            let value_len =
                u16::from_le_bytes(data[key_end..key_end + 2].try_into().unwrap()) as usize; // 继续读2字节获取 value 的长度
            let value_start = key_end + 2; // value 的起始位置
            let value_end = value_start + value_len; // value 的结束位置
            kv_data.extend_from_slice(&data[offset..value_end]); // 将当前条目的 key 和 value 数据追加到 kv_data 中.
        }

        // 最后构造 Block.
        Block {
            data: kv_data,
            offsets,
        }
    }
}
