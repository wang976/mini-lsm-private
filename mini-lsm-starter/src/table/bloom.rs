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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let filter = &buf[..buf.len() - 1];
        let k = buf[buf.len() - 1];
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend(&self.filter);
        buf.put_u8(self.k);
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size = -(entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    // 传入的 keys 已经预计算好的 32 位哈希值数组, 非原始键.
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.clamp(1, 30); // 将值限制在 1 到 30 之间.
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = nbits.div_ceil(8);
        let nbits = nbytes * 8; // 将 nbits 对齐到字节边界.

        // 分配并初始化了对应大小的位数组.
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        // TODO: build the bloom filter
        // 你将根据键哈希值（u32 数字）构建一个布隆过滤器. 对于每个哈希值，你需要设置 k 个位. 注意: keys 是 hash 值数组.
        for h in keys {
            let delta = h.rotate_left(15);
            for i in 0..k {
                let pos = (h.wrapping_add(i.wrapping_mul(delta))) % (nbits as u32); // 溢出处理
                filter.set_bit(pos as usize, true);
            }
        }

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = h.rotate_left(15); // 对 h 做循环左移 15 位, 得到 delta (第二个哈希值)

            // TODO: probe the bloom filter
            // 查对应的 k 个位置, 只要有一个位置为 0 就可以返回 false. 需要注意 nbits 的值.
            for i in 0..self.k {
                let pos = (h.wrapping_add((i as u32).wrapping_mul(delta))) % (nbits as u32);
                if !self.filter.get_bit(pos as usize) {
                    return false;
                }
            }

            true
        }
    }
}
