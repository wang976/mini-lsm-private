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

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>); // I 必须满足 StorageIterator
// usize: iterator的编号, 约小约新.  Box<I>: 真正的底层 iterator
// -> 用 Box<I> 可以理解成把 iterator 放到堆上，HeapWrapper 里只存指针。
// -> BinaryHeap 内部会频繁移动元素，用 Box 可以避免直接移动较复杂/较大的 iterator 对象

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// 重点: HeapWrapper 的排序规则.
impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse() // 二叉堆默认是大根堆, 这里将比较结果反过来.
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>, // 候选 iterator 的堆
    current: Option<HeapWrapper<I>>,   // 当前对外可见的 iterator
}

// 传进来的参数已经经过各自memtable的scan得到iterator, 并且被 "Box" 起来.
impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // unimplemented!()
        // 堆中的数据是 HeapWrapper类型.  那就逐个插入到小根堆中呗
        let mut heap = BinaryHeap::new(); // rust会根据 .push() 自动推断类型参数
        for (idx, it) in iters.into_iter().enumerate() {
            // fix: 当前迭代器可能非法
            if it.is_valid() {
                heap.push(HeapWrapper(idx, it));
            }
        }

        let cur = heap.pop(); // 弹出堆顶

        Self {
            iters: heap,
            current: cur,
        }
    }
}

// MergeIterator 应该对外表现成一个普通有序 iterator
// 内部则是: BinaryHeap 负责找最小 key. index 负责同 key 时选择最新版本. current 负责保存当前对外暴露的位置

// 对任意生命周期 'a, 这个 iterator 的 key 类型都是 KeySlice<'a>.
// 不同 iterator 可以返回不同形式的 key，但这里 MergeIterator 当前只支持 key 类型为 KeySlice 的底层 iterator
impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        // unimplemented!()
        // current 是当前对外可见的 HeapWrapper. 读 current 中的 iterator 的 key
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        // self.current.1.value()
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        // unimplemented!()
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        // 类似于对多个有序数组的多路归并, 利用小根堆, 每个叶子节点都是一个 iterator, 每个iter内部因跳表保持有序. 小根堆根据第一个元素构建
        let mut current = self.current.take().unwrap(); // 取出 current, 以便后续操作
        let old_key = current.1.key().to_key_vec(); // 记录当前 key, 用于后续比较

        // 先next在判断valid.  注意: 当前还在相同iter中, 无重复key.
        current.1.next()?; // 当前 iterator 继续往下走
        if current.1.is_valid() {
            // 若当前还可用, 则重新插入堆中
            self.iters.push(current); // 重新插入堆中, 因为 current 的 key 已经变了, 需要重新调整堆
        }

        // 判断最新的iter中是否有重复key. 这个key是旧的, 需要舍去.
        while let Some(top) = self.iters.peek() {
            if top.1.key() == old_key.as_key_slice() {
                // 若堆顶的 key 与旧 key 相同, 则说明有重复 key, 需要舍去.  循环执行弹出再插入, 直到堆顶的 key 与旧 key 不同为止.
                let mut top = self.iters.pop().unwrap(); // 弹出堆顶
                top.1.next()?; // 这个 iterator 继续往下走
                if top.1.is_valid() {
                    self.iters.push(top); // 若仍然有效, 则重新插入堆中
                }
            } else {
                break; // 若堆顶的 key 与旧 key 不同, 则说明没有重复 key, 可以停止检查
            }
        }

        self.current = self.iters.pop(); // 更新 current 为堆顶的 iterator, 以便下次访问

        Ok(())
    }
}
