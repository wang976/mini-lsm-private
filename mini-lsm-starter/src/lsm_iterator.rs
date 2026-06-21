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

use bytes::Bytes;
use std::ops::Bound;

use anyhow::Result;

use crate::{
    iterators::{
        StorageIterator, merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator,
    },
    key::KeySlice,
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
// type LsmIteratorInner = MergeIterator<MemTableIterator>;
// [w1d5] 实现 TwoMergeIterator 后，我们可以将 LsmIteratorInner 的类型修改为以下形式:
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(mut iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        // codex review: 需要保证 key 是否还在 end_bound 之内.
        while iter.is_valid()
            && match &end_bound {
                Bound::Included(upper_key) => iter.key() <= KeySlice::from_slice(upper_key),
                Bound::Excluded(upper_key) => iter.key() < KeySlice::from_slice(upper_key),
                Bound::Unbounded => true,
            }
        {
            if !iter.value().is_empty() {
                break;
            }
            iter.next()?;
        }
        Ok(Self {
            inner: iter,
            end_bound,
        })
    }
}

// LsmIterator不能简单透传 MergrIterator, 需要跳过 tombstone.
// TODO()[w1d5] 你需要修改 LsmIterator 的迭代逻辑，确保当内部迭代器返回的键达到或超过指定的 end_bound 时停止迭代。
impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        // unimplemented!()
        match &self.end_bound {
            Bound::Included(upper_key) => {
                self.inner.is_valid() && self.inner.key().raw_ref() <= upper_key
            }
            Bound::Excluded(upper_key) => {
                self.inner.is_valid() && self.inner.key().raw_ref() < upper_key
            }
            Bound::Unbounded => self.inner.is_valid(),
        }
    }

    fn key(&self) -> &[u8] {
        // unimplemented!()
        // 需注意 KeySlice -> u8
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        // 调用 merge iterator 就行. ❌️
        // 对于 MergeIterator 对应接口的调用 -> 对 TwoMergeIterator 的调用.
        self.inner.next()?;
        // 需要保证当前 value 不为空, 即不是 tombstone.  mergeiterator的next只看key序.
        while self.is_valid() {
            if !self.inner.value().is_empty() {
                break;
            }
            self.inner.next()?;
        }

        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
// 一个包裹现有迭代器的结构，防止用户在迭代器无效时调用 `next`。
// 如果迭代器已经无效，`next` 不会做任何操作。如果 `next` 返回错误，
// 则 `is_valid` 应返回 false，并且之后对 `next` 的调用应始终返回错误。
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        // unimplemented!()
        if self.has_errored {
            false
        } else {
            self.iter.is_valid()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        // unimplemented!()
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        // fix: 先判断 has_errored, 避免在迭代器无效时调用 next.
        if self.has_errored {
            return Err(anyhow::anyhow!("Iterator has errored"));
        }

        if !self.iter.is_valid() {
            return Ok(()); // 若当前 iterator 无效, 应直接返回成功.
        }

        // cargo: map_err -> inspect_err
        self.iter.next().inspect_err(|e| {
            self.has_errored = true;
        })
    }
}
