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

use anyhow::{Ok, Result};

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
// A 表示优先级更高的迭代器, 在当前 A 表示  memtable 的 mergeiterator, B 表示 SsTableIterator.
// 作用: 把两个 已经按 key 排好序 的 iterator 合成一个有序 iterator, 而且允许这两个 iterator 是不同具体类型.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    // 为表示有序, 我们可以添加字段表示先输出那个迭代器的元素.
    prior_a: bool,

    // 当前字段是否相同. -> 相同的话两个迭代器都要执行 next.
    cur_equal: bool,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        // unimplemented!()

        // 分情形:
        let mut prior = false;
        let mut equal = false;
        if a.is_valid() {
            if b.is_valid() {
                // 判断移动后的元素优先级以及是否相同
                prior = a.key() <= b.key(); // fix: 小的
                equal = a.key() == b.key();
            } else {
                // 对应情形 2.
                prior = true;
                equal = false;
            }
        } else {
            if b.is_valid() {
                // 对应情形3.
                prior = false;
                equal = false
            }
        }
        Ok(Self {
            a,
            b,
            prior_a: prior,
            cur_equal: equal,
        })
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        // unimplemented!()
        if self.prior_a && self.a.is_valid() {
            return self.a.key();
        }

        // 否则输出 b.
        self.b.key()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        if self.prior_a && self.a.is_valid() {
            return self.a.value();
        }

        self.b.value()
    }

    fn is_valid(&self) -> bool {
        // unimplemented!()
        // 必须是两个都失效才能算作无效.
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()

        // 需要考虑四种情况: 1. 均有效  2. a 有效, b 无效  3. a 无效, b 有效  4. 均无效.

        // 若 a 优先高, 则表示 a 对应 kv 已输出, 先移动 a.  需要判断当前
        if self.prior_a && self.a.is_valid() {
            // 子迭代器应该保证其内部不存在重复 key.
            // 说明当前 A 优先级高, 移动 A.
            self.a.next()?;

            if self.cur_equal && self.b.is_valid() {
                // 相等, 但 A 优先级高已经输出. 故 B 也要执行下一个.  若 B 失效则不移动.
                self.b.next()?;
            }
        } else if self.b.is_valid() {
            // 否则, 说明 B 优先级好, 移动 B 即可.
            self.b.next()?;
        }

        if self.a.is_valid() {
            if self.b.is_valid() {
                // 判断移动后的元素优先级以及是否相同
                self.prior_a = self.a.key() <= self.b.key(); // fix: 小的
                self.cur_equal = self.a.key() == self.b.key();
            } else {
                // 对应情形 2.
                self.prior_a = true;
                self.cur_equal = false;
            }
        } else {
            if self.b.is_valid() {
                // 对应情形3.
                self.prior_a = false;
                self.cur_equal = false
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let mut num = 0;
        if self.a.is_valid() {
            num += self.a.num_active_iterators();
        }
        if self.b.is_valid() {
            num += self.b.num_active_iterators();
        }

        num
    }
}
