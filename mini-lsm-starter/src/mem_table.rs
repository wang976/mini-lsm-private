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

use core::sync;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
// wal: 日志, 保证崩溃恢复.
// memtable: 内存有序结构  -> 一般用跳表实现
pub struct MemTable {
    // Bytes 来自 bytes crate，可以理解为便宜 clone 的不可变字节缓冲区
    map: Arc<SkipMap<Bytes, Bytes>>, // Arc<T> 类似于 C++ 中的 std::shared_ptr<T>
    wal: Option<Wal>,
    id: usize,                          // 类似于 C++ 中 size_t.  memtable的编号.
    approximate_size: Arc<AtomicUsize>, // 记录 memtable 大概占用的内存大小，用来判断什么时候需要 freeze/flush
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
// fn 定义函数。pub(crate) 表示“当前 crate 内可见”. &[u8] 是字节 slice，类似 C++ 的 std::span<const uint8_t> 或 string_view。它不拥有数据，只借用一段连续内存。
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        // unimplemented!()
        // 参考 LsmStorageState::create()

        Self {
            map: Arc::new(SkipMap::new()),
            wal: None, // TODO(after): week1 day1
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    // impl AsRef<Path> 表示这个参数可以是任何实现了 AsRef<Path> trait 的类型。可以传 Path、PathBuf、字符串等可转为路径引用的东西。
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        unimplemented!()
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        // This function is only used in week 1 tests, so during the week 3 key-ts refactor, you do
        // not need to consider the bound exclude/include logic. Simply provide `DEFAULT_TS` as the
        // timestamp for the key-ts pair.
        self.scan(lower, upper)
    }

    /// Get a value by key.  Option<Bytes>含义: None 没找到这个 key, Some(bytes) 找到了value.
    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        // unimplemented!()
        // 你需要实现 MemTable::get 和 MemTable::put 以支持对内存表的修改

        // .map(...): 如果是 Some(e), 就把里面的 e 转换成 e.value().clone(); 如果是 None, 就继续保持 None.
        self.map.get(_key).map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        // unimplemented!()
        self.map
            .insert(Bytes::copy_from_slice(_key), Bytes::copy_from_slice(_value));

        // 添加 memtable 占用内存大小
        // 即使某个键被插入两次（尽管跳表仅保留最新值），在计算大致 memtable 大小时仍需将其计算两次。
        let size = self
            .approximate_size
            .fetch_add(_key.len() + _value.len(), sync::atomic::Ordering::Relaxed);

        Ok(()) // 插入成功返回.  以符合 Result<()>签名
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    // Result<()> 这里来自 anyhow::Result，大致等价于 Result<(), anyhow::Error>。() 是 unit 类型，类似 C++ 的 void 值。
    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?; // \ ? 是错误传播操作符。wal.sync()?; 的含义是：如果成功，继续执行；如果失败，立刻从当前函数返回错误
        }
        Ok(()) // 表示返回成功且没有额外值。
    }

    /// Get an iterator over a range of keys.
    // week 1 day 2: 实现 LSM 的 scan 接口.  参数: 指定迭代器的范围
    pub fn scan(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> MemTableIterator {
        // unimplemented!()
        let low = map_bound(_lower);
        let up = map_bound(_upper);

        let map = self.map.clone();
        let mut it = {
            MemTableIteratorBuilder {
                map,
                iter_builder: |map_ref| map_ref.range((low, up)),
                item: (Bytes::default(), Bytes::default()),
            }
            .build() // woc, 这是什么语法.
        };

        let ret = it.next();

        it
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        unimplemented!()
    }

    // &self 是不可变借用当前对象，类似 C++ 的 const MemTable& self，但 Rust 显式写出来
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
///
// 我们已利用 ouroboros 为您定义了自引用的 MemtableIterator 字段。
// 您需要基于此提供的结构实现 MemtableIterator 逻辑和 Memtable::scan API。
#[self_referencing] // ouroboros 库的宏, 其会自动生成构造函数. 用unsafe代码安全保障这个模式, 生成Builder构造器.
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>, //
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

// 类比 C++
// struct MemTableIterator {
//     shared_ptr<SkipMap> map;
//     SkipMapRangeIterator iter; // iter 指向 map 内部
//     pair<Bytes, Bytes> item;
// };

// 为 MemTableIterator 类型实现 StorageIterator 这个接口.
// 即给 MemTableIterator 实现统一的存储迭代器接口.
impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        // unimplemented!()
        &self.borrow_item().1
    }

    fn key(&self) -> KeySlice<'_> {
        // unimplemented!()
        // let slice = &self.borrow_item().0;
        // let key = Key::from_bytes(slice.clone());
        // key.as_key_slice()
        KeySlice::from_slice(self.borrow_item().0.as_ref())
    }

    fn is_valid(&self) -> bool {
        // unimplemented!()
        if self.borrow_item().clone() == (Bytes::default(), Bytes::default()) {
            return false;
        }

        true
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        // ouroboros 会生成访问器。你需要用它提供的方式访问字段.
        self.with_mut(|fields| {
            let entry_opt = fields.iter.next();

            match entry_opt {
                Some(entry) => {
                    fields.item.0 = entry.key().clone();
                    fields.item.1 = entry.value().clone();
                }
                None => {
                    // 无效, 设为默认值
                    fields.item.0 = Bytes::default();
                    fields.item.1 = Bytes::default();
                }
            };
        });

        Ok(())
    }
}
