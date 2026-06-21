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

use std::collections::HashMap;
use std::ops::Bound;
// use std::os::linux::raw::stat;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::Key;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, map_bound};
use crate::mvcc::LsmMvccInner;
use crate::table::SsTable;
use crate::table::SsTableBuilder;
use crate::table::SsTableIterator;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>, // 存储当前正在写的可变内存表.  week1: 目前只会使用 memtable 字段.
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>, // 已冻结、等待 flush 的内存表
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>, // L0 层 SSTable id, 新的在前
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>, // L1+ 或 tiers 的 SSTable id
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>, // id -> SSTable 对象
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    // 创建 LSM 结构时, 会初始化一个 ID 为0的内存表.
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize, //* SSTable 内部每个 block 的目标大小
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize, //* SSTable 目标大小, 也作为 memtable 的近似容量上限.
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize, //* 内存中 memtable 数量上限, 超过后推动 flush
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,   // 是否启用预写日志, 用于崩溃恢复
    pub serializable: bool, // 是否开启 serializable 事务隔离
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,         // 4 kiB
            target_sst_size: 2 << 20, // 2 MB
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    // 访问 memtable 只需要拿 state 读锁. 当要改变 "LSM结构" 本身, 才需要 state.write.
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>, // tip: 要访问 memtable, 你需要获取 state 锁. 你只需获取读锁即可修改内存表.
    pub(crate) state_lock: Mutex<()>, // "互斥"令牌, 拿到这个锁, 就有资格执行一次 LSM 状态变更流程.   不影响读进程.
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // unimplemented!()
        // 用户调用 close 函数时，应等待刷新线程（以及第二周的压缩线程）完成.
        let _ = self
            .flush_thread
            .lock()
            .take()
            .map(|h| h.join())
            .transpose();
        let _ = self
            .compaction_thread
            .lock()
            .take()
            .map(|h| h.join())
            .transpose();

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?; // 启动刷新线程.
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    // 如果 LSM 数据库目录不存在，则需要创建该目录.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        // 处理 path. 如果 LSM 数据库目录不存在，则需要创建该目录.
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        } else if !path.is_dir() {
            anyhow::bail!("Path {} is not a directory", path.display());
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)), // 设置缓存容量. 1024 个 block 的缓存容量, 也就是 4MB 的缓存容量.
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        // unimplemented!()
        let state = {
            let state_guard = self.state.read();
            Arc::clone(&*state_guard)
        };

        // tips: 在探查完所有内存表后，可以创建一个覆盖所有 SST 文件的合并迭代器.
        let val = state.memtable.get(_key);

        // 若没找到key, 则依次查询下一层  memtable -> imm_memtable -> L0 SSTables -> L1/L2/...SSTables.
        if val.is_none() {
            // let state_guard = self.state.read();

            for imm_table in &state.imm_memtables {
                let imm_val = imm_table.get(_key);

                match imm_val {
                    None => {
                        continue;
                    }
                    // 处理空切片, 即墓碑
                    Some(value) if value.is_empty() => {
                        return Ok(None);
                    }
                    Some(value) => {
                        return Ok(Some(value));
                    }
                }
            }

            // 处理 sstable. 目前可仅处理 L0 SSTable.  创建合并迭代器
            let mut sst_iters = Vec::new();
            for sst_id in state.l0_sstables.iter() {
                let sst = state.sstables.get(sst_id).unwrap();
                // let sst_iter = SsTableIterator::create_and_seek_to_first(sst.clone())?;
                // fix: 从设计意图上, 应该使用 create_and_seek_to_key 来创建
                let sst_iter =
                    SsTableIterator::create_and_seek_to_key(sst.clone(), Key::from_slice(_key))?;
                sst_iters.push(Box::new(sst_iter));
            }

            let merge_iter = MergeIterator::create(sst_iters);

            // let mut it = merge_iter;

            // 仅需判断第一个是否等于 key 即可.  因为找到的是 >= key 的第一个元素.
            if merge_iter.is_valid() {
                let key = merge_iter.key();
                if key.to_key_vec().raw_ref() == _key {
                    let value = merge_iter.value();
                    if value.is_empty() {
                        return Ok(None);
                    } else {
                        return Ok(Some(value.to_vec().into()));
                    }
                }
            }
        }

        // 这里也要处理墓碑
        if let Some(ref v) = val
            && v.is_empty()
        {
            return Ok(None);
        }

        // TODO(w1d5): 当前 L0 实现了, 未实现 L1+ 的 SSTable 逻辑

        Ok(val)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        // unimplemented!()
        // 你只需对 state 获取读锁即可修改内存表。这使得多个线程可以并发访问内存表。

        let is_freeze = {
            let state_guard = self.state.read();
            // let state = Arc::clone(&*&state_guard);

            state_guard.memtable.put(_key, _value)?;

            // 要更改 optional 中的尺寸值吗? optional 中是用来对比的.
            let size = state_guard.memtable.approximate_size();

            size > self.options.target_sst_size
        }; // 读锁释放

        if is_freeze {
            // 对 "Lsm" 结构改变, 需要获取 ~State 写锁
            let state_lock_guard = self.state_lock.lock();
            self.force_freeze_memtable(&state_lock_guard)?;
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        // unimplemented!()
        // 你的 delete 实现应仅为该键放入一个空切片，我们称之为删除墓碑标记。你的 get 实现应相应处理这种情况。
        let is_freeze = {
            let state_guard = self.state.read();
            // Arc::clone(&*&state_guard);

            state_guard.memtable.put(_key, b"")?;

            let size = state_guard.memtable.approximate_size();
            size > self.options.target_sst_size
        };

        if is_freeze {
            // 对 "Lsm" 结构改变, 需要获取 ~State 写锁
            let state_lock_guard = self.state_lock.lock();
            self.force_freeze_memtable(&state_lock_guard)?;
        }

        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    // 一旦 memtale 达到限制, 应调用此函数来冻结当前 memtable 并创建新的 memtable.
    // 在修改 LSM 状态时存在多个操作点：冻结可变内存表、将内存表刷入 SST 文件、以及垃圾回收/压缩。所有这些修改过程中都可能涉及 I/O 操作。
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // unimplemented!()
        // 调用此函数前应该获取锁: state_lock
        let mut guard = self.state.write(); // 注意写锁是否重复获取
        let state = Arc::make_mut(&mut *guard); // state 持有对 guard 内部数据的 &mut 借用.

        // 还要检查一下.
        let size = state.memtable.approximate_size();

        // fix: 测试会调用, 故这里不能加判断 size > app_size. 那还要加吗? 加到哪里?
        state.imm_memtables.insert(0, Arc::clone(&state.memtable)); // 克隆一份 Arc 放进去.  fix: 放到最前面
        state.memtable = Arc::new(MemTable::create(self.next_sst_id()));

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    // 实现将数据从内存移动到磁盘的逻辑（即所谓的刷新操作）.
    // 要将内存表刷新到磁盘: 1. 选择一个可刷新的内存表.  2. 创建一个与内存表对应的SST 文件.  3. 将内存表从 imm_memtables 列表中移除, 并将 SST 文件添加到 L0 中.
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // unimplemented!()
        // 首先创建 SSTableBuilder.  注意正确获得锁.  先获取读锁, 再获取写锁
        let state_lock = self.state_lock.lock();
        let state = {
            let guard = self.state.read();
            Arc::clone(&*guard)
        };

        let block_size = self.options.block_size;

        let mut sstbuilder = SsTableBuilder::new(block_size);

        // 获取最早的 imm_memtable, 即末尾元素.
        let imm_mem = match state.imm_memtables.last() {
            Some(imm) => imm,
            None => return Ok(()), // 没有可刷新的内存表, 直接返回
        };

        // 调用 memtable::flush 将数据写入 SSTableBuilder.
        imm_mem.flush(&mut sstbuilder)?;

        // 将 SST 文件添加到 L0 中.  fix: 不用 self.next_sst_id() 来获取 id, 因为 memtable 的 id 已经是 sst_id 了. 直接用 imm_mem 的 id 就行了.
        let sst_id = imm_mem.id();
        // 创建path
        let sst_path = self.path_of_sst(sst_id);
        // 注意: build 是慢操作.
        let sstable = sstbuilder.build(sst_id, Some(self.block_cache.clone()), sst_path)?;

        // build成功, 执行 step3. 更新相关参数. 将内存表从 imm_memtables 列表中移除. 可在这时获取写锁, 因为 flush 比较耗时.
        {
            let mut state_write_guard = self.state.write();
            let state_write = Arc::make_mut(&mut *state_write_guard);

            state_write.imm_memtables.pop();

            state_write.sstables.insert(sst_id, Arc::new(sstable));
            state_write.l0_sstables.insert(0, sst_id); // 新的放在最前面
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    // 为 scan 中跳过不可能包含该键/键范围的 SST. 添加辅助函数
    fn no_range_overlap(&self, sst: &SsTable, lower: &Bound<&[u8]>, upper: &Bound<&[u8]>) -> bool {
        // 没有交集的情况: 1. sst 的最大 key 小于扫描范围的最小 key. 2. sst 的最小 key 大于扫描范围的最大 key.
        // 处理边界情况: 分别处理 included 和 excluded 的情况.
        if let Bound::Included(lower_key) = lower {
            return sst.last_key().raw_ref() < *lower_key;
        }

        if let Bound::Excluded(lower_key) = lower {
            return sst.last_key().raw_ref() <= *lower_key;
        }

        if let Bound::Included(upper_key) = upper {
            return sst.first_key().raw_ref() > *upper_key;
        }

        if let Bound::Excluded(upper_key) = upper {
            return sst.first_key().raw_ref() >= *upper_key;
        }

        false
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // unimplemented!()
        // 借助实现的所有迭代器, 完成 LSM 引擎的 scan 接口.
        // 只需将内存表迭代器（记得将最新内存表放在合并迭代器最前面）组合成 LSM 迭代器，你的存储引擎就能处理扫描请求了.

        // [w1d5] 创建一个覆盖所有内存表和 SST 文件的合并迭代器，从而完成存储引擎的读取路径.

        // 思路: 先调用 memtable 的 scan 获得 memtable iterator 再 得到 vec<Box<>>, 调用merge iterator 的 create. -> 内存表迭代器
        // SST 文件迭代器呢?  其没有对应 scan 操作. 如何调用 api.

        // 你应首先读取 state 并克隆 LSM 状态快照的 Arc 。然后释放锁.
        let snapshot = {
            let state_guard = self.state.read();
            Arc::clone(&*state_guard)
        };
        // 创建 memtable iterator.
        let mut mti_iters = Vec::new();

        // 先添加 memtable.
        let mem_iter = snapshot.memtable.scan(_lower, _upper);
        mti_iters.push(Box::new(mem_iter));

        // 再添加已冻结, 等待 flush 的内存表.
        for imm_mem in snapshot.imm_memtables.clone().iter() {
            let imm_iter = imm_mem.scan(_lower, _upper);
            mti_iters.push(Box::new(imm_iter));
        }

        // 之后再创建 sstable iterator.  遍历所有 L0 SST 文件并为每个文件创建迭代器.
        // w1d6: 跳过不可能包含该键/键范围的 SST.
        let mut sst_iters = Vec::new();
        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables.get(sst_id).unwrap();

            // 利用 sst.first/last_key 来判断是否需要创建迭代器.  如果 sst 的 key 范围与扫描范围没有交集, 则跳过该 sst.
            // 没有交集的情况: 1. sst 的最大 key 小于扫描范围的最小 key. 2. sst 的最小 key 大于扫描范围的最大 key.

            if self.no_range_overlap(sst, &_lower, &_upper) {
                continue;
            }

            // let sst_iter = SsTableIterator::create_and_seek_to_key(sst.clone(), _lower.clone())?;
            let sst_iter = match _lower {
                Bound::Included(lower_key) => SsTableIterator::create_and_seek_to_key(
                    sst.clone(),
                    Key::from_slice(lower_key),
                )?,
                Bound::Excluded(lower_key) => {
                    // 先 seek 到该 key，如果命中了相等的 key，再 skip 一次
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        sst.clone(),
                        Key::from_slice(lower_key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == lower_key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst.clone())?,
            };
            sst_iters.push(Box::new(sst_iter));
        }

        // 调用 mergeiterator的create()
        let a = MergeIterator::create(mti_iters);
        let b = MergeIterator::create(sst_iters);
        let innr_iter = TwoMergeIterator::create(a, b)?;
        let upper_bound = map_bound(_upper);
        let lsm = LsmIterator::new(innr_iter, upper_bound)?;
        let fuse = FusedIterator::new(lsm);

        Ok(fuse)
    }
}
