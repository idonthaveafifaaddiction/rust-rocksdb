// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


use {DB, Error, Options, WriteOptions, ColumnFamily, ColumnFamilyDescriptor};
use base_db::{BaseDb, BaseDbImpl};
use ffi;

use libc::{self, c_char, c_int, c_uchar, c_void, size_t};
use std::any::Any;
use std::fmt;
use std::ffi::CString;
use std::ops::Deref;
use std::path::Path;
use std::ptr;
use std::slice;
use std::str;

pub fn new_bloom_filter(bits: c_int) -> *mut ffi::rocksdb_filterpolicy_t {
    unsafe { ffi::rocksdb_filterpolicy_create_bloom(bits) }
}

unsafe impl Send for DB {}
unsafe impl Sync for DB {}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DBCompressionType {
    None = ffi::rocksdb_no_compression as isize,
    Snappy = ffi::rocksdb_snappy_compression as isize,
    Zlib = ffi::rocksdb_zlib_compression as isize,
    Bz2 = ffi::rocksdb_bz2_compression as isize,
    Lz4 = ffi::rocksdb_lz4_compression as isize,
    Lz4hc = ffi::rocksdb_lz4hc_compression as isize,
    Zstd = ffi::rocksdb_zstd_compression as isize,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DBCompactionStyle {
    Level = ffi::rocksdb_level_compaction as isize,
    Universal = ffi::rocksdb_universal_compaction as isize,
    Fifo = ffi::rocksdb_fifo_compaction as isize,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DBRecoveryMode {
    TolerateCorruptedTailRecords = ffi::rocksdb_tolerate_corrupted_tail_records_recovery as isize,
    AbsoluteConsistency = ffi::rocksdb_absolute_consistency_recovery as isize,
    PointInTime = ffi::rocksdb_point_in_time_recovery as isize,
    SkipAnyCorruptedRecord = ffi::rocksdb_skip_any_corrupted_records_recovery as isize,
}

/// An atomic batch of write operations.
///
/// Making an atomic commit of several writes:
///
/// ```
/// use rocksdb::{DB, WriteBatch};
///
/// let db = DB::open_default("path/for/rocksdb/storage1").unwrap();
/// {
///     let mut batch = WriteBatch::default();
///     batch.put(b"my key", b"my value");
///     batch.put(b"key2", b"value2");
///     batch.put(b"key3", b"value3");
///     db.write(batch); // Atomically commits the batch
/// }
/// ```
pub struct WriteBatch {
    pub(crate) inner: *mut ffi::rocksdb_writebatch_t,  // FIXME no pub
}

pub struct ReadOptions {
    pub(crate) inner: *mut ffi::rocksdb_readoptions_t,  // FIXME no pub
}

/// A consistent view of the database at the point of creation.
///
/// ```
/// use rocksdb::{DB, IteratorMode};
///
/// let db = DB::open_default("path/for/rocksdb/storage3").unwrap();
/// let snapshot = db.snapshot(); // Creates a longer-term snapshot of the DB, but closed when goes out of scope
/// let mut iter = snapshot.iterator(IteratorMode::Start); // Make as many iterators as you'd like from one snapshot
/// ```
///
pub struct Snapshot<'a> {
    db: &'a BaseDb,
    inner: *const ffi::rocksdb_snapshot_t,
}

/// An iterator over a database or column family, with specifiable
/// ranges and direction.
///
/// This iterator is different to the standard ``DBIterator`` as it aims Into
/// replicate the underlying iterator API within RocksDB itself. This should
/// give access to more performance and flexibility but departs from the
/// widely recognised Rust idioms.
///
/// ```
/// use rocksdb::DB;
///
/// let mut db = DB::open_default("path/for/rocksdb/storage4").unwrap();
/// let mut iter = db.raw_iterator();
///
/// // Forwards iteration
/// iter.seek_to_first();
/// while iter.valid() {
///     println!("Saw {:?} {:?}", iter.key(), iter.value());
///     iter.next();
/// }
///
/// // Reverse iteration
/// iter.seek_to_last();
/// while iter.valid() {
///     println!("Saw {:?} {:?}", iter.key(), iter.value());
///     iter.prev();
/// }
///
/// // Seeking
/// iter.seek(b"my key");
/// while iter.valid() {
///     println!("Saw {:?} {:?}", iter.key(), iter.value());
///     iter.next();
/// }
///
/// // Reverse iteration from key
/// // Note, use seek_for_prev when reversing because if this key doesn't exist,
/// // this will make the iterator start from the previous key rather than the next.
/// iter.seek_for_prev(b"my key");
/// while iter.valid() {
///     println!("Saw {:?} {:?}", iter.key(), iter.value());
///     iter.prev();
/// }
/// ```
pub struct DBRawIterator {
    inner: *mut ffi::rocksdb_iterator_t,
}


/// An iterator over a database or column family, with specifiable
/// ranges and direction.
///
/// ```
/// use rocksdb::{DB, Direction, IteratorMode};
///
/// let mut db = DB::open_default("path/for/rocksdb/storage2").unwrap();
/// let mut iter = db.iterator(IteratorMode::Start); // Always iterates forward
/// for (key, value) in iter {
///     println!("Saw {:?} {:?}", key, value);
/// }
/// iter = db.iterator(IteratorMode::End);  // Always iterates backward
/// for (key, value) in iter {
///     println!("Saw {:?} {:?}", key, value);
/// }
/// iter = db.iterator(IteratorMode::From(b"my key", Direction::Forward)); // From a key in Direction::{forward,reverse}
/// for (key, value) in iter {
///     println!("Saw {:?} {:?}", key, value);
/// }
///
/// // You can seek with an existing Iterator instance, too
/// iter = db.iterator(IteratorMode::Start);
/// iter.set_mode(IteratorMode::From(b"another key", Direction::Reverse));
/// for (key, value) in iter {
///     println!("Saw {:?} {:?}", key, value);
/// }
/// ```
pub struct DBIterator {
    raw: DBRawIterator,
    direction: Direction,
    just_seeked: bool,
}

unsafe impl Send for DBIterator {}

pub enum Direction {
    Forward,
    Reverse,
}

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

pub enum IteratorMode<'a> {
    Start,
    End,
    From(&'a [u8], Direction),
}

impl DBRawIterator {
    pub(crate) fn new(db: &BaseDb, readopts: &ReadOptions) -> DBRawIterator {
        unsafe {
            DBRawIterator { inner: ffi::rocksdb_create_iterator(db.get_base_db(), readopts.inner) }
        }
    }

    pub(crate) fn new_cf(
        db: &BaseDb,
        cf_handle: ColumnFamily,
        readopts: &ReadOptions,
    ) -> Result<DBRawIterator, Error> {
        unsafe {
            Ok(DBRawIterator {
                inner: ffi::rocksdb_create_iterator_cf(
                    db.get_base_db(),
                    readopts.inner,
                    cf_handle.inner
                ),
            })
        }
    }

    /// Returns true if the iterator is valid.
    pub fn valid(&self) -> bool {
        unsafe { ffi::rocksdb_iter_valid(self.inner) != 0 }
    }

    /// Seeks to the first key in the database.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rocksdb::DB;
    ///
    /// let mut db = DB::open_default("path/for/rocksdb/storage5").unwrap();
    /// let mut iter = db.raw_iterator();
    ///
    /// // Iterate all keys from the start in lexicographic order
    ///
    /// iter.seek_to_first();
    ///
    /// while iter.valid() {
    ///    println!("{:?} {:?}", iter.key(), iter.value());
    ///
    ///    iter.next();
    /// }
    ///
    /// // Read just the first key
    ///
    /// iter.seek_to_first();
    ///
    /// if iter.valid() {
    ///    println!("{:?} {:?}", iter.key(), iter.value());
    /// } else {
    ///    // There are no keys in the database
    /// }
    /// ```
    pub fn seek_to_first(&mut self) {
        unsafe {
            ffi::rocksdb_iter_seek_to_first(self.inner);
        }
    }

    /// Seeks to the last key in the database.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rocksdb::DB;
    ///
    /// let mut db = DB::open_default("path/for/rocksdb/storage6").unwrap();
    /// let mut iter = db.raw_iterator();
    ///
    /// // Iterate all keys from the end in reverse lexicographic order
    ///
    /// iter.seek_to_last();
    ///
    /// while iter.valid() {
    ///    println!("{:?} {:?}", iter.key(), iter.value());
    ///
    ///    iter.prev();
    /// }
    ///
    /// // Read just the last key
    ///
    /// iter.seek_to_last();
    ///
    /// if iter.valid() {
    ///    println!("{:?} {:?}", iter.key(), iter.value());
    /// } else {
    ///    // There are no keys in the database
    /// }
    /// ```
    pub fn seek_to_last(&mut self) {
        unsafe {
            ffi::rocksdb_iter_seek_to_last(self.inner);
        }
    }

    /// Seeks to the specified key or the first key that lexicographically follows it.
    ///
    /// This method will attempt to seek to the specified key. If that key does not exist, it will
    /// find and seek to the key that lexicographically follows it instead.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rocksdb::DB;
    ///
    /// let mut db = DB::open_default("path/for/rocksdb/storage7").unwrap();
    /// let mut iter = db.raw_iterator();
    ///
    /// // Read the first key that starts with 'a'
    ///
    /// iter.seek(b"a");
    ///
    /// if iter.valid() {
    ///    println!("{:?} {:?}", iter.key(), iter.value());
    /// } else {
    ///    // There are no keys in the database
    /// }
    /// ```
    pub fn seek(&mut self, key: &[u8]) {
        unsafe {
            ffi::rocksdb_iter_seek(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    /// Seeks to the specified key, or the first key that lexicographically precedes it.
    ///
    /// Like ``.seek()`` this method will attempt to seek to the specified key.
    /// The difference with ``.seek()`` is that if the specified key do not exist, this method will
    /// seek to key that lexicographically precedes it instead.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rocksdb::DB;
    ///
    /// let mut db = DB::open_default("path/for/rocksdb/storage8").unwrap();
    /// let mut iter = db.raw_iterator();
    ///
    /// // Read the last key that starts with 'a'
    ///
    /// iter.seek_for_prev(b"b");
    ///
    /// if iter.valid() {
    ///    println!("{:?} {:?}", iter.key(), iter.value());
    /// } else {
    ///    // There are no keys in the database
    /// }
    pub fn seek_for_prev(&mut self, key: &[u8]) {
        unsafe {
            ffi::rocksdb_iter_seek_for_prev(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    /// Seeks to the next key.
    ///
    /// Returns true if the iterator is valid after this operation.
    pub fn next(&mut self) {
        unsafe {
            ffi::rocksdb_iter_next(self.inner);
        }
    }

    /// Seeks to the previous key.
    ///
    /// Returns true if the iterator is valid after this operation.
    pub fn prev(&mut self) {
        unsafe {
            ffi::rocksdb_iter_prev(self.inner);
        }
    }

    /// Returns a slice to the internal buffer storing the current key.
    ///
    /// This may be slightly more performant to use than the standard ``.key()`` method
    /// as it does not copy the key. However, you must be careful to not use the buffer
    /// if the iterator's seek position is ever moved by any of the seek commands or the
    /// ``.next()`` and ``.previous()`` methods as the underlying buffer may be reused
    /// for something else or freed entirely.
    pub unsafe fn key_inner<'a>(&'a self) -> Option<&'a [u8]> {
        if self.valid() {
            let mut key_len: size_t = 0;
            let key_len_ptr: *mut size_t = &mut key_len;
            let key_ptr = ffi::rocksdb_iter_key(self.inner, key_len_ptr) as *const c_uchar;

            Some(slice::from_raw_parts(key_ptr, key_len as usize))
        } else {
            None
        }
    }

    /// Returns a copy of the current key.
    pub fn key(&self) -> Option<Vec<u8>> {
        unsafe { self.key_inner().map(|key| key.to_vec()) }
    }

    /// Returns a slice to the internal buffer storing the current value.
    ///
    /// This may be slightly more performant to use than the standard ``.value()`` method
    /// as it does not copy the value. However, you must be careful to not use the buffer
    /// if the iterator's seek position is ever moved by any of the seek commands or the
    /// ``.next()`` and ``.previous()`` methods as the underlying buffer may be reused
    /// for something else or freed entirely.
    pub unsafe fn value_inner<'a>(&'a self) -> Option<&'a [u8]> {
        if self.valid() {
            let mut val_len: size_t = 0;
            let val_len_ptr: *mut size_t = &mut val_len;
            let val_ptr = ffi::rocksdb_iter_value(self.inner, val_len_ptr) as *const c_uchar;

            Some(slice::from_raw_parts(val_ptr, val_len as usize))
        } else {
            None
        }
    }

    /// Returns a copy of the current value.
    pub fn value(&self) -> Option<Vec<u8>> {
        unsafe { self.value_inner().map(|value| value.to_vec()) }
    }
}

impl Drop for DBRawIterator {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_iter_destroy(self.inner);
        }
    }
}

impl DBIterator {
    pub(crate) fn new(db: &BaseDb, readopts: &ReadOptions, mode: IteratorMode) -> DBIterator {
        let mut rv = DBIterator {
            raw: DBRawIterator::new(db, readopts),
            direction: Direction::Forward, // blown away by set_mode()
            just_seeked: false,
        };
        rv.set_mode(mode);
        rv
    }

    pub(crate) fn new_cf(
        db: &BaseDb,
        cf_handle: ColumnFamily,
        readopts: &ReadOptions,
        mode: IteratorMode,
    ) -> Result<DBIterator, Error> {
        let mut rv = DBIterator {
            raw: try!(DBRawIterator::new_cf(db, cf_handle, readopts)),
            direction: Direction::Forward, // blown away by set_mode()
            just_seeked: false,
        };
        rv.set_mode(mode);
        Ok(rv)
    }

    pub fn set_mode(&mut self, mode: IteratorMode) {
        match mode {
            IteratorMode::Start => {
                self.raw.seek_to_first();
                self.direction = Direction::Forward;
            }
            IteratorMode::End => {
                self.raw.seek_to_last();
                self.direction = Direction::Reverse;
            }
            IteratorMode::From(key, Direction::Forward) => {
                self.raw.seek(key);
                self.direction = Direction::Forward;
            }
            IteratorMode::From(key, Direction::Reverse) => {
                self.raw.seek_for_prev(key);
                self.direction = Direction::Reverse;
            }
        };

        self.just_seeked = true;
    }

    pub fn valid(&self) -> bool {
        self.raw.valid()
    }
}

impl Iterator for DBIterator {
    type Item = KVBytes;

    fn next(&mut self) -> Option<KVBytes> {
        // Initial call to next() after seeking should not move the iterator
        // or the first item will not be returned
        if !self.just_seeked {
            match self.direction {
                Direction::Forward => self.raw.next(),
                Direction::Reverse => self.raw.prev(),
            }
        } else {
            self.just_seeked = false;
        }

        if self.raw.valid() {
            // .key() and .value() only ever return None if valid == false, which we've just cheked
            Some((
                self.raw.key().unwrap().into_boxed_slice(),
                self.raw.value().unwrap().into_boxed_slice(),
            ))
        } else {
            None
        }
    }
}

impl Into<DBRawIterator> for DBIterator {
    fn into(self) -> DBRawIterator {
        self.raw
    }
}

impl<'a> Snapshot<'a> {
    pub fn new(db: &BaseDb) -> Snapshot {
        // FIXME snaphots are created with a different function call for transactiondb
        let snapshot = unsafe { ffi::rocksdb_create_snapshot(db.get_base_db()) };
        Snapshot {
            db: db,
            inner: snapshot,
        }
    }

    pub fn iterator(&self, mode: IteratorMode) -> DBIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        DBIterator::new(self.db, &readopts, mode)
    }

    pub fn iterator_cf(
        &self,
        cf_handle: ColumnFamily,
        mode: IteratorMode,
    ) -> Result<DBIterator, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        DBIterator::new_cf(self.db, cf_handle, &readopts, mode)
    }

    pub fn raw_iterator(&self) -> DBRawIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        DBRawIterator::new(self.db, &readopts)
    }

    pub fn raw_iterator_cf(&self, cf_handle: ColumnFamily) -> Result<DBRawIterator, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        DBRawIterator::new_cf(self.db, cf_handle, &readopts)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        self.db.get_opt(key, &readopts)
    }

    pub fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_snapshot(self);
        self.db.get_cf_opt(cf, key, &readopts)
    }
}

impl<'a> Drop for Snapshot<'a> {
    fn drop(&mut self) {
        // FIXME transactiondb snaphots need to be released with a different function call
        unsafe {
            ffi::rocksdb_release_snapshot(self.db.get_base_db(), self.inner);
        }
    }
}

impl ColumnFamilyDescriptor {
    // Create a new column family descriptor with the specified name and options.
    pub fn new<S>(name: S, options: Options) -> Self where S: Into<String> {
        ColumnFamilyDescriptor {
            name: name.into(),
            options
        }
    }
}

impl DB {
    /// Open a database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Ok(Self {
            inner: BaseDb::open_default::<P, DbBaseDbImpl>(path)?
        })
    }

    /// Open the database with the specified options.
    pub fn open<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Self, Error> {
        Ok(Self {
            inner: BaseDb::open::<P, DbBaseDbImpl>(opts, path)?
        })
    }

    /// Open a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P: AsRef<Path>>(opts: &Options, path: P, cfs: &[&str]) -> Result<Self, Error> {
        Ok(Self {
            inner: BaseDb::open_cf::<P, DbBaseDbImpl>(opts, path, cfs)?
        })
    }

    /// Open a database with the given database options and column family names/options.
    pub fn open_cf_descriptors<P: AsRef<Path>>(
        opts: &Options,
        path: P,
        cfs: Vec<ColumnFamilyDescriptor>
    ) -> Result<Self, Error> {
        Ok(Self {
            inner: BaseDb::open_cf_descriptors::<P, DbBaseDbImpl>(opts, path, cfs)?
        })
    }

    pub fn list_cf<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Vec<String>, Error> {
        BaseDb::list_cf(opts, path)
    }

    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        BaseDb::destroy(opts, path)
    }

    pub fn repair<P: AsRef<Path>>(opts: Options, path: P) -> Result<(), Error> {
        BaseDb::repair(opts, path)
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<(), Error> {
        self.inner.write_opt(batch, writeopts)
    }

    pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        self.inner.write(batch)
    }

    pub fn write_without_wal(&self, batch: WriteBatch) -> Result<(), Error> {
        self.inner.write_without_wal(batch)
    }

    pub fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DBVector>, Error> {
        self.inner.get_opt(key, readopts)
    }

    /// Return the bytes associated with a key value
    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.inner.get(key)
    }

    pub fn get_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<DBVector>, Error> {
        self.inner.get_cf_opt(cf, key, readopts)
    }

    pub fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.inner.get_cf(cf, key)
    }

    pub fn create_cf(&mut self, name: &str, opts: &Options) -> Result<ColumnFamily, Error> {
        self.inner.create_cf(name, opts)
    }

    pub fn drop_cf(&mut self, name: &str) -> Result<(), Error> {
        self.inner.drop_cf(name)
    }

    /// Return the underlying column family handle.
    pub fn cf_handle(&self, name: &str) -> Option<ColumnFamily> {
        self.inner.cf_handle(name)
    }

    pub fn iterator(&self, mode: IteratorMode) -> DBIterator {
        self.inner.iterator(mode)
    }

    /// Opens an interator with `set_total_order_seek` enabled.
    /// This must be used to iterate across prefixes when `set_memtable_factory` has been called
    /// with a Hash-based implementation.
    pub fn full_iterator(&self, mode: IteratorMode) -> DBIterator {
        self.inner.full_iterator(mode)
    }

    pub fn prefix_iterator<'a>(&self, prefix: &'a [u8]) -> DBIterator {
        self.inner.prefix_iterator(prefix)
    }

    pub fn iterator_cf(
        &self,
        cf_handle: ColumnFamily,
        mode: IteratorMode,
    ) -> Result<DBIterator, Error> {
        self.inner.iterator_cf(cf_handle, mode)
    }

    pub fn full_iterator_cf(
        &self,
        cf_handle: ColumnFamily,
        mode: IteratorMode,
    ) -> Result<DBIterator, Error> {
        self.inner.full_iterator_cf(cf_handle, mode)
    }

    pub fn prefix_iterator_cf<'a>(
        &self,
        cf_handle: ColumnFamily,
        prefix: &'a [u8]
    ) -> Result<DBIterator, Error> {
        self.inner.prefix_iterator_cf(cf_handle, prefix)
    }

    pub fn raw_iterator(&self) -> DBRawIterator {
        self.inner.raw_iterator()
    }

    pub fn raw_iterator_cf(&self, cf_handle: ColumnFamily) -> Result<DBRawIterator, Error> {
        self.inner.raw_iterator_cf(cf_handle)
    }

    pub fn snapshot(&self) -> Snapshot {
        self.inner.snapshot()
    }

    pub fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        self.inner.put_opt(key, value, writeopts)
    }

    pub fn put_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        self.inner.put_cf_opt(cf, key, value, writeopts)
    }

    pub fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        self.inner.merge_opt(key, value, writeopts)
    }

    pub fn merge_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        self.inner.merge_cf_opt(cf, key, value, writeopts)
    }

    pub fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        self.inner.delete_opt(key, &writeopts)
    }

    pub fn delete_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        self.inner.delete_cf_opt(cf, key, writeopts)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.inner.put(key, value)
    }

    pub fn put_cf(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.inner.put_cf(cf, key, value)
    }

    pub fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.inner.merge(key, value)
    }

    pub fn merge_cf(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.inner.merge_cf(cf, key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.inner.delete(key)
    }

    pub fn delete_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        self.inner.delete_cf(cf, key)
    }

    pub fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) {
        self.inner.compact_range(start, end)
    }

    pub fn compact_range_cf(&self, cf: ColumnFamily, start: Option<&[u8]>, end: Option<&[u8]>) {
        self.inner.compact_range_cf(cf, start, end)
    }
}

impl WriteBatch {
    pub fn len(&self) -> usize {
        unsafe { ffi::rocksdb_writebatch_count(self.inner) as usize }
    }

    /// Return WriteBatch serialized size (in bytes).
    pub fn size_in_bytes(&self) -> usize {
        unsafe {
            let mut batch_size: size_t = 0;
            ffi::rocksdb_writebatch_data(self.inner, &mut batch_size);
            batch_size as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert a value into the database under the given key.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_put(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn put_cf(&mut self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_put_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn merge(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_merge(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn merge_cf(&mut self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_merge_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
            Ok(())
        }
    }

    /// Remove the database entry for key.
    ///
    /// Returns an error if the key was not found.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
            Ok(())
        }
    }

    pub fn delete_cf(&mut self, cf: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_writebatch_delete_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
            Ok(())
        }
    }
}

impl Default for WriteBatch {
    fn default() -> WriteBatch {
        WriteBatch { inner: unsafe { ffi::rocksdb_writebatch_create() } }
    }
}

impl Drop for WriteBatch {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_writebatch_destroy(self.inner) }
    }
}

impl fmt::Debug for DB {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksDB {{ path: {:?} }}", self.path())
    }
}

impl Drop for ReadOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_readoptions_destroy(self.inner) }
    }
}

impl ReadOptions {
    // TODO add snapshot setting here
    // TODO add snapshot wrapper structs with proper destructors;
    // that struct needs an "iterator" impl too.
    #[allow(dead_code)]
    fn fill_cache(&mut self, v: bool) {
        unsafe {
            ffi::rocksdb_readoptions_set_fill_cache(self.inner, v as c_uchar);
        }
    }

    fn set_snapshot(&mut self, snapshot: &Snapshot) {
        unsafe {
            ffi::rocksdb_readoptions_set_snapshot(self.inner, snapshot.inner);
        }
    }

    pub fn set_iterate_upper_bound(&mut self, key: &[u8]) {
        unsafe {
            ffi::rocksdb_readoptions_set_iterate_upper_bound(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    pub fn set_prefix_same_as_start(&mut self, v: bool) {
        unsafe {
            ffi::rocksdb_readoptions_set_prefix_same_as_start(self.inner, v as c_uchar)
        }
    }

    pub fn set_total_order_seek(&mut self, v:bool) {
        unsafe {
            ffi::rocksdb_readoptions_set_total_order_seek(self.inner, v as c_uchar)
        }
    }
}

impl Default for ReadOptions {
    fn default() -> ReadOptions {
        unsafe { ReadOptions { inner: ffi::rocksdb_readoptions_create() } }
    }
}

/// Vector of bytes stored in the database.
///
/// This is a `C` allocated byte array and a length value.
/// Normal usage would be to utilize the fact it implements `Deref<[u8]>` and use it as
/// a slice.
pub struct DBVector {
    base: *mut u8,
    len: usize,
}

impl Deref for DBVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.base, self.len) }
    }
}

impl Drop for DBVector {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.base as *mut c_void);
        }
    }
}

impl DBVector {
    /// Used internally to create a DBVector from a `C` memory block
    ///
    /// # Unsafe
    /// Requires that the ponter be allocated by a `malloc` derivative (all C libraries), and
    /// `val_len` be the length of the C array to be safe (since `sizeof(u8) = 1`).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let buf_len: libc::size_t = unsafe { mem::uninitialized() };
    /// // Assume the function fills buf_len with the length of the returned array
    /// let buf: *mut u8 = unsafe { ffi_function_returning_byte_array(&buf_len) };
    /// DBVector::from_c(buf, buf_len)
    /// ```
    pub unsafe fn from_c(val: *mut u8, val_len: size_t) -> DBVector {
        DBVector {
            base: val,
            len: val_len as usize,
        }
    }

    /// Convenience function to attempt to reinterperet value as string.
    ///
    /// implemented as `str::from_utf8(&self[..])`
    pub fn to_utf8(&self) -> Option<&str> {
        str::from_utf8(self.deref()).ok()
    }
}

pub struct DbBaseDbImpl {
    inner: *mut ffi::rocksdb_t,
    initialized: bool
}

impl Drop for DbBaseDbImpl {
    fn drop(&mut self) {
        if self.initialized {
            unsafe { ffi::rocksdb_close(self.inner); }
        }
    }
}

impl Default for DbBaseDbImpl {
    fn default() -> Self {
        Self {
            inner: ptr::null_mut(),
            initialized: false
        }
    }
}

impl BaseDbImpl for DbBaseDbImpl {
    fn open_raw(&mut self, opts: &Options, cpath: &CString) -> Result<(), Error> {
        if self.initialized {
            return Err(Error::new("Database is already open".into()));
        }
        let db = unsafe {
            ffi_try!(ffi::rocksdb_open(
                opts.inner,
                cpath.as_ptr() as *const _,
            ))
        };
        if db.is_null() {
            return Err(Error::new("Failed to open database".into()));
        }
        self.inner = db;
        self.initialized = true;
        Ok(())
    }

    fn open_cf_raw(
        &mut self,
        opts: &Options,
        cpath: &CString,
        num_cfs: usize,
        cfnames: &mut Vec<*const c_char>,
        cfopts: &mut Vec<*const ffi::rocksdb_options_t>,
        cfhandles: &mut Vec<*mut ffi::rocksdb_column_family_handle_t>
    ) -> Result<(), Error> {
        if self.initialized {
            return Err(Error::new("Database is already open".into()));
        }
        let db = unsafe {
            ffi_try!(ffi::rocksdb_open_column_families(
                opts.inner,
                cpath.as_ptr(),
                num_cfs as c_int,
                cfnames.as_mut_ptr(),
                cfopts.as_mut_ptr(),
                cfhandles.as_mut_ptr(),
            ))
        };
        if db.is_null() {
            return Err(Error::new("Failed to open database".into()));
        }
        self.inner = db;
        self.initialized = true;
        Ok(())
    }

    fn get_base_db(&self) -> Result<*mut ffi::rocksdb_t, Error> {
        if !self.initialized {
            return Err(Error::new("Database is not open".into()));
        }
        Ok(self.inner)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[test]
fn test_db_vector() {
    use std::mem;
    let len: size_t = 4;
    let data: *mut u8 = unsafe { mem::transmute(libc::calloc(len, mem::size_of::<u8>())) };
    let v = unsafe { DBVector::from_c(data, len) };
    let ctrl = [0u8, 0, 0, 0];
    assert_eq!(&*v, &ctrl[..]);
}


#[test]
fn external() {
    let path = "_rust_rocksdb_externaltest";
    {
        let db = DB::open_default(path).unwrap();
        let p = db.put(b"k1", b"v1111");
        assert!(p.is_ok());
        let r: Result<Option<DBVector>, Error> = db.get(b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
        assert!(db.delete(b"k1").is_ok());
        assert!(db.get(b"k1").unwrap().is_none());
    }
    let opts = Options::default();
    let result = DB::destroy(&opts, path);
    assert!(result.is_ok());
}

#[test]
fn errors_do_stuff() {
    let path = "_rust_rocksdb_error";
    let _db = DB::open_default(path).unwrap();
    let opts = Options::default();
    // The DB will still be open when we try to destroy it and the lock should fail.
    match DB::destroy(&opts, path) {
        Err(s) => {
            let message = s.to_string();
            assert!(message.find("IO error:").is_some());
            assert!(message.find("_rust_rocksdb_error/LOCK:").is_some());
        }
        Ok(_) => panic!("should fail"),
    }
}

#[test]
fn writebatch_works() {
    let path = "_rust_rocksdb_writebacktest";
    {
        let db = DB::open_default(path).unwrap();
        {
            // test put
            let mut batch = WriteBatch::default();
            assert!(db.get(b"k1").unwrap().is_none());
            assert_eq!(batch.len(), 0);
            assert!(batch.is_empty());
            let _ = batch.put(b"k1", b"v1111");
            assert_eq!(batch.len(), 1);
            assert!(!batch.is_empty());
            assert!(db.get(b"k1").unwrap().is_none());
            let p = db.write(batch);
            assert!(p.is_ok());
            let r: Result<Option<DBVector>, Error> = db.get(b"k1");
            assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
        }
        {
            // test delete
            let mut batch = WriteBatch::default();
            let _ = batch.delete(b"k1");
            assert_eq!(batch.len(), 1);
            assert!(!batch.is_empty());
            let p = db.write(batch);
            assert!(p.is_ok());
            assert!(db.get(b"k1").unwrap().is_none());
        }
        {
            // test size_in_bytes
            let mut batch = WriteBatch::default();
            let before = batch.size_in_bytes();
            let _ = batch.put(b"k1", b"v1234567890");
            let after = batch.size_in_bytes();
            assert!(before + 10 <= after);
        }
    }
    let opts = Options::default();
    assert!(DB::destroy(&opts, path).is_ok());
}

#[test]
fn iterator_test() {
    let path = "_rust_rocksdb_iteratortest";
    {
        let db = DB::open_default(path).unwrap();
        let p = db.put(b"k1", b"v1111");
        assert!(p.is_ok());
        let p = db.put(b"k2", b"v2222");
        assert!(p.is_ok());
        let p = db.put(b"k3", b"v3333");
        assert!(p.is_ok());
        let iter = db.iterator(IteratorMode::Start);
        for (k, v) in iter {
            println!(
                "Hello {}: {}",
                str::from_utf8(&*k).unwrap(),
                str::from_utf8(&*v).unwrap()
            );
        }
    }
    let opts = Options::default();
    assert!(DB::destroy(&opts, path).is_ok());
}

#[test]
fn snapshot_test() {
    let path = "_rust_rocksdb_snapshottest";
    {
        let db = DB::open_default(path).unwrap();
        let p = db.put(b"k1", b"v1111");
        assert!(p.is_ok());

        let snap = db.snapshot();
        let r: Result<Option<DBVector>, Error> = snap.get(b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");

        let p = db.put(b"k2", b"v2222");
        assert!(p.is_ok());

        assert!(db.get(b"k2").unwrap().is_some());
        assert!(snap.get(b"k2").unwrap().is_none());
    }
    let opts = Options::default();
    assert!(DB::destroy(&opts, path).is_ok());
}
