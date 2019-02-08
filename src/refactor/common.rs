// Copyright 2018. Starry, Inc. All Rights Reserved.
//
// FIXME based off prior work...
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
// Software written by Steven Sloboda <ssloboda@starry.com>.

use std::{ops::Deref, sync::Arc};

use libc::{c_char, c_int, c_uchar, c_void};

use ffi;
use refactor::database::{InnerDB, InnerDbType, Options};
use refactor::transaction::Transaction;

///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReadOptions {
    pub(crate) inner: *mut ffi::rocksdb_readoptions_t
}

// FIXME this okay?
unsafe impl Send for ReadOptions {}

impl Default for ReadOptions {
    fn default() -> Self {
        let inner = unsafe {
            ffi::rocksdb_readoptions_create()
        };
        if inner.is_null() {
            panic!("Could not create RocksDB read options");
        }
        Self { inner }
    }
}

impl Drop for ReadOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_readoptions_destroy(self.inner)
        }
    }
}

impl ReadOptions {
    pub fn new() -> Self {
        Self::default()
    }

    // FIXME what's this about?
    #[allow(dead_code)]
    fn fill_cache(&mut self, enable: bool) {
        unsafe {
            ffi::rocksdb_readoptions_set_fill_cache(self.inner, enable as c_uchar);
        }
    }

    pub fn set_snapshot(&mut self, snapshot: &Snapshot) {
        unsafe {
            ffi::rocksdb_readoptions_set_snapshot(self.inner, snapshot.inner);
        }
    }

    pub fn set_iterate_upper_bound(&mut self, key: &[u8]) {
        unsafe {
            ffi::rocksdb_readoptions_set_iterate_upper_bound(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len()
            );
        }
    }

    pub fn set_prefix_same_as_start(&mut self, enable: bool) {
        unsafe {
            ffi::rocksdb_readoptions_set_prefix_same_as_start(self.inner, enable as c_uchar)
        }
    }

    pub fn set_total_order_seek(&mut self, enable: bool) {
        unsafe {
            ffi::rocksdb_readoptions_set_total_order_seek(self.inner, enable as c_uchar)
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WriteOptions {
    pub(crate) inner: *mut ffi::rocksdb_writeoptions_t
}

// FIXME this okay?
unsafe impl Send for WriteOptions {}

impl Default for WriteOptions {
    fn default() -> Self {
        let inner = unsafe {
            ffi::rocksdb_writeoptions_create()
        };
        if inner.is_null() {
            panic!("Could not create RocksDB write options");
        }
        Self { inner }
    }
}

impl Drop for WriteOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_writeoptions_destroy(self.inner);
        }
    }
}

impl WriteOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_sync(&mut self, sync: bool) {
        unsafe {
            ffi::rocksdb_writeoptions_set_sync(self.inner, sync as c_uchar);
        }
    }

    pub fn disable_wal(&mut self, disable: bool) {
        unsafe {
            ffi::rocksdb_writeoptions_disable_WAL(self.inner, disable as c_int);
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Copy, Clone)]  // XXX(ssloboda) why was this needed?
pub struct ColumnFamily {
    pub(crate) inner: *mut ffi::rocksdb_column_family_handle_t
}

// XXX(ssloboda) why was this deemed okay?
unsafe impl Send for ColumnFamily {}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// A description of the RocksDB column family, containing the name and `Options`.
pub struct ColumnFamilyDescriptor {
    pub(crate) name: String,
    pub(crate) options: Options
}

impl ColumnFamilyDescriptor {
    // Create a new column family descriptor with the specified name and options.
    pub fn new<S>(name: S, options: Options) -> Self
        where S: Into<String>
    {
        ColumnFamilyDescriptor {
            name: name.into(),
            options
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Vector of bytes stored in the database.
///
/// This is a `C` allocated byte array and a length value.
/// Normal usage would be to utilize the fact it implements `Deref<[u8]>` and use it as
/// a slice.
pub struct DatabaseVector {
    base: *mut u8,
    len: usize
}

impl Deref for DatabaseVector {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            ::std::slice::from_raw_parts(self.base, self.len)
        }
    }
}

impl Drop for DatabaseVector {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.base as *mut c_void);
        }
    }
}

impl DatabaseVector {
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
    pub(crate) unsafe fn from_c(val: *mut u8, val_len: usize) -> Self {
        Self {
            base: val,
            len: val_len
        }
    }

    /// Convenience function to attempt to reinterperet value as string.
    ///
    /// implemented as `str::from_utf8(&self[..])`
    pub fn to_utf8(&self) -> Option<&str> {
        ::std::str::from_utf8(self.deref()).ok()
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Give this to `ReadOptions` to read/iterate on a snapshot.
pub struct Snapshot {
    inner: *const ffi::rocksdb_snapshot_t,
    db: InnerDbType
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        match self.db {
            InnerDbType::DB(ref db) => unsafe {
                ffi::rocksdb_release_snapshot(db.inner, self.inner)
            },
            InnerDbType::TxnDB(ref db) => unsafe {
                ffi::rocksdb_transactiondb_release_snapshot(db.inner, self.inner)
            }
        }
    }
}

impl Snapshot {
    pub(crate) fn from_innerdbtype(db: InnerDbType) -> Snapshot {
        let snapshot = match db {
            InnerDbType::DB(ref db) => unsafe {
                ffi::rocksdb_create_snapshot(db.inner)
            },
            InnerDbType::TxnDB(ref db) => unsafe {
                ffi::rocksdb_transactiondb_create_snapshot(db.inner)
            }
        };
        if snapshot.is_null() {
            panic!("Cannot create RocksDB snapshot");
        }
        Self {
            inner: snapshot,
            db: db
        }
    }

    pub(crate) fn from_txn(txn: &Transaction) -> Snapshot {
        let snapshot = unsafe {
            ffi::rocksdb_transaction_get_snapshot(txn.inner)
        };
        if snapshot.is_null() {
            panic!("Cannot create RocksDB snapshot");
        }
        Self {
            inner: snapshot,
            db: txn.db.clone()
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RawDatabaseIterator {
    inner: *mut ffi::rocksdb_iterator_t,
    // Keep the DB alive while we're alive.
    _db: InnerDbType
}

impl RawDatabaseIterator {
    pub(crate) fn from_raw(inner: *mut ffi::rocksdb_iterator_t, db: InnerDbType) -> Self {
        if inner.is_null() {
            panic!("Unable to create RocksDB iterator")
        }
        Self {
            inner,
            _db: db
        }
    }

    pub(crate) fn from_innerdbtype(db: InnerDbType, readopts: &ReadOptions) -> Self {
        let inner = match db {
            InnerDbType::DB(ref db) => unsafe {
                ffi::rocksdb_create_iterator(db.inner, readopts.inner)
            },
            InnerDbType::TxnDB(ref db) => unsafe {
                ffi::rocksdb_transactiondb_create_iterator(db.inner, readopts.inner)
            }
        };
        if inner.is_null() {
            panic!("Unable to create RocksDB iterator")
        }
        Self {
            inner,
            _db: db
        }
    }

    pub(crate) fn from_db_cf(
        db: Arc<InnerDB>,
        cf_handle: ColumnFamily,
        readopts: &ReadOptions,
    ) -> Self {
        let inner = unsafe {
            ffi::rocksdb_create_iterator_cf(db.inner, readopts.inner, cf_handle.inner)
        };
        if inner.is_null() {
            panic!("Unable to create RocksDB cf iterator")
        }
        Self {
            inner,
            _db: InnerDbType::DB(db)
        }
    }

    /// Returns true if the iterator is valid.
    pub fn valid(&self) -> bool {
        unsafe {
            ffi::rocksdb_iter_valid(self.inner) != 0
        }
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
                key.len()
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
                key.len()
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
            let mut key_len = 0;
            let key_ptr = ffi::rocksdb_iter_key(self.inner, &mut key_len) as *const c_uchar;
            Some(::std::slice::from_raw_parts(key_ptr, key_len))
        } else {
            None
        }
    }

    /// Returns a copy of the current key.
    pub fn key(&self) -> Option<Vec<u8>> {
        unsafe {
            self.key_inner().map(|key| key.to_vec())
        }
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
            let mut val_len = 0;
            let val_ptr = ffi::rocksdb_iter_value(self.inner, &mut val_len) as *const c_uchar;

            Some(::std::slice::from_raw_parts(val_ptr, val_len))
        } else {
            None
        }
    }

    /// Returns a copy of the current value.
    pub fn value(&self) -> Option<Vec<u8>> {
        unsafe { self.value_inner().map(|value| value.to_vec()) }
    }
}

impl Drop for RawDatabaseIterator {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_iter_destroy(self.inner);
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub enum DatabaseIteratorDirection {
    Forward,
    Reverse
}

pub enum DatabaseIteratorMode<'a> {
    Start,
    End,
    From(&'a [u8], DatabaseIteratorDirection)
}

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

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
pub struct DatabaseIterator {
    raw: RawDatabaseIterator,
    direction: DatabaseIteratorDirection,
    just_seeked: bool
}

// XXX(ssloboda) why was this deemed okay?
unsafe impl Send for DatabaseIterator {}

impl DatabaseIterator {
    pub(crate) fn from_raw(raw: RawDatabaseIterator, mode: DatabaseIteratorMode) -> Self {
        let mut rv = Self {
            raw,
            direction: DatabaseIteratorDirection::Forward, // blown away by set_mode()
            just_seeked: false,
        };
        rv.set_mode(mode);
        rv
    }

    pub fn set_mode(&mut self, mode: DatabaseIteratorMode) {
        match mode {
            DatabaseIteratorMode::Start => {
                self.raw.seek_to_first();
                self.direction = DatabaseIteratorDirection::Forward;
            }
            DatabaseIteratorMode::End => {
                self.raw.seek_to_last();
                self.direction = DatabaseIteratorDirection::Reverse;
            }
            DatabaseIteratorMode::From(key, DatabaseIteratorDirection::Forward) => {
                self.raw.seek(key);
                self.direction = DatabaseIteratorDirection::Forward;
            }
            DatabaseIteratorMode::From(key, DatabaseIteratorDirection::Reverse) => {
                self.raw.seek_for_prev(key);
                self.direction = DatabaseIteratorDirection::Reverse;
            }
        };

        self.just_seeked = true;
    }

    pub fn valid(&self) -> bool {
        self.raw.valid()
    }
}

impl Iterator for DatabaseIterator {
    type Item = KVBytes;

    fn next(&mut self) -> Option<KVBytes> {
        // Initial call to next() after seeking should not move the iterator
        // or the first item will not be returned
        if self.just_seeked {
            self.just_seeked = false;
        } else {
            match self.direction {
                DatabaseIteratorDirection::Forward => self.raw.next(),
                DatabaseIteratorDirection::Reverse => self.raw.prev()
            }
        }

        if self.raw.valid() {
            // .key() and .value() only ever return None if valid == false, which we've just cheked
            Some((
                self.raw.key().unwrap().into_boxed_slice(),
                self.raw.value().unwrap().into_boxed_slice()
            ))
        } else {
            None
        }
    }
}

impl Into<RawDatabaseIterator> for DatabaseIterator {
    fn into(self) -> RawDatabaseIterator {
        self.raw
    }
}
