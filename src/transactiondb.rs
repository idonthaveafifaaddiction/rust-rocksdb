// Copyright 2018. Starry, Inc. All Rights Reserved.
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

// NOTE no support implemented yet for OptimisticTransactionDb

use libc::{/*self,*/ c_char, /*c_int*,*/ c_uchar, /*c_void,*/ size_t};
// use std::collections::BTreeMap;
use std::ffi::CString;
// use std::fmt;
use std::fs;
// use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
use std::slice;
// use std::str;
// use std::ffi::CStr;

use {Error, Options, WriteOptions, WriteBatch, ReadOptions, DBVector};
use checkpoint::Checkpoint;
use ffi;
// use ffi_util::opt_bytes_to_ptr;

// FIXME move to utils file
pub fn pathref_to_cstring<P>(path: P) -> Result<CString, Error>
    where P: AsRef<Path>
{
    match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
        Ok(cpath) => Ok(cpath),
        Err(err) => {
            Err(Error::new(
                format!("Failed to convert path to CString when opening DB: {:?}", err).into()
            ))
        }
    }
}

// FIXME lots of duplication with normal Snapshot
pub struct TransactionDbSnapshot<'a> {
    txndb: &'a TransactionDb,
    inner: *const ffi::rocksdb_snapshot_t
}

impl<'a> TransactionDbSnapshot<'a> {
    pub fn iterator(&self, mode: IteratorMode) -> TransactionDbIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_raw_snapshot(self.inner);
        TransactionDbIterator::new(self.txndb, &readopts, mode)
    }

    // TODO implement
    // pub fn iterator_cf(
    //     &self,
    //     cf_handle: ColumnFamily,
    //     mode: IteratorMode,
    // ) -> Result<DBIterator, Error> {
    //     let mut readopts = ReadOptions::default();
    //     readopts.set_snapshot(self);
    //     DBIterator::new_cf(self.db, cf_handle, &readopts, mode)
    // }

    pub fn raw_iterator(&self) -> TransactionDbRawIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_raw_snapshot(self.inner);
        TransactionDbRawIterator::new(self.txndb, &readopts)
    }

    // TODO implement
    // pub fn raw_iterator_cf(&self, cf_handle: ColumnFamily) -> Result<DBRawIterator, Error> {
    //     let mut readopts = ReadOptions::default();
    //     readopts.set_snapshot(self);
    //     DBRawIterator::new_cf(self.db, cf_handle, &readopts)
    // }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let mut readopts = ReadOptions::default();
        readopts.set_raw_snapshot(self.inner);
        self.txndb.get_opt(key, &readopts)
    }

    // TODO implement
    // pub fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<DBVector>, Error> {
    //     let mut readopts = ReadOptions::default();
    //     readopts.set_snapshot(self);
    //     self.db.get_cf_opt(cf, key, &readopts)
    // }
}

impl<'a> Drop for TransactionDbSnapshot<'a> {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_transactiondb_release_snapshot(self.txndb.inner, self.inner); }
    }
}

pub struct TransactionOptions {
    inner: *mut ffi::rocksdb_transaction_options_t
}

impl Default for TransactionOptions {
    fn default() -> Self {
        let inner = unsafe { ffi::rocksdb_transaction_options_create() };
        assert!(!inner.is_null());  // FIXME
        Self { inner }
    }
}

impl Drop for TransactionOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_transaction_options_destroy(self.inner) }
    }
}

impl TransactionOptions {
    pub fn set_set_snapshot(&self, set_snapshot: bool) {
        let set_snapshot = c_uchar::from(set_snapshot);
        unsafe { ffi::rocksdb_transaction_options_set_set_snapshot(self.inner, set_snapshot); }
    }

    pub fn set_deadlock_detect(&self, enabled: bool) {
        let enabled = c_uchar::from(enabled);
        unsafe { ffi::rocksdb_transaction_options_set_deadlock_detect(self.inner, enabled); }
    }

    pub fn set_lock_timeout(&self, timeout: i64) {
        unsafe { ffi::rocksdb_transaction_options_set_lock_timeout(self.inner, timeout); }
    }

    pub fn set_expiration(&self, expiration: i64) {
        unsafe { ffi::rocksdb_transaction_options_set_expiration(self.inner, expiration); }
    }

    pub fn set_deadlock_detect_depth(&self, depth: i64) {
        unsafe { ffi::rocksdb_transaction_options_set_expiration(self.inner, depth); }
    }

    pub fn set_max_write_batch_size(&self, size: usize) {
        unsafe { ffi::rocksdb_transaction_options_set_max_write_batch_size(self.inner, size); }
    }
}

pub struct Transaction {
    inner: *mut ffi::rocksdb_transaction_t
}

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_transaction_destroy(self.inner); }
    }
}

impl Transaction {
    pub fn commit(&self) -> Result<(), Error> {
        Ok(unsafe { ffi_try!(ffi::rocksdb_transaction_commit(self.inner,)) })
    }

    pub fn rollback(&self) -> Result<(), Error> {
        Ok(unsafe { ffi_try!(ffi::rocksdb_transaction_rollback(self.inner,)) })
    }

    pub fn set_savepoint(&self) {
        unsafe { ffi::rocksdb_transaction_set_savepoint(self.inner); }
    }

    pub fn rollback_to_savepoint(&self) -> Result<(), Error> {
        Ok(unsafe { ffi_try!(ffi::rocksdb_transaction_rollback_to_savepoint(self.inner,)) })
    }

    // pub fn transaction_get_snapshot() {} // TODO implement

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let readopts = ReadOptions::default();
        self.get_opt(key, &readopts)
    }

    pub fn get_opt(&self, key: &[u8], options: &ReadOptions) -> Result<Option<DBVector>, Error> {
        let mut val_len: size_t = 0;
        let val = unsafe {
            ffi_try!(ffi::rocksdb_transaction_get(
                self.inner,
                options.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                &mut val_len,
            ))
        } as *mut u8;

        if val.is_null() {
            Ok(None)
        } else {
            Ok(Some(unsafe { DBVector::from_c(val, val_len) }))
        }
    }

    // pub fn transaction_get_cf() {}  // TODO implement
    // pub fn transaction_get_for_update() {}  // TODO implement

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transaction_put(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len(),
            ))
        })
    }

    // pub fn transaction_put_cf() {}  // TODO implement

    pub fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transaction_merge(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len(),
            ))
        })
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len(),
            ))
        })
    }

    // pub fn transaction_delete_cf() {}  // TODO implement
    // pub fn transaction_create_iterator() {}  // TODO implement
    // pub fn transaction_create_iterator_cf() {}  // TODO implement
}

pub struct TransactionDbOptions {
    inner: *mut ffi::rocksdb_transactiondb_options_t
}

impl Default for TransactionDbOptions {
    fn default() -> Self {
        let inner = unsafe { ffi::rocksdb_transactiondb_options_create() };
        assert!(!inner.is_null());  // FIXME
        Self { inner }
    }
}

impl Drop for TransactionDbOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_transactiondb_options_destroy(self.inner) }
    }
}

impl TransactionDbOptions {
    pub fn transactiondb_options_set_max_num_locks(&self, num_locks: i64) {
        unsafe { ffi::rocksdb_transactiondb_options_set_max_num_locks(self.inner, num_locks); }
    }

    pub fn transactiondb_options_set_num_stripes(&self, num_stripes: usize) {
        unsafe { ffi::rocksdb_transactiondb_options_set_num_stripes(self.inner, num_stripes); }
    }

    pub fn transactiondb_options_set_transaction_lock_timeout(&self, timeout: i64) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_transaction_lock_timeout(self.inner, timeout);
        }
    }

    pub fn transactiondb_options_set_default_lock_timeout(&self, timeout: i64) {
        unsafe { ffi::rocksdb_transactiondb_options_set_default_lock_timeout(self.inner, timeout); }
    }
}

pub struct TransactionDb {
    inner: *mut ffi::rocksdb_transactiondb_t,
    path: PathBuf
}

impl Drop for TransactionDb {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_transactiondb_close(self.inner); }
    }
}

// FIXME is this okay to assume?
unsafe impl Send for TransactionDb {}
unsafe impl Sync for TransactionDb {}

impl TransactionDb {
    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }

    // pub fn transactiondb_create_column_family() {}  // TODO implement

    /// Open a transaction database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<TransactionDb, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let txndb_opts = TransactionDbOptions::default();
        TransactionDb::open(&opts, &txndb_opts, path)
    }

    /// Open the transaction database with the specified options.
    pub fn open<P: AsRef<Path>>(
        opts: &Options,
        txndb_opts: &TransactionDbOptions,
        path: P
    ) -> Result<TransactionDb, Error> {
        let path = path.as_ref();
        let cpath = pathref_to_cstring(path)?;
        if let Err(err) = fs::create_dir_all(&path) {
            return Err(Error::new(format!("Failed to create RocksDB directory: `{:?}`.", err)));
        }

        let db: *mut ffi::rocksdb_transactiondb_t = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_open(
                opts.inner,
                txndb_opts.inner,
                cpath.as_ptr() as *const _,
            ))
        };

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        Ok(Self {
            inner: db,
            path: path.to_path_buf()
        })
    }

    pub fn snapshot(&self) -> TransactionDbSnapshot {
        let snapshot = unsafe { ffi::rocksdb_transactiondb_create_snapshot(self.inner,) };
        TransactionDbSnapshot { txndb: self, inner: snapshot }
    }

    pub fn begin_transaction(&self, old_txn: Option<Transaction>) -> Transaction {
        let writeopts = WriteOptions::default();
        let txnopts = TransactionOptions::default();
        self.begin_transaction_opt(&writeopts, &txnopts, old_txn)
    }

    pub fn begin_transaction_opt(
        &self,
        write_options: &WriteOptions,
        txn_options: &TransactionOptions,
        old_txn: Option<Transaction>
    ) -> Transaction {
        let txn = unsafe {
            ffi::rocksdb_transaction_begin(
                self.inner,
                write_options.inner,
                txn_options.inner,
                match old_txn {
                    Some(ref old_txn) => old_txn.inner,
                    None => ptr::null_mut()
                }
            )
        };
        assert!(!txn.is_null());  // FIXME
        match old_txn {
            Some(old_txn) => old_txn,
            None => Transaction { inner: txn }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        let readopts = ReadOptions::default();
        self.get_opt(key, &readopts)
    }

    // FIXME copypasta from Transaction::get_opt()
    pub fn get_opt(&self, key: &[u8], options: &ReadOptions) -> Result<Option<DBVector>, Error> {
        let mut val_len: size_t = 0;
        let val = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_get(
                self.inner,
                options.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                &mut val_len,
            ))
        } as *mut u8;

        if val.is_null() {
            Ok(None)
        } else {
            Ok(Some(unsafe { DBVector::from_c(val, val_len) }))
        }
    }

    // pub fn transactiondb_get_cf() {}  // TODO implement

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.put_opt(key, value, &writeopts)
    }

    // FIXME copypasta from Transaction::put_opt()
    pub fn put_opt(&self, key: &[u8], value: &[u8], options: &WriteOptions) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_put(
                self.inner,
                options.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len(),
            ))
        })
    }

    // pub fn transactiondb_put_cf() {}  // TODO implement

    pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.write_opt(batch, &writeopts)
    }

    pub fn write_opt(&self, batch: WriteBatch, write_opts: &WriteOptions) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_write(
                self.inner,
                write_opts.inner,
                batch.inner,
            ))
        })
    }

    pub fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.merge_opt(key, value, &writeopts)
    }

    pub fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        write_opts: &WriteOptions
    ) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_merge(
                self.inner,
                write_opts.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len(),
            ))
        })
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.delete_opt(key, &writeopts)
    }

    // FIXME copypasta from Transaction::delete_opt()
    pub fn delete_opt(&self, key: &[u8], options: &WriteOptions) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_delete(
                self.inner,
                options.inner,
                key.as_ptr() as *const c_char,
                key.len(),
            ))
        })
    }

    // pub fn transactiondb_delete_cf() {}  // TODO implement

    pub fn iterator(&self, mode: IteratorMode) -> TransactionDbIterator {
        let opts = ReadOptions::default();
        TransactionDbIterator::new(self, &opts, mode)
    }

    pub fn full_iterator(&self, mode: IteratorMode) -> TransactionDbIterator {
        let mut opts = ReadOptions::default();
        opts.set_total_order_seek(true);
        TransactionDbIterator::new(self, &opts, mode)
    }

    pub fn prefix_iterator<'a>(&self, prefix: &'a [u8]) -> TransactionDbIterator {
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        TransactionDbIterator::new(self, &opts, IteratorMode::From(prefix, Direction::Forward))
    }

    // TODO implement
    // pub fn iterator_cf(
    //     &self,
    //     cf_handle: ColumnFamily,
    //     mode: IteratorMode,
    // ) -> Result<TransactionDbIterator, Error> {
    //     let opts = ReadOptions::default();
    //     TransactionDbIterator::new_cf(self, cf_handle, &opts, mode)
    // }

    // TODO implement
    // pub fn full_iterator_cf(
    //     &self,
    //     cf_handle: ColumnFamily,
    //     mode: IteratorMode,
    // ) -> Result<TransactionDbIterator, Error> {
    //     let mut opts = ReadOptions::default();
    //     opts.set_total_order_seek(true);
    //     TransactionDbIterator::new_cf(self, cf_handle, &opts, mode)
    // }

    // TODO implement
    // pub fn prefix_iterator_cf<'a>(
    //     &self,
    //     cf_handle: ColumnFamily,
    //     prefix: &'a [u8]
    // ) -> Result<TransactionDbIterator, Error> {
    //     let mut opts = ReadOptions::default();
    //     opts.set_prefix_same_as_start(true);
    //     TransactionDbIterator::new_cf(
    //         self,
    //         cf_handle,
    //         &opts,
    //         IteratorMode::From(prefix, Direction::Forward)
    //     )
    // }

    pub fn raw_iterator(&self) -> TransactionDbRawIterator {
        let opts = ReadOptions::default();
        TransactionDbRawIterator::new(self, &opts)
    }

    // TODO implement
    // pub fn raw_iterator_cf(
    //     &self,
    //     cf_handle: ColumnFamily
    // ) -> Result<TransactionDbRawIterator, Error> {
    //     let opts = ReadOptions::default();
    //     TransactionDbRawIterator::new_cf(self, cf_handle, &opts)
    // }

    pub fn create_checkpoint(&self) -> Result<Checkpoint, Error> {
        let checkpoint = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_checkpoint_object_create(self.inner,))
        };
        if checkpoint.is_null() {
            Err(Error::new("Could not create checkpoint object.".into()))
        } else {
            Ok(Checkpoint { inner: checkpoint })
        }
    }

    // FIXME copypasta from DB::destroy()
    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = pathref_to_cstring(path)?;
        unsafe { ffi_try!(ffi::rocksdb_destroy_db(opts.inner, cpath.as_ptr(),)); }
        Ok(())
    }
}

// FIXME all the TransactionDb iterator stuff was mostly copypasta

pub struct TransactionDbRawIterator {
    inner: *mut ffi::rocksdb_iterator_t
}

pub struct TransactionDbIterator {
    raw: TransactionDbRawIterator,
    direction: Direction,
    just_seeked: bool
}

unsafe impl Send for TransactionDbIterator {}

pub enum Direction {
    Forward,
    Reverse
}

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

pub enum IteratorMode<'a> {
    Start,
    End,
    From(&'a [u8], Direction)
}

impl TransactionDbRawIterator {
    fn new(txndb: &TransactionDb, readopts: &ReadOptions) -> TransactionDbRawIterator {
        unsafe {
            TransactionDbRawIterator {
                inner: ffi::rocksdb_transactiondb_create_iterator(txndb.inner, readopts.inner)
            }
        }
    }

    // TODO implement
    // fn new_cf(
    //     txndb: &TransactionDb,
    //     cf_handle: ColumnFamily,
    //     readopts: &ReadOptions,
    // ) -> Result<TransactionDbRawIterator, Error> {
    //     unsafe {
    //         Ok(TransactionDbRawIterator {
    //             inner: ffi::rocksdb_create_iterator_cf(db.inner, readopts.inner, cf_handle.inner),
    //         })
    //     }
    // }

    pub fn valid(&self) -> bool {
        unsafe { ffi::rocksdb_iter_valid(self.inner) != 0 }
    }

    pub fn seek_to_first(&mut self) {
        unsafe {
            ffi::rocksdb_iter_seek_to_first(self.inner);
        }
    }

    pub fn seek_to_last(&mut self) {
        unsafe {
            ffi::rocksdb_iter_seek_to_last(self.inner);
        }
    }

    pub fn seek(&mut self, key: &[u8]) {
        unsafe {
            ffi::rocksdb_iter_seek(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    pub fn seek_for_prev(&mut self, key: &[u8]) {
        unsafe {
            ffi::rocksdb_iter_seek_for_prev(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    pub fn next(&mut self) {
        unsafe {
            ffi::rocksdb_iter_next(self.inner);
        }
    }

    pub fn prev(&mut self) {
        unsafe {
            ffi::rocksdb_iter_prev(self.inner);
        }
    }

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

    pub fn key(&self) -> Option<Vec<u8>> {
        unsafe { self.key_inner().map(|key| key.to_vec()) }
    }

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

    pub fn value(&self) -> Option<Vec<u8>> {
        unsafe { self.value_inner().map(|value| value.to_vec()) }
    }
}

impl Drop for TransactionDbRawIterator {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_iter_destroy(self.inner);
        }
    }
}

impl TransactionDbIterator {
    fn new(
        txndb: &TransactionDb,
        readopts: &ReadOptions,
        mode: IteratorMode
    ) -> TransactionDbIterator {
        let mut rv = TransactionDbIterator {
            raw: TransactionDbRawIterator::new(txndb, readopts),
            direction: Direction::Forward, // blown away by set_mode()
            just_seeked: false,
        };
        rv.set_mode(mode);
        rv
    }

    // TODO implement
    // fn new_cf(
    //     txndb: &TransactionDb,
    //     cf_handle: ColumnFamily,
    //     readopts: &ReadOptions,
    //     mode: IteratorMode,
    // ) -> Result<TransactionDbIterator, Error> {
    //     let mut rv = TransactionDbIterator {
    //         raw: try!(TransactionDbRawIterator::new_cf(db, cf_handle, readopts)),
    //         direction: Direction::Forward, // blown away by set_mode()
    //         just_seeked: false,
    //     };
    //     rv.set_mode(mode);
    //     Ok(rv)
    // }

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

impl Iterator for TransactionDbIterator {
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

impl Into<TransactionDbRawIterator> for TransactionDbIterator {
    fn into(self) -> TransactionDbRawIterator {
        self.raw
    }
}

// FIXME add tests for all functions
#[cfg(test)]
mod test {

use super::*;

#[test]
fn transactiondb_put_get_delete() {
    let path = "_rust_rocksdb_transactiondb_test";
    {
        let db = TransactionDb::open_default(path).unwrap();
        assert!(db.put(b"k1", b"v1111").is_ok());
        assert!(db.get(b"k1").unwrap().unwrap().to_utf8().unwrap() == "v1111");
        assert!(db.delete(b"k1").is_ok());
        assert!(db.get(b"k1").unwrap().is_none());
    }
    let opts = Options::default();
    let result = TransactionDb::destroy(&opts, path);
    assert!(result.is_ok());
}

#[test]
fn transactiondb_transaction_put_get_delete() {
    let path = "_rust_rocksdb_transactiondb_test";
    {
        let db = TransactionDb::open_default(path).unwrap();

        let txn = db.begin_transaction(None);
        assert!(txn.put(b"k1", b"v1111").is_ok());
        assert!(txn.get(b"k1").unwrap().unwrap().to_utf8().unwrap() == "v1111");
        assert!(txn.delete(b"k1").is_ok());
        assert!(txn.get(b"k1").unwrap().is_none());
        assert!(txn.put(b"k2", b"v2222").is_ok());
        assert!(txn.commit().is_ok());

        assert!(db.get(b"k2").unwrap().unwrap().to_utf8().unwrap() == "v2222");
        assert!(db.get(b"k1").unwrap().is_none());
    }
    let opts = Options::default();
    let result = TransactionDb::destroy(&opts, path);
    assert!(result.is_ok());
}

#[test]
fn transactiondb_transaction_conflict() {
    let path = "_rust_rocksdb_transactiondb_test";
    {
        let db = TransactionDb::open_default(path).unwrap();
        assert!(db.put(b"k1", b"v1111").is_ok());
        assert!(db.put(b"k2", b"v2222").is_ok());

        let txn1 = db.begin_transaction(None);
        let txn2 = db.begin_transaction(None);

        assert!(txn1.delete(b"k2").is_ok());
        assert!(txn2.delete(b"k2").is_err());

        assert!(txn2.put(b"k3", b"v3333").is_ok());
        assert!(txn1.put(b"k3", b"conflict").is_err());

        assert!(txn1.put(b"k1", b"v1111-updated").is_ok());

        assert!(txn2.commit().is_ok());
        assert!(txn1.commit().is_ok());

        assert!(db.get(b"k1").unwrap().unwrap().to_utf8().unwrap() == "v1111-updated");
        assert!(db.get(b"k2").unwrap().is_none());
        assert!(db.get(b"k3").unwrap().unwrap().to_utf8().unwrap() == "v3333");
    }
    let opts = Options::default();
    let result = TransactionDb::destroy(&opts, path);
    assert!(result.is_ok());
}

}
