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

use libc::{c_char, c_int};
use std::any::Any;
use std::ffi::CString;
use std::fmt;
use std::path::Path;
use std::ptr;
use std::str;

use {
    BaseDb,
    Error,
    Options,
    WriteOptions,
    WriteBatch,
    ReadOptions,
    DBVector,
    ColumnFamily,
    ColumnFamilyDescriptor,
    DBIterator,
    DBRawIterator,
    IteratorMode,
    Snapshot
};
use ffi;
use transaction::{Transaction, OptimisticTransactionOptions};
use base_db::BaseDbImpl;

pub struct OptTxnDbBaseDbImpl {
    inner: *mut ffi::rocksdb_optimistictransactiondb_t,
    initialized: bool
}

impl Drop for OptTxnDbBaseDbImpl {
    fn drop(&mut self) {
        if self.initialized {
            unsafe { ffi::rocksdb_optimistictransactiondb_close(self.inner); }
        }
    }
}

impl Default for OptTxnDbBaseDbImpl {
    fn default() -> Self {
        Self {
            inner: ptr::null_mut(),
            initialized: false
        }
    }
}

impl BaseDbImpl for OptTxnDbBaseDbImpl {
    fn open_raw(&mut self, opts: &Options, cpath: &CString) -> Result<(), Error> {
        if self.initialized {
            return Err(Error::new("Database is already open".into()));
        }
        let db = unsafe {
            ffi_try!(ffi::rocksdb_optimistictransactiondb_open(
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
            ffi_try!(ffi::rocksdb_optimistictransactiondb_open_column_families(
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
        Ok(unsafe {
            ffi::rocksdb_optimistictransactiondb_get_base_db(self.inner)
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct OptimisticTransactionDB {
    inner: BaseDb
}

impl fmt::Debug for OptimisticTransactionDB {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OptimisticTransactionDB {{ path: {:?} }}", self.path())
    }
}

// FIXME is this okay to assume?
unsafe impl Send for OptimisticTransactionDB {}
unsafe impl Sync for OptimisticTransactionDB {}

impl OptimisticTransactionDB {
    pub fn begin_transaction(&self, old_txn: Option<Transaction>) -> Transaction {
        let writeopts = WriteOptions::default();
        let txnopts = OptimisticTransactionOptions::default();
        self.begin_transaction_opt(&writeopts, &txnopts, old_txn)
    }

    pub fn begin_transaction_opt(
        &self,
        writeopts: &WriteOptions,
        txnopts: &OptimisticTransactionOptions,
        old_txn: Option<Transaction>
    ) -> Transaction {
        // FIXME this is pretty gross but what else can we do?
        let opttxndb = self
            .inner
            .inner()
            .as_any()
            .downcast_ref::<OptTxnDbBaseDbImpl>()
            .expect("Contained BaseDbImpl is of the wrong type")
            .inner;

        let txn = unsafe {
            ffi::rocksdb_optimistictransaction_begin(
                opttxndb,
                writeopts.inner,
                txnopts.inner,
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

    // FIXME don't need this right?
    // extern "C" {
    //     pub fn rocksdb_optimistictransactiondb_close_base_db(base_db: *mut rocksdb_t);
    // }

    // FIXME write macro that impls the functions from here down with the appropriate BaseDbImpl

    /// Open a database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Ok(Self {
            inner: BaseDb::open_default::<P, OptTxnDbBaseDbImpl>(path)?
        })
    }

    /// Open the database with the specified options.
    pub fn open<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Self, Error> {
        Ok(Self {
            inner: BaseDb::open::<P, OptTxnDbBaseDbImpl>(opts, path)?
        })
    }

    /// Open a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P: AsRef<Path>>(opts: &Options, path: P, cfs: &[&str]) -> Result<Self, Error> {
        Ok(Self {
            inner: BaseDb::open_cf::<P, OptTxnDbBaseDbImpl>(opts, path, cfs)?
        })
    }

    /// Open a database with the given database options and column family names/options.
    pub fn open_cf_descriptors<P: AsRef<Path>>(
        opts: &Options,
        path: P,
        cfs: Vec<ColumnFamilyDescriptor>
    ) -> Result<Self, Error> {
        Ok(Self {
            inner: BaseDb::open_cf_descriptors::<P, OptTxnDbBaseDbImpl>(opts, path, cfs)?
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
