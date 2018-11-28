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

use libc::{self, c_char, c_int, c_uchar, c_void, size_t};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fmt;
use std::fs;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
use std::slice;
use std::str;
use std::ffi::CStr;

use {Error, Options, WriteOptions, ReadOptions, DBVector};
use ffi;
use ffi_util::opt_bytes_to_ptr;

pub struct TransactionOptions {
    inner: *mut ffi::rocksdb_transaction_options_t
}

impl Default for TransactionOptions {
    fn default() -> Self {
        unsafe {
            Self {
                inner: ffi::rocksdb_transaction_options_create()
            }
        }
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
        unsafe {
            ffi::rocksdb_transaction_options_set_set_snapshot(self.inner, set_snapshot);
        }
    }

    pub fn set_deadlock_detect(&self, enabled: bool) {
        let enabled = c_uchar::from(enabled);
        unsafe {
            ffi::rocksdb_transaction_options_set_deadlock_detect(self.inner, enabled);
        }
    }

    pub fn set_lock_timeout(&self, timeout: i64) {
        unsafe {
            ffi::rocksdb_transaction_options_set_lock_timeout(self.inner, timeout);
        }
    }

    pub fn set_expiration(&self, expiration: i64) {
        unsafe {
            ffi::rocksdb_transaction_options_set_expiration(self.inner, expiration);
        }
    }

    pub fn set_deadlock_detect_depth(&self, depth: i64) {
        unsafe {
            ffi::rocksdb_transaction_options_set_expiration(self.inner, depth);
        }
    }

    pub fn set_max_write_batch_size(&self, size: usize) {
        unsafe {
            ffi::rocksdb_transaction_options_set_max_write_batch_size(self.inner, size);
        }
    }
}

pub struct Transaction {
    inner: *mut ffi::rocksdb_transaction_t
}

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_destroy(self.inner);
        }
    }
}

impl Transaction {
    pub fn commit(&self) -> Result<(), Error> {
        Ok(unsafe { ffi_try!(ffi::rocksdb_transaction_commit(self.inner,)) })
    }

    // pub fn transaction_rollback() {}  // TODO implement
    // pub fn transaction_set_savepoint() {}  // TODO implement
    // pub fn transaction_rollback_to_savepoint() {}  // TODO implement
    // pub fn transaction_get_snapshot() {}  // TODO implement

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.get_opt(key, Default::default())
    }

    pub fn get_opt(&self, key: &[u8], options: ReadOptions) -> Result<Option<DBVector>, Error> {
        let mut val_len: size_t = 0;
        let val = unsafe {
            ffi_try!(ffi::rocksdb_transaction_get(
                self.inner,
                options.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
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
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ))
        })
    }

    // pub fn transaction_put_cf() {}  // TODO implement
    // pub fn transaction_merge() {}  // TODO implement

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
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
        Self { inner: unsafe { ffi::rocksdb_transactiondb_options_create() } }
    }
}

impl Drop for TransactionDbOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_transactiondb_options_destroy(self.inner) }
    }
}

impl TransactionDbOptions {
    pub fn transactiondb_options_set_max_num_locks(&self, num_locks: i64) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_max_num_locks(self.inner, num_locks);
        }
    }

    pub fn transactiondb_options_set_num_stripes(&self, num_stripes: usize) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_num_stripes(self.inner, num_stripes);
        }
    }

    pub fn transactiondb_options_set_transaction_lock_timeout(&self, timeout: i64) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_transaction_lock_timeout(self.inner, timeout);
        }
    }

    pub fn transactiondb_options_set_default_lock_timeout(&self, timeout: i64) {
        unsafe {
            ffi::rocksdb_transactiondb_options_set_default_lock_timeout(self.inner, timeout);
        }
    }
}

pub struct TransactionDb {
    inner: *mut ffi::rocksdb_transactiondb_t,
    path: PathBuf
}

impl Drop for TransactionDb {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_close(self.inner);
        }
    }
}

// FIXME is this okay to assume?
unsafe impl Send for TransactionDb {}
unsafe impl Sync for TransactionDb {}

impl TransactionDb {
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
        let cpath = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(cpath) => cpath,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString when opening DB.".to_owned()
                ))
            }
        };

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

    // pub fn transactiondb_create_snapshot() {}  // TODO implement
    // pub fn transactiondb_release_snapshot() {}  // TODO implement

    pub fn begin_transaction(&self, old_txn: Option<Transaction>) -> Transaction {
        self.begin_transaction_opt(Default::default(), Default::default(), old_txn)
    }

    pub fn begin_transaction_opt(
        &self,
        write_options: WriteOptions,
        txn_options: TransactionOptions,
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
        match old_txn {
            Some(old_txn) => old_txn,
            None => Transaction { inner: txn }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.get_opt(key, Default::default())
    }

    // FIXME copypasta from Transaction::get_opt()
    pub fn get_opt(&self, key: &[u8], options: ReadOptions) -> Result<Option<DBVector>, Error> {
        let mut val_len: size_t = 0;
        let val = unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_get(
                self.inner,
                options.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
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
        self.put_opt(key, value, Default::default())
    }

    // FIXME copypasta from Transaction::put_opt()
    pub fn put_opt(&self, key: &[u8], value: &[u8], options: WriteOptions) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_put(
                self.inner,
                options.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ))
        })
    }

    // pub fn transactiondb_put_cf() {}  // TODO implement
    // pub fn transactiondb_write() {}  // TODO implement
    // pub fn transactiondb_merge() {}  // TODO implement


    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.delete_opt(key, Default::default())
    }

    // FIXME copypasta from Transaction::delete_opt()
    pub fn delete_opt(&self, key: &[u8], options: WriteOptions) -> Result<(), Error> {
        Ok(unsafe {
            ffi_try!(ffi::rocksdb_transactiondb_delete(
                self.inner,
                options.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ))
        })
    }


    // pub fn transactiondb_delete_cf() {}  // TODO implement
    // pub fn transactiondb_create_iterator() {}  // TODO implement
    // pub fn transactiondb_checkpoint_object_create() {}  // TODO implement

    // FIXME copypasta from DB::destroy()
    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = CString::new(path.as_ref().to_string_lossy().as_bytes()).unwrap();
        unsafe {
            ffi_try!(ffi::rocksdb_destroy_db(opts.inner, cpath.as_ptr(),));
        }
        Ok(())
    }
}

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
