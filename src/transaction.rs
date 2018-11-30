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

use libc::{c_char, c_uchar, size_t};

use {Error, ReadOptions, DBVector};
use ffi;

////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransactionOptions {
    pub(crate) inner: *mut ffi::rocksdb_transaction_options_t
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

////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OptimisticTransactionOptions {
    pub(crate) inner: *mut ffi::rocksdb_optimistictransaction_options_t
}

impl Default for OptimisticTransactionOptions {
    fn default() -> Self {
        let inner = unsafe { ffi::rocksdb_optimistictransaction_options_create() };
        assert!(!inner.is_null());  // FIXME
        Self { inner }
    }
}

impl Drop for OptimisticTransactionOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_optimistictransaction_options_destroy(self.inner) }
    }
}

impl OptimisticTransactionOptions {
    pub fn set_set_snapshot(&self, set_snapshot: bool) {
        let set_snapshot = c_uchar::from(set_snapshot);
        unsafe {
            ffi::rocksdb_optimistictransaction_options_set_set_snapshot(self.inner, set_snapshot);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Transaction {
    pub(crate) inner: *mut ffi::rocksdb_transaction_t
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
