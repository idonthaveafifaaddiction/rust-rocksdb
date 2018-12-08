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

use libc::{c_char, c_uchar};

use ffi;
use refactor::common::{
    ColumnFamily,
    DatabaseVector,
    RawDatabaseIterator,
    ReadOptions,
    Snapshot
};
use refactor::database::InnerDbType;
use refactor::errors::Error;
use refactor::traits::{
    ColumnFamilyIteration,
    DatabaseIteration,
    DatabaseReadNoOptOperations,
    DatabaseReadOptOperations,
    DatabaseSnapshotting,
    DatabaseWriteNoOptOperations
};
use refactor::utils::c_buf_to_opt_dbvec;

////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransactionOptions {
    pub(crate) inner: *mut ffi::rocksdb_transaction_options_t
}

// FIXME is this okay to assume?
unsafe impl Send for TransactionOptions {}

impl Default for TransactionOptions {
    fn default() -> Self {
        let inner = unsafe {
            ffi::rocksdb_transaction_options_create()
        };
        if inner.is_null() {
            panic!("Coud not create RocksDB transaction options")
        }
        Self { inner }
    }
}

impl Drop for TransactionOptions {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_transaction_options_destroy(self.inner) }
    }
}

impl TransactionOptions {
    pub fn new() -> Self {
        Self::default()
    }

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

////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OptimisticTransactionOptions {
    pub(crate) inner: *mut ffi::rocksdb_optimistictransaction_options_t
}

// FIXME is this okay to assume?
unsafe impl Send for OptimisticTransactionOptions {}

impl Default for OptimisticTransactionOptions {
    fn default() -> Self {
        let inner = unsafe { ffi::rocksdb_optimistictransaction_options_create() };
        if inner.is_null() {
            panic!("Cannot create RocksDB optimistic transaction options")
        }
        Self { inner }
    }
}

impl Drop for OptimisticTransactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_optimistictransaction_options_destroy(self.inner)
        }
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
    pub(crate) inner: *mut ffi::rocksdb_transaction_t,
    pub(crate) db: InnerDbType
}

// FIXME is this okay to assume?
unsafe impl Send for Transaction {}

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_destroy(self.inner);
        }
    }
}

// FIXME sort out all the extra things in this impl
impl Transaction {
    pub fn commit(&self) -> Result<(), Error> {
        Ok(
            unsafe {
                try_ffi!(ffi::rocksdb_transaction_commit(self.inner))
            }
        )
    }

    pub fn rollback(&self) -> Result<(), Error> {
        Ok(
            unsafe {
                try_ffi!(ffi::rocksdb_transaction_rollback(self.inner))
            }
        )
    }

    pub fn set_savepoint(&self) {
        unsafe {
            ffi::rocksdb_transaction_set_savepoint(self.inner);
        }
    }

    pub fn rollback_to_savepoint(&self) -> Result<(), Error> {
        Ok(
            unsafe {
                try_ffi!(ffi::rocksdb_transaction_rollback_to_savepoint(self.inner))
            }
        )
    }

    pub fn get_for_update(&self, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        let readopts = ReadOptions::default();
        self.get_for_update_opt(key, &readopts)
    }

    pub fn get_for_update_opt(
        &self,
        key: &[u8],
        readopts: &ReadOptions
    ) -> Result<Option<DatabaseVector>, Error> {
        let mut val_len = 0;
        let val = unsafe {
            try_ffi!(ffi::rocksdb_transaction_get_for_update(
                self.inner,
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                &mut val_len,
                true as c_uchar
            ))
        } as *mut u8;
        Ok(c_buf_to_opt_dbvec(val, val_len))
    }
}

impl DatabaseReadOptOperations for Transaction {
    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DatabaseVector>, Error> {
        let mut val_len = 0;
        let val = unsafe {
            try_ffi!(ffi::rocksdb_transaction_get(
                self.inner,
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                &mut val_len
            ))
        } as *mut u8;
        Ok(c_buf_to_opt_dbvec(val, val_len))
    }

    fn get_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        readopts: &ReadOptions
    ) -> Result<Option<DatabaseVector>, Error> {
        let mut val_len = 0;
        let val = unsafe {
            try_ffi!(ffi::rocksdb_transaction_get_cf(
                self.inner,
                readopts.inner,
                cf_handle.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                &mut val_len
            ))
        } as *mut u8;
        Ok(c_buf_to_opt_dbvec(val, val_len))
    }
}

impl DatabaseReadNoOptOperations for Transaction {
    fn get(&self, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        let readopts = ReadOptions::default();
        self.get_opt(&key, &readopts)
    }

    fn get_cf(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8]
    ) -> Result<Option<DatabaseVector>, Error> {
        let readopts = ReadOptions::default();
        self.get_cf_opt(cf_handle, &key, &readopts)
    }
}

impl DatabaseWriteNoOptOperations for Transaction {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        Ok(
            unsafe {
                try_ffi!(ffi::rocksdb_transaction_put(
                    self.inner,
                    key.as_ptr() as *const c_char,
                    key.len(),
                    value.as_ptr() as *const c_char,
                    value.len()
                ))
            }
        )
    }

    fn put_cf(&self, cf_handle: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_transaction_put_cf(
                self.inner,
                cf_handle.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len()
            ));
        }
        Ok(())
    }

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        Ok(
            unsafe {
                try_ffi!(ffi::rocksdb_transaction_merge(
                    self.inner,
                    key.as_ptr() as *const c_char,
                    key.len(),
                    value.as_ptr() as *const c_char,
                    value.len()
                ))
            }
        )
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        Ok(
            unsafe {
                try_ffi!(ffi::rocksdb_transaction_delete(
                    self.inner,
                    key.as_ptr() as *const c_char,
                    key.len()
                ))
            }
        )
    }

    fn delete_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_transaction_delete_cf(
                self.inner,
                cf_handle.inner,
                key.as_ptr() as *const c_char,
                key.len()
            ));
        }
        Ok(())
    }
}

impl DatabaseIteration for Transaction {
    fn iter_raw_opt(&self, readopts: &ReadOptions) -> RawDatabaseIterator {
        let iter = unsafe {
            ffi::rocksdb_transaction_create_iterator(self.inner, readopts.inner)
        };
        RawDatabaseIterator::from_raw(
            iter,
            match self.db {
                InnerDbType::DB(ref db) => InnerDbType::DB(db.clone()),
                InnerDbType::TxnDB(ref db) => InnerDbType::TxnDB(db.clone())
            }
        )
    }
}

impl ColumnFamilyIteration for Transaction {
    fn iter_cf_raw_opt(
        &self,
        cf_handle: ColumnFamily,
        readopts: &ReadOptions
    ) -> RawDatabaseIterator {
        let iter = unsafe {
            ffi::rocksdb_transaction_create_iterator_cf(
                self.inner,
                readopts.inner,
                cf_handle.inner
            )
        };
        RawDatabaseIterator::from_raw(
            iter,
            match self.db {
                InnerDbType::DB(ref db) => InnerDbType::DB(db.clone()),
                InnerDbType::TxnDB(ref db) => InnerDbType::TxnDB(db.clone())
            }
        )
    }
}

impl DatabaseSnapshotting for Transaction {
    fn snapshot(&self) -> Snapshot {
        Snapshot::from_txn(&self)
    }
}
