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

use refactor::{
    backup::BackupEngine,
    errors::Error,
    checkpoint::Checkpoint,
    common::{
        ColumnFamily,
        DatabaseIterator,
        DatabaseIteratorDirection,
        DatabaseIteratorMode,
        DatabaseVector,
        RawDatabaseIterator,
        ReadOptions,
        Snapshot,
        WriteOptions
    },
    transaction::Transaction
};

use std::ffi::CStr;

///////////////////////////////////////////////////////////////////////////////////////////////////

// pub trait DatabaseMetaOperations {
    // FIXME implement, maybe these should go in DatabaseBatchOperations trait
    // fn write(&self, batch: WriteBatch) -> Result<(), Error>;
    // fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<(), Error>;
    // fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>);
    // fn compact_range_cf(&self, cf: ColumnFamily, start: Option<&[u8]>, end: Option<&[u8]>);
    // fn drop_cf(&mut self, name: &str) -> Result<(), Error>;
    // fn create_cf(&mut self, name: &str, opts: &Options) -> Result<ColumnFamily, Error>;
// }

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseOperations: DatabaseReadOperations + DatabaseWriteOperations {}

// FIXME I don't understand these blanket impls...
impl<T> DatabaseOperations for T where T: DatabaseReadOperations + DatabaseWriteOperations {}
// impl<'a, T> DatabaseOperations for &'a T
//     where T: DatabaseReadOperations + DatabaseWriteOperations {}
// impl<'a, T> DatabaseOperations for &'a mut T
//     where T: DatabaseReadOperations + DatabaseWriteOperations {}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseReadOperations: DatabaseReadNoOptOperations + DatabaseReadOptOperations {}

// FIXME I don't understand these blanket impls...
impl<T> DatabaseReadOperations for T
    where T: DatabaseReadNoOptOperations + DatabaseReadOptOperations {}
// impl<'a, T> DatabaseReadOperations for &'a T
//     where T: DatabaseReadNoOptOperations + DatabaseReadOptOperations {}
// impl<'a, T> DatabaseReadOperations for &'a mut T
//     where T: DatabaseReadNoOptOperations + DatabaseReadOptOperations {}

pub trait DatabaseReadNoOptOperations {
    fn get(&self, key: &[u8]) -> Result<Option<DatabaseVector>, Error>;

    fn get_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<Option<DatabaseVector>, Error>;
}

impl<'a, T> DatabaseReadNoOptOperations for &'a T where T: DatabaseReadNoOptOperations {
    fn get(&self, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        (**self).get(&key)
    }

    fn get_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        (**self).get_cf(cf_handle, &key)
    }
}

impl<'a, T> DatabaseReadNoOptOperations for &'a mut T where T: DatabaseReadNoOptOperations {
    fn get(&self, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        (**self).get(&key)
    }

    fn get_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        (**self).get_cf(cf_handle, &key)
    }
}

pub trait DatabaseReadOptOperations {
    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DatabaseVector>, Error>;

    fn get_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<DatabaseVector>, Error>;
}

impl<'a, T> DatabaseReadOptOperations for &'a T where T: DatabaseReadOptOperations {
    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DatabaseVector>, Error> {
        (**self).get_opt(&key, &readopts)
    }

    fn get_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<DatabaseVector>, Error> {
        (**self).get_cf_opt(cf_handle, &key, &readopts)
    }
}

impl<'a, T> DatabaseReadOptOperations for &'a mut T where T: DatabaseReadOptOperations {
    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DatabaseVector>, Error> {
        (**self).get_opt(&key, &readopts)
    }

    fn get_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<DatabaseVector>, Error> {
        (**self).get_cf_opt(cf_handle, &key, &readopts)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseWriteOperations: DatabaseWriteNoOptOperations + DatabaseWriteOptOperations {}

// FIXME I don't understand these blanket impls...
impl<T> DatabaseWriteOperations for T
    where T: DatabaseWriteNoOptOperations + DatabaseWriteOptOperations {}
// impl<'a, T> DatabaseWriteOperations for &'a T
//     where T: DatabaseWriteNoOptOperations + DatabaseWriteOptOperations {}
// impl<'a, T> DatabaseWriteOperations for &'a mut T
//     where T: DatabaseWriteNoOptOperations + DatabaseWriteOptOperations {}

pub trait DatabaseWriteNoOptOperations {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;

    fn put_cf(&self, cf_handle: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error>;

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;

    fn delete(&self, key: &[u8]) -> Result<(), Error>;

    fn delete_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<(), Error>;
}

impl<'a, T> DatabaseWriteNoOptOperations for &'a T where T: DatabaseWriteNoOptOperations {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        (**self).put(&key, &value)
    }

    fn put_cf(&self, cf_handle: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        (**self).put_cf(cf_handle, &key, &value)
    }

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        (**self).merge(&key, &value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        (**self).delete(&key)
    }

    fn delete_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        (**self).delete_cf(cf_handle, &key)
    }
}

impl<'a, T> DatabaseWriteNoOptOperations for &'a mut T where T: DatabaseWriteNoOptOperations {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        (**self).put(&key, &value)
    }

    fn put_cf(&self, cf_handle: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        (**self).put_cf(cf_handle, &key, &value)
    }

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        (**self).merge(&key, &value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        (**self).delete(&key)
    }

    fn delete_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        (**self).delete_cf(cf_handle, &key)
    }
}

pub trait DatabaseWriteOptOperations {
    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<(), Error>;

    fn put_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error>;

    fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error>;

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), Error>;

    fn delete_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error>;
}

impl<'a, T> DatabaseWriteOptOperations for &'a T where T: DatabaseWriteOptOperations {
    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        (**self).put_opt(&key, &value, &writeopts)
    }

    fn put_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        (**self).put_cf_opt(cf_handle, &key, &value, &writeopts)
    }

    fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        (**self).merge_opt(&key, &value, &writeopts)
    }

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        (**self).delete_opt(&key, &writeopts)
    }

    fn delete_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        (**self).delete_cf_opt(cf_handle, &key, &writeopts)
    }
}

impl<'a, T> DatabaseWriteOptOperations for &'a mut T where T: DatabaseWriteOptOperations {
    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        (**self).put_opt(&key, &value, &writeopts)
    }

    fn put_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        (**self).put_cf_opt(cf_handle, &key, &value, &writeopts)
    }

    fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        (**self).merge_opt(&key, &value, &writeopts)
    }

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        (**self).delete_opt(&key, &writeopts)
    }

    fn delete_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        (**self).delete_cf_opt(cf_handle, &key, &writeopts)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// FIXME this trait is awkdward...
pub trait ColumnFamilyMergeOperations {
    fn merge_cf(&self, cf_handle: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.merge_cf_opt(cf_handle, key, value, &writeopts)
    }

    fn merge_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error>;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseIteration {
    fn iter_raw(&self) -> RawDatabaseIterator {
        let readopts = ReadOptions::default();
        self.iter_raw_opt(&readopts)
    }

    fn iter_raw_opt(&self, readopts: &ReadOptions) -> RawDatabaseIterator;

    fn iter(&self, mode: DatabaseIteratorMode) -> DatabaseIterator {
        DatabaseIterator::from_raw(self.iter_raw(), mode)
    }

    /// Opens an interator with `set_total_order_seek` enabled.
    /// This must be used to iterate across prefixes when `set_memtable_factory` has been called
    /// with a Hash-based implementation.
    fn iter_full(&self, mode: DatabaseIteratorMode) -> DatabaseIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_total_order_seek(true);
        DatabaseIterator::from_raw(self.iter_raw_opt(&readopts), mode)
    }

    fn iter_prefix<'p>(&self, prefix: &'p [u8]) -> DatabaseIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_prefix_same_as_start(true);
        DatabaseIterator::from_raw(
            self.iter_raw_opt(&readopts),
            DatabaseIteratorMode::From(prefix, DatabaseIteratorDirection::Forward)
        )
    }

    fn iter_prefix_opt<'p>(
        &self,
        prefix: &'p [u8],
        readopts: &mut ReadOptions // FIXME kinda gross that this is mut
    ) -> DatabaseIterator {
        readopts.set_prefix_same_as_start(true);
        DatabaseIterator::from_raw(
            self.iter_raw_opt(&readopts),
            DatabaseIteratorMode::From(prefix, DatabaseIteratorDirection::Forward)
        )
    }
}

impl<'a, T> DatabaseIteration for &'a T where T: DatabaseIteration {
    fn iter_raw_opt(&self, readopts: &ReadOptions) -> RawDatabaseIterator {
        (**self).iter_raw_opt(&readopts)
    }
}

impl<'a, T> DatabaseIteration for &'a mut T where T: DatabaseIteration {
    fn iter_raw_opt(&self, readopts: &ReadOptions) -> RawDatabaseIterator {
        (**self).iter_raw_opt(&readopts)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ColumnFamilyIteration {
    fn iter_cf_raw(&self, cf_handle: ColumnFamily) -> RawDatabaseIterator {
        let readopts = ReadOptions::default();
        self.iter_cf_raw_opt(cf_handle, &readopts)
    }

    fn iter_cf_raw_opt(
        &self,
        cf_handle: ColumnFamily,
        readopts: &ReadOptions
    ) -> RawDatabaseIterator;

    fn iter_cf(&self, cf_handle: ColumnFamily, mode: DatabaseIteratorMode) -> DatabaseIterator {
        DatabaseIterator::from_raw(self.iter_cf_raw(cf_handle), mode)
    }

    fn iter_cf_full(
        &self,
        cf_handle: ColumnFamily,
        mode: DatabaseIteratorMode
    ) -> DatabaseIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_total_order_seek(true);
        DatabaseIterator::from_raw(self.iter_cf_raw_opt(cf_handle, &readopts), mode)
    }

    fn iter_cf_prefix<'p>(&self, cf_handle: ColumnFamily, prefix: &'p [u8]) -> DatabaseIterator {
        let mut readopts = ReadOptions::default();
        readopts.set_prefix_same_as_start(true);
        DatabaseIterator::from_raw(
            self.iter_cf_raw_opt(cf_handle, &readopts),
            DatabaseIteratorMode::From(prefix, DatabaseIteratorDirection::Forward)
        )
    }
}

impl<'a, T> ColumnFamilyIteration for &'a T where T: ColumnFamilyIteration {
    fn iter_cf_raw(&self, cf_handle: ColumnFamily) -> RawDatabaseIterator {
        (**self).iter_cf_raw(cf_handle)
    }

    fn iter_cf_raw_opt(
        &self,
        cf_handle: ColumnFamily,
        readopts: &ReadOptions
    ) -> RawDatabaseIterator {
        (**self).iter_cf_raw_opt(cf_handle, &readopts)
    }

    fn iter_cf(&self, cf_handle: ColumnFamily, mode: DatabaseIteratorMode) -> DatabaseIterator {
        (**self).iter_cf(cf_handle, mode)
    }

    fn iter_cf_full(
        &self,
        cf_handle: ColumnFamily,
        mode: DatabaseIteratorMode
    ) -> DatabaseIterator {
        (**self).iter_cf_full(cf_handle, mode)
    }

    fn iter_cf_prefix<'p>(&self, cf_handle: ColumnFamily, prefix: &'p [u8]) -> DatabaseIterator {
        (**self).iter_cf_prefix(cf_handle, &prefix)
    }
}

impl<'a, T> ColumnFamilyIteration for &'a mut T where T: ColumnFamilyIteration {
    fn iter_cf_raw(&self, cf_handle: ColumnFamily) -> RawDatabaseIterator {
        (**self).iter_cf_raw(cf_handle)
    }

    fn iter_cf_raw_opt(
        &self,
        cf_handle: ColumnFamily,
        readopts: &ReadOptions
    ) -> RawDatabaseIterator {
        (**self).iter_cf_raw_opt(cf_handle, &readopts)
    }

    fn iter_cf(&self, cf_handle: ColumnFamily, mode: DatabaseIteratorMode) -> DatabaseIterator {
        (**self).iter_cf(cf_handle, mode)
    }

    fn iter_cf_full(
        &self,
        cf_handle: ColumnFamily,
        mode: DatabaseIteratorMode
    ) -> DatabaseIterator {
        (**self).iter_cf_full(cf_handle, mode)
    }

    fn iter_cf_prefix<'p>(&self, cf_handle: ColumnFamily, prefix: &'p [u8]) -> DatabaseIterator {
        (**self).iter_cf_prefix(cf_handle, &prefix)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseTransactions {
    fn begin_transaction(&self) -> Transaction {
        let writeopts = WriteOptions::default();
        self.begin_transaction_opt(&writeopts)
    }

    fn begin_transaction_opt(&self, writeopts: &WriteOptions) -> Transaction;
}

impl<'a, T> DatabaseTransactions for &'a T where T: DatabaseTransactions {
    fn begin_transaction(&self) -> Transaction {
        (**self).begin_transaction()
    }

    fn begin_transaction_opt(&self, writeopts: &WriteOptions) -> Transaction {
        (**self).begin_transaction_opt(&writeopts)
    }
}

impl<'a, T> DatabaseTransactions for &'a mut T where T: DatabaseTransactions {
    fn begin_transaction(&self) -> Transaction {
        (**self).begin_transaction()
    }

    fn begin_transaction_opt(&self, writeopts: &WriteOptions) -> Transaction {
        (**self).begin_transaction_opt(&writeopts)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseSnapshotting {
    fn snapshot(&self) -> Snapshot;
}

impl<'a, T> DatabaseSnapshotting for &'a T where T: DatabaseSnapshotting {
    fn snapshot(&self) -> Snapshot {
        (**self).snapshot()
    }
}

impl<'a, T> DatabaseSnapshotting for &'a mut T where T: DatabaseSnapshotting {
    fn snapshot(&self) -> Snapshot {
        (**self).snapshot()
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseCheckpoints {
    fn checkpoint_object(&self) -> Result<Checkpoint, Error>;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// FIXME keep this trait?
pub trait DatabaseBackups {
    // FIXME not threadsafe as noted in rocksdb
    fn create_backup(&self, backup_engine: &BackupEngine) -> Result<(), Error>;

    // FIXME not threadsafe as noted in rocksdb
    fn create_backup_with_metadata(
        &self,
        backup_engine: &BackupEngine,
        metadata: &CStr
    ) -> Result<(), Error>;
}
