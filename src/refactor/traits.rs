// Copyright 2018. Starry, Inc. All Rights Reserved.
//
// FIXME based off prior work...
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

use refactor::errors::Error;
use refactor::common::{
    ColumnFamily,
    DatabaseIterator,
    DatabaseIteratorDirection,
    DatabaseIteratorMode,
    DatabaseVector,
    RawDatabaseIterator,
    ReadOptions,
    Snapshot,
    WriteOptions
};
use refactor::transaction::Transaction;

///////////////////////////////////////////////////////////////////////////////////////////////////

// FIXME define these combindation traits

// pub trait DatabaseOperations: DatabaseReadOperations + DatabaseWriteOperations
// impl<T> DatabaseOperations for T where T: DatabaseReadOperations + DatabaseWriteOperations

// pub trait OptDatabaseOperations: OptDatabaseReadOperations + OptDatabaseWriteOperations
// impl<T> OptDatabaseOperations for T where T: OptDatabaseReadOperations + OptDatabaseWriteOperations

// pub trait AllDatabaseOperations: DatabaseOperations + OptDatabaseOperations
// impl<T> AllDatabaseOperations for T where T: DatabaseOperations + OptDatabaseOperations

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseMetaOperations {
    // FIXME implement, maybe these should go in DatabaseBatchOperations trait
    // fn write(&self, batch: WriteBatch) -> Result<(), Error>;
    // fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<(), Error>;
    // fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>);
    // fn compact_range_cf(&self, cf: ColumnFamily, start: Option<&[u8]>, end: Option<&[u8]>);
    // fn drop_cf(&mut self, name: &str) -> Result<(), Error>;
    // fn create_cf(&mut self, name: &str, opts: &Options) -> Result<ColumnFamily, Error>;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// FIXME split into read/write operations for Snapshot
pub trait DatabaseOperations {
    fn get(&self, key: &[u8]) -> Result<Option<DatabaseVector>, Error>;

    fn get_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<Option<DatabaseVector>, Error>;

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;

    fn put_cf(&self, cf_handle: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error>;

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;

    fn delete(&self, key: &[u8]) -> Result<(), Error>;

    fn delete_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<(), Error>;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait OptDatabaseOperations {
    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DatabaseVector>, Error>;

    fn get_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<DatabaseVector>, Error>;

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

///////////////////////////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseTransactions {
    fn begin_transaction(&self) -> Transaction {
        let writeopts = WriteOptions::default();
        self.begin_transaction_opt(&writeopts)
    }

    fn begin_transaction_opt(&self, writeopts: &WriteOptions) -> Transaction;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatabaseSnapshotting {
    fn snaphot(&self) -> Snapshot;
}
