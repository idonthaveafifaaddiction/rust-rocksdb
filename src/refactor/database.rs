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

use std::{
    collections::BTreeMap,
    ffi::{CStr, CString},
    fs,
    path::{Path, PathBuf},
    ptr,
    sync::Arc,
    slice
};

use libc::{c_char, c_int, c_uchar};

use ffi;
use refactor::{
    backup::BackupEngine,
    checkpoint::Checkpoint,
    common::{
        ColumnFamily,
        ColumnFamilyDescriptor,
        DatabaseVector,
        RawDatabaseIterator,
        ReadOptions,
        Snapshot,
        WriteOptions
    },
    errors::Error,
    traits::{
        ColumnFamilyIteration,
        ColumnFamilyMergeOperations,
        DatabaseBackups,
        DatabaseCheckpoints,
        DatabaseIteration,
        DatabaseReadNoOptOperations,
        DatabaseReadOptOperations,
        DatabaseTransactions,
        DatabaseSnapshotting,
        DatabaseWriteNoOptOperations,
        DatabaseWriteOptOperations
    },
    transaction::{OptimisticTransactionOptions, Transaction, TransactionOptions},
    utils::{c_buf_to_opt_dbvec, pathref_to_cstring}
};

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub(crate) enum InnerDbType {
    DB(Arc<InnerDB>),
    TxnDB(Arc<InnerTransactionDB>)
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// FIXME probably not the best way to group these? A module would probably be fine...
pub struct DBTool;
impl DBTool {
    pub fn list_cf<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Vec<String>, Error> {
        let cpath = pathref_to_cstring(path)?;
        let mut length = 0;
        unsafe {
            let ptr = try_ffi!(ffi::rocksdb_list_column_families(
                opts.inner,
                cpath.as_ptr() as *const _,
                &mut length
            ));

            let vec = slice::from_raw_parts(ptr, length)
                .iter()
                .map(|ptr| CStr::from_ptr(*ptr).to_string_lossy().into_owned())
                .collect();
            ffi::rocksdb_list_column_families_destroy(ptr, length);
            Ok(vec)
        }
    }

    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = pathref_to_cstring(path)?;
        unsafe {
            try_ffi!(ffi::rocksdb_destroy_db(opts.inner, cpath.as_ptr()));
        }
        Ok(())
    }

    pub fn repair<P: AsRef<Path>>(opts: Options, path: P) -> Result<(), Error> {
        let cpath = pathref_to_cstring(path)?;
        unsafe {
            try_ffi!(ffi::rocksdb_repair_db(opts.inner, cpath.as_ptr()));
        }
        Ok(())
    }

    pub(crate) fn prepare_open_column_family_args(
        cfs: Vec<ColumnFamilyDescriptor>
    ) -> (
        Vec<ColumnFamilyDescriptor>,
        Vec<*const c_char>,
        Vec<*const ffi::rocksdb_options_t>,
        Vec<*mut ffi::rocksdb_column_family_handle_t>
    ) {
        let mut cfs_v = cfs;
        // Always open the default column family.
        if !cfs_v.iter().any(|cf| cf.name == "default") {
            cfs_v.push(ColumnFamilyDescriptor {
                name: String::from("default"),
                options: Options::default()
            });
        }
        // We need to store our CStrings in an intermediate vector
        // so that their pointers remain valid.
        let c_cfs: Vec<CString> = cfs_v
            .iter()
            .map(|cf| CString::new(cf.name.as_bytes()).unwrap())
            .collect();

        let cfnames: Vec<_> = c_cfs.iter().map(|cf| cf.as_ptr()).collect();

        // These handles will be populated by DB.
        let cfhandles: Vec<_> = cfs_v.iter().map(|_| ptr::null_mut()).collect();

        let cfopts: Vec<_> = cfs_v.iter()
            .map(|cf| cf.options.inner as *const _)
            .collect();

        (cfs_v, cfnames, cfopts, cfhandles)
    }

    pub(crate) fn make_column_families(
        cfs_v: Vec<ColumnFamilyDescriptor>,
        cfhandles: Vec<*mut ffi::rocksdb_column_family_handle_t>
    ) -> Result<BTreeMap<String, ColumnFamily>, Error> {
        let mut cf_map = BTreeMap::new();

        for handle in &cfhandles {
            if handle.is_null() {
                return Err(Error::new(
                    "Received null column family handle from DB.".to_owned(),
                ));
            }
        }

        for (n, h) in cfs_v.iter().zip(cfhandles) {
            cf_map.insert(n.name.clone(), ColumnFamily { inner: h });
        }

        Ok(cf_map)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DBRecoveryMode {
    TolerateCorruptedTailRecords = ffi::rocksdb_tolerate_corrupted_tail_records_recovery as isize,
    AbsoluteConsistency = ffi::rocksdb_absolute_consistency_recovery as isize,
    PointInTime = ffi::rocksdb_point_in_time_recovery as isize,
    SkipAnyCorruptedRecord = ffi::rocksdb_skip_any_corrupted_records_recovery as isize
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Database-wide options around performance and behavior.
///
/// Please read
/// [the official tuning guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide),
/// and most importantly, measure performance under realistic workloads with realistic hardware.
///
/// # Examples
///
/// ```
/// use rocksdb::{Options, DB};
/// use rocksdb::DBCompactionStyle;
///
/// fn badly_tuned_for_somebody_elses_disk() -> DB {
///    let path = "path/for/rocksdb/storageX";
///    let mut opts = Options::default();
///    opts.create_if_missing(true);
///    opts.set_max_open_files(10000);
///    opts.set_use_fsync(false);
///    opts.set_bytes_per_sync(8388608);
///    opts.optimize_for_point_lookup(1024);
///    opts.set_table_cache_num_shard_bits(6);
///    opts.set_max_write_buffer_number(32);
///    opts.set_write_buffer_size(536870912);
///    opts.set_target_file_size_base(1073741824);
///    opts.set_min_write_buffer_number_to_merge(4);
///    opts.set_level_zero_stop_writes_trigger(2000);
///    opts.set_level_zero_slowdown_writes_trigger(0);
///    opts.set_compaction_style(DBCompactionStyle::Universal);
///    opts.set_max_background_compactions(4);
///    opts.set_max_background_flushes(4);
///    opts.set_disable_auto_compactions(true);
///
///    DB::open(&opts, path).unwrap()
/// }
/// ```
pub struct Options {
    pub(crate) inner: *mut ffi::rocksdb_options_t // FIXME
}

// XXX(ssloboda) why was this deemed okay?
unsafe impl Send for Options {}

impl Default for Options {
    fn default() -> Self {
        let inner = unsafe {
            ffi::rocksdb_options_create()
        };
        if inner.is_null() {
            panic!("Could not create RocksDB options");
        }
        Self { inner }
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_options_destroy(self.inner);
        }
    }
}

// FIXME FIXME add all functions...
impl Options {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn create_if_missing(self, create_if_missing: bool) -> Self {
        unsafe {
            ffi::rocksdb_options_set_create_if_missing(self.inner, create_if_missing as c_uchar);
        }
        self
    }

    /// Recovery mode to control the consistency while replaying WAL.
    ///
    /// Default: DBRecoveryMode::PointInTime
    ///
    /// # Example
    ///
    /// ```
    /// use rocksdb::{Options, DBRecoveryMode};
    ///
    /// let mut opts = Options::default();
    /// opts.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);
    /// ```
    pub fn wal_recovery_mode(self, mode: DBRecoveryMode) -> Self {
        unsafe {
            ffi::rocksdb_options_set_wal_recovery_mode(self.inner, mode as c_int);
        }
        self
    }

    pub fn paranoid_checks(self, paranoid_checks: bool) -> Self {
        unsafe {
            ffi::rocksdb_options_set_paranoid_checks(self.inner, paranoid_checks as c_uchar);
        }
        self
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

type DBDropFn = unsafe extern "C" fn(*mut ffi::rocksdb_t);

pub(crate) struct InnerDB {
    pub(crate) inner: *mut ffi::rocksdb_t,
    drop_fn: DBDropFn
}

impl Drop for InnerDB {
    fn drop(&mut self) {
        unsafe {
            (self.drop_fn)(self.inner)
        }
    }
}

pub struct DB {
    inner: Arc<InnerDB>,
    cfs: BTreeMap<String, ColumnFamily>,  // FIXME where is this used?
    path: PathBuf

}

unsafe impl Send for DB {} // FIXME is this okay?

impl DB {
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Open a database with default options.
    pub fn open_default<P>(path: P) -> Result<Self, Error>
        where
            P: AsRef<Path>
    {
        let opts = Options::default()
            .create_if_missing(true);
        Self::open(&opts, path)
    }

    /// Open the database with the specified options.
    pub fn open<P>(opts: &Options, path: P) -> Result<Self, Error>
        where
            P: AsRef<Path>
    {
        Self::open_cf(opts, path, &[])
    }

    /// Open a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P>(opts: &Options, path: P, cfs: &[&str]) -> Result<Self, Error>
        where
            P: AsRef<Path>
    {
        let cfs_v = cfs
            .to_vec()
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        Self::open_cf_descriptors(opts, path, cfs_v)
    }

    /// Open a database with the given database options and column family names/options.
    pub fn open_cf_descriptors<P>(
        opts: &Options,
        path: P,
        cfs: Vec<ColumnFamilyDescriptor>
    ) -> Result<Self, Error>
        where
            P: AsRef<Path>
    {
        let path = path.as_ref();
        let cpath = pathref_to_cstring(path)?;
        if let Err(err) = fs::create_dir_all(&path) {
            return Err(Error::new(format!("Failed to create RocksDB directory: `{:?}`.", err)));
        }

        let (db, cf_map) = if cfs.len() == 0 {
            let db = unsafe {
                try_ffi!(ffi::rocksdb_open(opts.inner, cpath.as_ptr() as *const _))
            };
            if db.is_null() {
                return Err(Error::new("Failed to open database".into()));
            }

            (db, BTreeMap::new())
        } else {
            let (cfs_v, mut cfnames, mut cfopts, mut cfhandles) =
                DBTool::prepare_open_column_family_args(cfs);

            let db = unsafe {
                try_ffi!(ffi::rocksdb_open_column_families(
                    opts.inner,
                    cpath.as_ptr(),
                    cfs_v.len() as c_int,
                    cfnames.as_mut_ptr(),
                    cfopts.as_mut_ptr(),
                    cfhandles.as_mut_ptr()
                ))
            };
            if db.is_null() {
                return Err(Error::new("Failed to open database".into()));
            }

            let cf_map = DBTool::make_column_families(cfs_v, cfhandles)?;

            (db, cf_map)
        };

        Ok(Self {
            inner: Arc::new(InnerDB { inner: db, drop_fn: ffi::rocksdb_close }),
            cfs: cf_map,
            path: path.to_path_buf(),
        })
    }
}

impl DatabaseReadNoOptOperations for DB {
    fn get(&self, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        let readopts = ReadOptions::default();
        self.get_opt(key, &readopts)
    }

    fn get_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        let readopts = ReadOptions::default();
        self.get_cf_opt(cf_handle, key, &readopts)
    }
}

impl DatabaseWriteNoOptOperations for DB {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.put_opt(key, value, &writeopts)
    }

    fn put_cf(&self, cf_handle: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.put_cf_opt(cf_handle, key, value, &writeopts)
    }

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.merge_opt(key, value, &writeopts)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.delete_opt(key, &writeopts)
    }

    fn delete_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.delete_cf_opt(cf_handle, key, &writeopts)
    }
}

impl DatabaseReadOptOperations for DB {
    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DatabaseVector>, Error> {
        let mut val_len = 0;
        let val = unsafe {
            try_ffi!(ffi::rocksdb_get(
                self.inner.inner,
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
        readopts: &ReadOptions,
    ) -> Result<Option<DatabaseVector>, Error> {
        let mut val_len = 0;
        let val = unsafe {
            try_ffi!(ffi::rocksdb_get_cf(
                self.inner.inner,
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

impl DatabaseWriteOptOperations for DB {
    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_put(
                self.inner.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len()
            ));
        }
        Ok(())
    }

    fn put_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_put_cf(
                self.inner.inner,
                writeopts.inner,
                cf_handle.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len()
            ));
        }
        Ok(())
    }

    fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_merge(
                self.inner.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len()
            ));
        }
        Ok(())
    }

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_delete(
                self.inner.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len()
            ));
        }
        Ok(())
    }

    fn delete_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_delete_cf(
                self.inner.inner,
                writeopts.inner,
                cf_handle.inner,
                key.as_ptr() as *const c_char,
                key.len()
            ));
        }
        Ok(())
    }
}

impl ColumnFamilyMergeOperations for DB {
    fn merge_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_merge_cf(
                self.inner.inner,
                writeopts.inner,
                cf_handle.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len()
            ));
        }
        Ok(())
    }
}

impl DatabaseIteration for DB {
    fn iter_raw_opt(&self, readopts: &ReadOptions) -> RawDatabaseIterator {
        RawDatabaseIterator::from_innerdbtype(InnerDbType::DB(self.inner.clone()), &readopts)
    }
}

impl ColumnFamilyIteration for DB {
    fn iter_cf_raw_opt(
        &self,
        cf_handle: ColumnFamily,
        readopts: &ReadOptions
    ) -> RawDatabaseIterator {
        RawDatabaseIterator::from_db_cf(self.inner.clone(), cf_handle, &readopts)
    }
}

impl DatabaseSnapshotting for DB {
    fn snapshot(&self) -> Snapshot {
        Snapshot::from_innerdbtype(InnerDbType::DB(self.inner.clone()))
    }
}

impl DatabaseCheckpoints for DB {
    fn checkpoint_object(&self) -> Result<Checkpoint, Error> {
        Checkpoint::from_innerdbtype(InnerDbType::DB(self.inner.clone()))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OptimisticTransactionDB {
    inner: *mut ffi::rocksdb_optimistictransactiondb_t,
    base_db: DB
}

unsafe impl Send for OptimisticTransactionDB {} // FIXME is this okay?

impl OptimisticTransactionDB {
    pub fn path(&self) -> &Path {
        self.base_db.path.as_path()
    }

    /// Open a database with default options.
    pub fn open_default<P>(path: P) -> Result<Self, Error>
        where
            P: AsRef<Path>
    {
        let opts = Options::default()
            .create_if_missing(true);
        Self::open(&opts, path)
    }

    /// Open the database with the specified options.
    pub fn open<P>(opts: &Options, path: P) -> Result<Self, Error>
        where
            P: AsRef<Path>
    {
        Self::open_cf(opts, path, &[])
    }

    /// Open a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P>(opts: &Options, path: P, cfs: &[&str]) -> Result<Self, Error>
        where
            P: AsRef<Path>
    {
        let cfs_v = cfs
            .to_vec()
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        Self::open_cf_descriptors(opts, path, cfs_v)
    }

    /// Open a database with the given database options and column family names/options.
    pub fn open_cf_descriptors<P>(
        opts: &Options,
        path: P,
        cfs: Vec<ColumnFamilyDescriptor>
    ) -> Result<Self, Error>
        where
            P: AsRef<Path>
    {
        let path = path.as_ref();
        let cpath = pathref_to_cstring(path)?;
        if let Err(err) = fs::create_dir_all(&path) {
            return Err(Error::new(format!("Failed to create RocksDB directory: `{:?}`.", err)));
        }

        let (db, cf_map) = if cfs.len() == 0 {
            let db = unsafe {
                ffi_try!(ffi::rocksdb_optimistictransactiondb_open(
                    opts.inner,
                    cpath.as_ptr() as *const _,
                ))
            };
            if db.is_null() {
                return Err(Error::new("Failed to open database".into()));
            }

            (db, BTreeMap::new())
        } else {
            let (cfs_v, mut cfnames, mut cfopts, mut cfhandles) =
                DBTool::prepare_open_column_family_args(cfs);

            let db = unsafe {
                ffi_try!(ffi::rocksdb_optimistictransactiondb_open_column_families(
                    opts.inner,
                    cpath.as_ptr(),
                    cfs_v.len() as c_int,
                    cfnames.as_mut_ptr(),
                    cfopts.as_mut_ptr(),
                    cfhandles.as_mut_ptr(),
                ))
            };
            if db.is_null() {
                return Err(Error::new("Failed to open database".into()));
            }

            let cf_map = DBTool::make_column_families(cfs_v, cfhandles)?;

            (db, cf_map)
        };

        Ok(Self {
            inner: db,
            base_db: Self::make_base_db(db, cf_map, path.to_path_buf())
        })
    }

    fn make_base_db(
        inner: *mut ffi::rocksdb_optimistictransactiondb_t,
        cfs: BTreeMap<String, ColumnFamily>,
        path: PathBuf
    ) -> DB {
        let db = unsafe {
            ffi::rocksdb_optimistictransactiondb_get_base_db(inner)
        };
        if db.is_null() {
            panic!("Could not get base DB for RocksDB OptimisticTransactionDB");
        }
        DB {
            inner: Arc::new(InnerDB {
                inner: db,
                drop_fn: ffi::rocksdb_optimistictransactiondb_close_base_db
            }),
            cfs,
            path
        }
    }

    pub fn begin_transaction_opt_full(
        &self,
        writeopts: &WriteOptions,
        opttxnopts: &OptimisticTransactionOptions
    ) -> Transaction {
        let txn = unsafe {
            ffi::rocksdb_optimistictransaction_begin(
                self.inner,
                writeopts.inner,
                opttxnopts.inner,
                ptr::null_mut()
            )
        };
        if txn.is_null() {
            panic!("Failed to creaet RocksDB transaction");
        }

        Transaction {
            inner: txn,
            db: InnerDbType::DB(self.base_db.inner.clone())
        }
    }
}

impl Drop for OptimisticTransactionDB {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_optimistictransactiondb_close(self.inner)
        }
    }
}

impl DatabaseReadNoOptOperations for OptimisticTransactionDB {
    fn get(&self, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        self.base_db.get(key)
    }

    fn get_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        self.base_db.get_cf(cf_handle, key)
    }
}

impl DatabaseWriteNoOptOperations for OptimisticTransactionDB {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.base_db.put(key, value)
    }

    fn put_cf(&self, cf_handle: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.base_db.put_cf(cf_handle, key, value)
    }

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.base_db.merge(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.base_db.delete(key)
    }

    fn delete_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        self.base_db.delete_cf(cf_handle, key)
    }
}

impl DatabaseReadOptOperations for OptimisticTransactionDB {
    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DatabaseVector>, Error> {
        self.base_db.get_opt(key, &readopts)
    }

    fn get_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<DatabaseVector>, Error> {
        self.base_db.get_cf_opt(cf_handle, key, &readopts)
    }
}

impl DatabaseWriteOptOperations for OptimisticTransactionDB {
    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        self.base_db.put_opt(key, value, &writeopts)
    }

    fn put_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        self.base_db.put_cf_opt(cf_handle, key, value, &writeopts)
    }

    fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        self.base_db.merge_opt(key, value, &writeopts)
    }

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        self.base_db.delete_opt(key, &writeopts)
    }

    fn delete_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        self.base_db.delete_cf_opt(cf_handle, key, &writeopts)
    }
}

impl ColumnFamilyMergeOperations for OptimisticTransactionDB {
    fn merge_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        self.base_db.merge_cf_opt(cf_handle, key, value, &writeopts)
    }
}

impl DatabaseIteration for OptimisticTransactionDB {
    fn iter_raw_opt(&self, readopts: &ReadOptions) -> RawDatabaseIterator {
        self.base_db.iter_raw_opt(&readopts)
    }
}

impl ColumnFamilyIteration for OptimisticTransactionDB {
    fn iter_cf_raw_opt(
        &self,
        cf_handle: ColumnFamily,
        readopts: &ReadOptions
    ) -> RawDatabaseIterator {
        self.base_db.iter_cf_raw_opt(cf_handle, readopts)
    }
}

impl DatabaseSnapshotting for OptimisticTransactionDB {
    fn snapshot(&self) -> Snapshot {
        Snapshot::from_innerdbtype(InnerDbType::DB(self.base_db.inner.clone()))
    }
}

impl DatabaseTransactions for OptimisticTransactionDB {
    fn begin_transaction_opt(&self, writeopts: &WriteOptions) -> Transaction {
        let opttxnopts = OptimisticTransactionOptions::default();
        self.begin_transaction_opt_full(&writeopts, &opttxnopts)
    }
}

impl DatabaseCheckpoints for OptimisticTransactionDB {
    fn checkpoint_object(&self) -> Result<Checkpoint, Error> {
        Checkpoint::from_innerdbtype(InnerDbType::DB(self.base_db.inner.clone()))
    }
}

impl DatabaseBackups for OptimisticTransactionDB {
    fn create_backup(&self, backup_engine: &BackupEngine) -> Result<(), Error> {
        backup_engine.create_new_backup_from_db((*self.base_db.inner).inner)
    }

    fn create_backup_with_metadata(
        &self,
        backup_engine: &BackupEngine,
        metadata: &CStr
    ) -> Result<(), Error> {
        backup_engine.create_new_backup_from_db_with_metadata((*self.base_db.inner).inner, metadata)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransactionDBOptions {
    inner: *mut ffi::rocksdb_transactiondb_options_t
}

impl Default for TransactionDBOptions {
    fn default() -> Self {
        let inner = unsafe {
            ffi::rocksdb_transactiondb_options_create()
        };
        if inner.is_null() {
            panic!("Cannot create RocksDB transactiondb options");
        }
        Self {
            inner
        }
    }
}

impl Drop for TransactionDBOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_options_destroy(self.inner)
        }
    }
}

impl TransactionDBOptions {
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

///////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct InnerTransactionDB {
    pub(crate) inner: *mut ffi::rocksdb_transactiondb_t
}

impl InnerTransactionDB {
    fn open(
        opts: &Options,
        txndb_opts: &TransactionDBOptions,
        cpath: &CString
    ) -> Result<Self, Error> {
        let inner = unsafe {
            try_ffi!(ffi::rocksdb_transactiondb_open(
                opts.inner,
                txndb_opts.inner,
                cpath.as_ptr() as *const _
            ))
        };

        if inner.is_null() {
            return Err(Error::new("Could not initialize RocksDB TransactionDB.".to_owned()));
        }

        Ok(Self { inner })
    }
}

impl Drop for InnerTransactionDB {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_close(self.inner)
        }
    }
}

pub struct TransactionDB {
    inner: Arc<InnerTransactionDB>,
    path: PathBuf
}

unsafe impl Send for TransactionDB {} // FIXME is this okay?

// FIXME implement
impl TransactionDB {
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Open a transaction database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let opts = Options::default()
            .create_if_missing(true);
        let txndb_opts = TransactionDBOptions::default();
        Self::open(&opts, &txndb_opts, path)
    }

    /// Open the transaction database with the specified options.
    pub fn open<P: AsRef<Path>>(
        opts: &Options,
        txndb_opts: &TransactionDBOptions,
        path: P
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        let cpath = pathref_to_cstring(path)?;
        if let Err(err) = fs::create_dir_all(&path) {
            return Err(Error::new(format!("Failed to create RocksDB directory: `{:?}`.", err)));
        }

        Ok(Self {
            inner: Arc::new(InnerTransactionDB::open(&opts, &txndb_opts, &cpath)?),
            path: path.to_path_buf()
        })
    }

    pub fn begin_transaction_opt_full(
        &self,
        writeopts: &WriteOptions,
        txnopts: &TransactionOptions
    ) -> Transaction {
        let txn = unsafe {
            ffi::rocksdb_transaction_begin(
                self.inner.inner,
                writeopts.inner,
                txnopts.inner,
                ptr::null_mut()
            )
        };
        if txn.is_null() {
            panic!("Failed to creaet RocksDB transaction");
        }

        Transaction {
            inner: txn,
            db: InnerDbType::TxnDB(self.inner.clone())
        }
    }
}

impl DatabaseReadNoOptOperations for TransactionDB {
    fn get(&self, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        let readopts = ReadOptions::default();
        self.get_opt(key, &readopts)
    }

    fn get_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<Option<DatabaseVector>, Error> {
        let readopts = ReadOptions::default();
        self.get_cf_opt(cf_handle, key, &readopts)
    }
}

impl DatabaseWriteNoOptOperations for TransactionDB {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.put_opt(key, value, &writeopts)
    }

    fn put_cf(&self, cf_handle: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.put_cf_opt(cf_handle, key, value, &writeopts)
    }

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.merge_opt(key, value, &writeopts)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.delete_opt(key, &writeopts)
    }

    fn delete_cf(&self, cf_handle: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        let writeopts = WriteOptions::default();
        self.delete_cf_opt(cf_handle, key, &writeopts)
    }
}

impl DatabaseReadOptOperations for TransactionDB {
    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DatabaseVector>, Error> {
        let mut val_len = 0;
        let val = unsafe {
            try_ffi!(ffi::rocksdb_transactiondb_get(
                self.inner.inner,
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
        readopts: &ReadOptions,
    ) -> Result<Option<DatabaseVector>, Error> {
        let mut val_len = 0;
        let val = unsafe {
            try_ffi!(ffi::rocksdb_transactiondb_get_cf(
                self.inner.inner,
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

impl DatabaseWriteOptOperations for TransactionDB {
    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_transactiondb_put(
                self.inner.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len()
            ));
        }
        Ok(())
    }

    fn put_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_transactiondb_put_cf(
                self.inner.inner,
                writeopts.inner,
                cf_handle.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len()
            ));
        }
        Ok(())
    }

    fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_transactiondb_merge(
                self.inner.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len(),
                value.as_ptr() as *const c_char,
                value.len()
            ));
        }
        Ok(())
    }

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_transactiondb_delete(
                self.inner.inner,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len()
            ));
        }
        Ok(())
    }

    fn delete_cf_opt(
        &self,
        cf_handle: ColumnFamily,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_transactiondb_delete_cf(
                self.inner.inner,
                writeopts.inner,
                cf_handle.inner,
                key.as_ptr() as *const c_char,
                key.len()
            ));
        }
        Ok(())
    }
}

// XXX(ssloboda) not supported?
// impl ColumnFamilyMergeOperations for TransactionDB {}

impl DatabaseIteration for TransactionDB {
    fn iter_raw_opt(&self, readopts: &ReadOptions) -> RawDatabaseIterator {
        RawDatabaseIterator::from_innerdbtype(InnerDbType::TxnDB(self.inner.clone()), &readopts)
    }
}

// XXX(ssloboda) not supported?
// impl ColumnFamilyIteration for TransactionDB {}

impl DatabaseSnapshotting for TransactionDB {
    fn snapshot(&self) -> Snapshot {
        Snapshot::from_innerdbtype(InnerDbType::TxnDB(self.inner.clone()))
    }
}

impl DatabaseTransactions for TransactionDB {
    fn begin_transaction_opt(&self, writeopts: &WriteOptions) -> Transaction {
        let txnopts = TransactionOptions::default();
        self.begin_transaction_opt_full(&writeopts, &txnopts)
    }
}

impl DatabaseCheckpoints for TransactionDB {
    fn checkpoint_object(&self) -> Result<Checkpoint, Error> {
        Checkpoint::from_innerdbtype(InnerDbType::TxnDB(self.inner.clone()))
    }
}
