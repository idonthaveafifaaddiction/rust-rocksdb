// Copyright 2018. Starry, Inc. All Rights Reserved.
//
// FIXME refactored code that was originally Copyright 2014 Tyler Neely
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

use libc::{c_char, size_t};
use std::any::Any;
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fs;
use std::path::{Path, PathBuf};
use std::ptr;
use std::slice;
use std::str;
use std::ffi::CStr;

use {
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
    Snapshot,
    Direction
};
use ffi;
use ffi_util::opt_bytes_to_ptr;

// FIXME better trait/method names?
// FIXME shouldn't be more than crate visible
pub trait BaseDbImpl {
    fn open_raw(&mut self, opts: &Options, cpath: &CString) -> Result<(), Error>;
    fn open_cf_raw(
        &mut self,
        opts: &Options,
        cpath: &CString,
        num_cfs: usize,
        cfnames: &mut Vec<*const c_char>,
        cfopts: &mut Vec<*const ffi::rocksdb_options_t>,
        cfhandles: &mut Vec<*mut ffi::rocksdb_column_family_handle_t>
    ) -> Result<(), Error>;
    fn get_base_db(&self) -> Result<*mut ffi::rocksdb_t, Error>;
    fn as_any(&self) -> &dyn Any;
}

/// Component providing functionality common to multiple DB types.
// FIXME shouldn't be more than crate visible
pub struct BaseDb {
    inner: Box<BaseDbImpl>,
    cfs: BTreeMap<String, ColumnFamily>,
    path: PathBuf
}

impl Drop for BaseDb {
    fn drop(&mut self) {
        for cf in self.cfs.values() {
            unsafe { ffi::rocksdb_column_family_handle_destroy(cf.inner); }
        }
    }
}

impl BaseDb {
    pub fn get_base_db(&self) -> *mut ffi::rocksdb_t {
        self.inner.get_base_db().unwrap()  // FIXME
    }

    pub fn inner(&self) -> &Box<BaseDbImpl> {
        &self.inner
    }

    /// Open a database with default options.
    pub fn open_default<P, D>(path: P) -> Result<Self, Error>
        where
            P: AsRef<Path>,
            D: BaseDbImpl + Default + 'static
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        Self::open::<P, D>(&opts, path)
    }

    /// Open the database with the specified options.
    pub fn open<P, D>(opts: &Options, path: P) -> Result<Self, Error>
        where
            P: AsRef<Path>,
            D: BaseDbImpl + Default + 'static
    {
        Self::open_cf::<P, D>(opts, path, &[])
    }

    /// Open a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P, D>(opts: &Options, path: P, cfs: &[&str]) -> Result<Self, Error>
        where
            P: AsRef<Path>,
            D: BaseDbImpl + Default + 'static
    {
        let cfs_v = cfs
            .to_vec()
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        Self::open_cf_descriptors::<P, D>(opts, path, cfs_v)
    }

    /// Open a database with the given database options and column family names/options.
    pub fn open_cf_descriptors<P, D>(
        opts: &Options,
        path: P,
        cfs: Vec<ColumnFamilyDescriptor>
    ) -> Result<Self, Error>
        where
            P: AsRef<Path>,
            D: BaseDbImpl + Default + 'static
    {
        let path = path.as_ref();
        let cpath = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                                       when opening DB."
                        .to_owned(),
                ))
            }
        };

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!(
                "Failed to create RocksDB\
                                           directory: `{:?}`.",
                e
            )));
        }

        let mut db = Box::new(D::default());
        let mut cf_map = BTreeMap::new();

        if cfs.len() == 0 {
            db.open_raw(&opts, &cpath)?;
        } else {
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

            let mut cfnames: Vec<_> = c_cfs.iter().map(|cf| cf.as_ptr()).collect();

            // These handles will be populated by DB.
            let mut cfhandles: Vec<_> = cfs_v.iter().map(|_| ptr::null_mut()).collect();

            let mut cfopts: Vec<_> = cfs_v.iter()
                .map(|cf| cf.options.inner as *const _)
                .collect();

            db.open_cf_raw(
                opts,
                &cpath,
                cfs_v.len(),
                &mut cfnames,
                &mut cfopts,
                &mut cfhandles
            )?;

            for handle in &cfhandles {
                if handle.is_null() {
                    return Err(Error::new(
                        "Received null column family \
                                           handle from DB."
                            .to_owned(),
                    ));
                }
            }

            for (n, h) in cfs_v.iter().zip(cfhandles) {
                cf_map.insert(n.name.clone(), ColumnFamily { inner: h });
            }
        }

        Ok(Self {
            inner: db,
            cfs: cf_map,
            path: path.to_path_buf(),
        })
    }

    pub fn list_cf<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Vec<String>, Error> {
        let cpath = match CString::new(path.as_ref().to_string_lossy().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                                       when opening DB."
                        .to_owned(),
                ))
            }
        };

        let mut length = 0;

        unsafe {
            let ptr = ffi_try!(ffi::rocksdb_list_column_families(
                opts.inner,
                cpath.as_ptr() as *const _,
                &mut length,
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
        let cpath = CString::new(path.as_ref().to_string_lossy().as_bytes()).unwrap();
        unsafe {
            ffi_try!(ffi::rocksdb_destroy_db(opts.inner, cpath.as_ptr(),));
        }
        Ok(())
    }

    pub fn repair<P: AsRef<Path>>(opts: Options, path: P) -> Result<(), Error> {
        let cpath = CString::new(path.as_ref().to_string_lossy().as_bytes()).unwrap();
        unsafe {
            ffi_try!(ffi::rocksdb_repair_db(opts.inner, cpath.as_ptr(),));
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }

    pub fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_write(self.inner.get_base_db()?, writeopts.inner, batch.inner,));
        }
        Ok(())
    }

    pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
        self.write_opt(batch, &WriteOptions::default())
    }

    pub fn write_without_wal(&self, batch: WriteBatch) -> Result<(), Error> {
        let mut wo = WriteOptions::new();
        wo.disable_wal(true);
        self.write_opt(batch, &wo)
    }

    pub fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DBVector>, Error> {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. \
                                   This is a fairly trivial call, and its \
                                   failure may be indicative of a \
                                   mis-compiled or mis-loaded RocksDB \
                                   library."
                    .to_owned(),
            ));
        }

        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_get(
                self.inner.get_base_db()?,
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len,
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    /// Return the bytes associated with a key value
    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.get_opt(key, &ReadOptions::default())
    }

    pub fn get_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<DBVector>, Error> {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. \
                                   This is a fairly trivial call, and its \
                                   failure may be indicative of a \
                                   mis-compiled or mis-loaded RocksDB \
                                   library."
                    .to_owned(),
            ));
        }

        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_get_cf(
                self.inner.get_base_db()?,
                readopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len,
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    pub fn get_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.get_cf_opt(cf, key, &ReadOptions::default())
    }

    pub fn create_cf(&mut self, name: &str, opts: &Options) -> Result<ColumnFamily, Error> {
        let cname = match CString::new(name.as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                                       when opening rocksdb"
                        .to_owned(),
                ))
            }
        };
        let cf = unsafe {
            let cf_handler = ffi_try!(ffi::rocksdb_create_column_family(
                self.inner.get_base_db()?,
                opts.inner,
                cname.as_ptr(),
            ));
            let cf = ColumnFamily { inner: cf_handler };
            self.cfs.insert(name.to_string(), cf);
            cf
        };
        Ok(cf)
    }

    pub fn drop_cf(&mut self, name: &str) -> Result<(), Error> {
        let cf = self.cfs.get(name);
        if cf.is_none() {
            return Err(Error::new(
                format!("Invalid column family: {}", name).to_owned(),
            ));
        }
        unsafe {
            ffi_try!(ffi::rocksdb_drop_column_family(
                self.inner.get_base_db()?,
                cf.unwrap().inner,
            ));
        }
        Ok(())
    }

    /// Return the underlying column family handle.
    pub fn cf_handle(&self, name: &str) -> Option<ColumnFamily> {
        self.cfs.get(name).cloned()
    }

    pub fn iterator(&self, mode: IteratorMode) -> DBIterator {
        let opts = ReadOptions::default();
        DBIterator::new(&self, &opts, mode)
    }

    /// Opens an interator with `set_total_order_seek` enabled.
    /// This must be used to iterate across prefixes when `set_memtable_factory` has been called
    /// with a Hash-based implementation.
    pub fn full_iterator(&self, mode: IteratorMode) -> DBIterator {
        let mut opts = ReadOptions::default();
        opts.set_total_order_seek(true);
        DBIterator::new(&self, &opts, mode)
    }

    pub fn prefix_iterator<'a>(&self, prefix: &'a [u8]) -> DBIterator {
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        DBIterator::new(
            &self,
            &opts,
            IteratorMode::From(prefix, Direction::Forward)
        )
    }

    pub fn iterator_cf(
        &self,
        cf_handle: ColumnFamily,
        mode: IteratorMode,
    ) -> Result<DBIterator, Error> {
        let opts = ReadOptions::default();
        DBIterator::new_cf(&self, cf_handle, &opts, mode)
    }

    pub fn full_iterator_cf(
        &self,
        cf_handle: ColumnFamily,
        mode: IteratorMode,
    ) -> Result<DBIterator, Error> {
        let mut opts = ReadOptions::default();
        opts.set_total_order_seek(true);
        DBIterator::new_cf(&self, cf_handle, &opts, mode)
    }

    pub fn prefix_iterator_cf<'a>(
        &self,
        cf_handle: ColumnFamily,
        prefix: &'a [u8]
    ) -> Result<DBIterator, Error> {
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        DBIterator::new_cf(
            &self,
            cf_handle,
            &opts, IteratorMode::From(prefix, Direction::Forward)
        )
    }

    pub fn raw_iterator(&self) -> DBRawIterator {
        let opts = ReadOptions::default();
        DBRawIterator::new(&self, &opts)
    }

    pub fn raw_iterator_cf(&self, cf_handle: ColumnFamily) -> Result<DBRawIterator, Error> {
        let opts = ReadOptions::default();
        DBRawIterator::new_cf(&self, cf_handle, &opts)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self)
    }

    pub fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_put(
                self.inner.get_base_db()?,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn put_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_put_cf(
                self.inner.get_base_db()?,
                writeopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn merge_opt(
        &self,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_merge(
                self.inner.get_base_db()?,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn merge_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_merge_cf(
                self.inner.get_base_db()?,
                writeopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_delete(
                self.inner.get_base_db()?,
                writeopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn delete_cf_opt(
        &self,
        cf: ColumnFamily,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_delete_cf(
                self.inner.get_base_db()?,
                writeopts.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.put_opt(key, value, &WriteOptions::default())
    }

    pub fn put_cf(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.put_cf_opt(cf, key, value, &WriteOptions::default())
    }

    pub fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.merge_opt(key, value, &WriteOptions::default())
    }

    pub fn merge_cf(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.merge_cf_opt(cf, key, value, &WriteOptions::default())
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.delete_opt(key, &WriteOptions::default())
    }

    pub fn delete_cf(&self, cf: ColumnFamily, key: &[u8]) -> Result<(), Error> {
        self.delete_cf_opt(cf, key, &WriteOptions::default())
    }

    pub fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) {
        unsafe {
            ffi::rocksdb_compact_range(
                self.inner.get_base_db().unwrap(),  // FIXME
                opt_bytes_to_ptr(start),
                start.map_or(0, |s| s.len()) as size_t,
                opt_bytes_to_ptr(end),
                end.map_or(0, |e| e.len()) as size_t,
            );
        }
    }

    pub fn compact_range_cf(&self, cf: ColumnFamily, start: Option<&[u8]>, end: Option<&[u8]>) {
        unsafe {
            ffi::rocksdb_compact_range_cf(
                self.inner.get_base_db().unwrap(),  // FIXME
                cf.inner,
                opt_bytes_to_ptr(start),
                start.map_or(0, |s| s.len()) as size_t,
                opt_bytes_to_ptr(end),
                end.map_or(0, |e| e.len()) as size_t,
            );
        }
    }
}
