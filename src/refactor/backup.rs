// Copyright 2019. Starry, Inc. All Rights Reserved.
//
// FIXME based off prior work...
// Copyright 2016 Alex Regueiro
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

use ffi;
use refactor::{database::Options, errors::Error, utils::pathref_to_cstring};

use std::{ffi::CStr, path::{Path, PathBuf}, sync::Arc};

use libc::c_int;

// FIXME need Arc?
pub struct RestoreOptions {
    inner: *mut ffi::rocksdb_restore_options_t
}

// FIXME need Arc?
struct RawBackupInfos {
    inner: *const ffi::rocksdb_backup_engine_info_t,
    count: u16
}

impl RawBackupInfos {
    fn new(backup_engine: *mut ffi::rocksdb_backup_engine_t) -> Self {
        let infos = unsafe { ffi::rocksdb_backup_engine_get_backup_info(backup_engine) };
        let num_infos = unsafe { ffi:: rocksdb_backup_engine_info_count(infos) };

        let count = if num_infos < 0 {
            panic!("Backup info count was negative");
        } else {
            num_infos as u16
        };

        Self {
            inner: infos,
            count
        }
    }

    #[inline]
    fn check_index(&self, index: u16) {
        if index > self.count {
            panic!("Index was greater than count");
        }
    }

    fn timestamp(&self, index: u16) -> i64 {
        self.check_index(index);
        unsafe { ffi::rocksdb_backup_engine_info_timestamp(self.inner, i32::from(index)) }
    }

    fn backup_id(&self, index: u16) -> u32 {
        self.check_index(index);
        unsafe { ffi::rocksdb_backup_engine_info_backup_id(self.inner, i32::from(index)) }
    }

    fn number_of_files(&self, index: u16) -> u32 {
        self.check_index(index);
        unsafe { ffi::rocksdb_backup_engine_info_number_files(self.inner, i32::from(index)) }
    }

    unsafe fn metadata(&self, index: u16) -> &CStr {
        self.check_index(index);
        let metadata = ffi::rocksdb_backup_engine_info_metadata(self.inner, i32::from(index));
        CStr::from_ptr(metadata)
    }

    fn get_infos(raw_infos: Arc<RawBackupInfos>) -> Vec<BackupInfo> {
        // FIXME make functional
        let mut infos = Vec::new();
        for index in 0..raw_infos.count {
            infos.push(BackupInfo::new(raw_infos.clone(), index))
        }
        infos
    }
}

impl Drop for RawBackupInfos {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_backup_engine_info_destroy(self.inner)
        }
    }
}

pub struct BackupInfo {
    raw_infos: Arc<RawBackupInfos>,
    index: u16
}

impl BackupInfo {
    fn new(raw_infos: Arc<RawBackupInfos>, index: u16) -> Self {
        Self {
            raw_infos,
            index
        }
    }

    pub fn timestamp(&self) -> i64 {
        self.raw_infos.timestamp(self.index)
    }

    pub fn backup_id(&self) -> u32 {
        self.raw_infos.backup_id(self.index)
    }

    pub fn number_of_files(&self) -> u32 {
        self.raw_infos.number_of_files(self.index)
    }

    pub fn metadata(&self) -> &CStr {
        unsafe { self.raw_infos.metadata(self.index) }
    }
}

// FIXME need Arc?
pub struct BackupEngine {
    inner: *mut ffi::rocksdb_backup_engine_t,
    path: PathBuf
}

unsafe impl Send for BackupEngine {} // FIXME FIXME FIXME not a good idea

impl BackupEngine {
    // FIXME options must always be the same whenever you open a particular backups directory as
    // noted in rocksdb
    /// Open a backup engine with the specified options.
    pub fn open<P: AsRef<Path>>(
        opts: &Options,
        path: P
    ) -> Result<BackupEngine, Error> {
        let cpath = pathref_to_cstring(path.as_ref())?;

        let share_table_files = false;
        let inner = unsafe {
            try_ffi!(ffi::rocksdb_backup_engine_open(
                opts.inner,
                cpath.as_ptr(),
                share_table_files as c_int
            ))
        };

        if inner.is_null() {
            return Err(Error::new("Could not initialize backup engine.".to_owned()));
        }

        Ok(BackupEngine { inner, path: path.as_ref().into() })
    }

    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }

    // FIXME not threadsafe as noted in rocksdb
    pub(crate) fn create_new_backup_from_db(&self, db: *mut ffi::rocksdb_t) -> Result<(), Error> {
        let flush_before_backup = true;
        unsafe {
            try_ffi!(ffi::rocksdb_backup_engine_create_new_backup(
                self.inner,
                db,
                flush_before_backup as c_int
            ))
        };
        Ok(())
    }

    // FIXME not threadsafe as noted in rocksdb
    pub(crate) fn create_new_backup_from_db_with_metadata(
        &self,
        db: *mut ffi::rocksdb_t,
        app_metadata: &CStr
    ) -> Result<(), Error> {
        let flush_before_backup = true;
        unsafe {
            try_ffi!(ffi::rocksdb_backup_engine_create_new_backup_with_metadata(
                self.inner,
                db,
                app_metadata.as_ptr(),
                flush_before_backup as c_int
            ))
        };
        Ok(())
    }

    pub fn purge_old_backups(&self, num_backups_to_keep: u32) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_backup_engine_purge_old_backups(self.inner, num_backups_to_keep))
        };
        Ok(())
    }

    pub fn delete_backup(&self, backup_id: u32) -> Result<(), Error> {
        unsafe {
            try_ffi!(ffi::rocksdb_backup_engine_delete_backup(self.inner, backup_id))
        };
        Ok(())
    }

    // FIXME as noted in rocksdb:
    // "
    //   restore from backup with backup_id
    //   IMPORTANT -- if options_.share_table_files == true,
    //   options_.share_files_with_checksum == false, you restore DB from some
    //   backup that is not the latest, and you start creating new backups from the
    //   new DB, they will probably fail.
    //
    //   Example: Let's say you have backups 1, 2, 3, 4, 5 and you restore 3.
    //   If you add new data to the DB and try creating a new backup now, the
    //   database will diverge from backups 4 and 5 and the new backup will fail.
    //   If you want to create new backup, you will first have to delete backups 4
    //   and 5.
    // "
    // FIXME as noted in rocksdb:
    // "
    //   Restoring DB from backup is NOT safe when there is another BackupEngine
    //   running that might call DeleteBackup() or PurgeOldBackups(). It is caller's
    //   responsibility to synchronize the operation, i.e. don't delete the backup
    //   when you're restoring from it
    // "
    pub fn restore_db_from_backup<P: AsRef<Path>>(
        &self,
        db_dir: P,
        backup_id: u32,
        restore_options: &RestoreOptions
    ) -> Result<(), Error> {
        // FIXME Assumption is made here that the WAL dir is the same as the DB dir, which is the
        // default unless the DB is opened with options that have the WAL dir set to something else.
        let db_dir = pathref_to_cstring(&db_dir)?;
        let wal_dir = &db_dir;

        unsafe {
            try_ffi!(ffi::rocksdb_backup_engine_restore_db_from_backup(
                self.inner,
                backup_id,
                db_dir.as_ptr() as *const _,
                wal_dir.as_ptr() as *const _,
                restore_options.inner
            ))
        };
        Ok(())
    }

    // FIXME as noted in rocksdb:
    // "
    //   restore from backup with backup_id
    //   IMPORTANT -- if options_.share_table_files == true,
    //   options_.share_files_with_checksum == false, you restore DB from some
    //   backup that is not the latest, and you start creating new backups from the
    //   new DB, they will probably fail.
    //
    //   Example: Let's say you have backups 1, 2, 3, 4, 5 and you restore 3.
    //   If you add new data to the DB and try creating a new backup now, the
    //   database will diverge from backups 4 and 5 and the new backup will fail.
    //   If you want to create new backup, you will first have to delete backups 4
    //   and 5.
    // "
    // FIXME as noted in rocksdb:
    // "
    //   Restoring DB from backup is NOT safe when there is another BackupEngine
    //   running that might call DeleteBackup() or PurgeOldBackups(). It is caller's
    //   responsibility to synchronize the operation, i.e. don't delete the backup
    //   when you're restoring from it
    // "
    pub fn restore_db_from_latest_backup<P: AsRef<Path>>(
        &self,
        db_dir: P,
        restore_options: &RestoreOptions
    ) -> Result<(), Error> {
        // FIXME Assumption is made here that the WAL dir is the same as the DB dir, which is the
        // default unless the DB is opened with options that have the WAL dir set to something else.
        let db_dir = pathref_to_cstring(&db_dir)?;
        let wal_dir = &db_dir;

        unsafe {
            try_ffi!(ffi::rocksdb_backup_engine_restore_db_from_latest_backup(
                self.inner,
                db_dir.as_ptr() as *const _,
                wal_dir.as_ptr() as *const _,
                restore_options.inner
            ))
        };
        Ok(())
    }

    pub fn get_backup_infos(&self) -> Vec<BackupInfo> {
        let raw_infos = Arc::new(RawBackupInfos::new(self.inner));
        RawBackupInfos::get_infos(raw_infos)
    }

}

impl RestoreOptions {
    pub fn set_keep_log_files(&mut self, keep_log_files: bool) {
        unsafe {
            ffi::rocksdb_restore_options_set_keep_log_files(self.inner, keep_log_files as c_int);
        }
    }
}

impl Default for RestoreOptions {
    fn default() -> RestoreOptions {
        let opts = unsafe { ffi::rocksdb_restore_options_create() };
        if opts.is_null() {
            panic!("Could not create RocksDB restore options".to_owned());
        }
        RestoreOptions { inner: opts }
    }
}

impl Drop for BackupEngine {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_backup_engine_close(self.inner);
        }
    }
}

impl Drop for RestoreOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_restore_options_destroy(self.inner);
        }
    }
}
