// Copyright 2018. Starry, Inc. All Rights Reserved.
//
// FIXME based off prior work...
// Copyright 2018 Eugene P.
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

///! Implementation of bindings to RocksDB Checkpoint[1] API
///
/// [1]: https://github.com/facebook/rocksdb/wiki/Checkpoints

use std::path::Path;

use ffi;
use refactor::database::InnerDbType;
use refactor::errors::Error;
use refactor::utils::pathref_to_cstring;

/// Undocumented parameter for `ffi::rocksdb_checkpoint_create` function. Zero by default.
const LOG_SIZE_FOR_FLUSH: u64 = 0_u64;

/// Database's checkpoint object.
/// Used to create checkpoints of the specified DB from time to time.
pub struct Checkpoint {
    inner: *mut ffi::rocksdb_checkpoint_t,
    // Keep the DB alive while we're alive.
    _db: InnerDbType
}

impl Drop for Checkpoint {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_checkpoint_object_destroy(self.inner);
        }
    }
}

impl Checkpoint {
    /// Creates new checkpoint object for specific DB.
    ///
    /// Does not actually produce checkpoints, call `.create_checkpoint()` method to produce
    /// a DB checkpoint.
    pub(crate) fn from_innerdbtype(db: InnerDbType) -> Result<Checkpoint, Error> {
        let checkpoint = match db {
            InnerDbType::DB(ref db) => unsafe {
                try_ffi!(ffi::rocksdb_checkpoint_object_create(db.inner))
            },
            InnerDbType::TxnDB(ref db) => unsafe {
                try_ffi!(ffi::rocksdb_transactiondb_checkpoint_object_create(db.inner))
            }
        };

        if checkpoint.is_null() {
            return Err(Error::new("Could not create checkpoint object.".to_owned()));
        }

        Ok(Checkpoint {
            inner: checkpoint,
            _db: db
        })
    }

    /// Creates new physical DB checkpoint in directory specified by `path`.
    pub fn create<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        let path = path.as_ref();
        let cpath = pathref_to_cstring(path)?;
        unsafe {
            try_ffi!(
                ffi::rocksdb_checkpoint_create(self.inner, cpath.as_ptr(), LOG_SIZE_FOR_FLUSH)
            );
        }
        Ok(())
    }
}
