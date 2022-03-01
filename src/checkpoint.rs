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

//! Implementation of bindings to RocksDB Checkpoint[1] API
//!
//! [1]: https://github.com/facebook/rocksdb/wiki/Checkpoints

use crate::AsColumnFamilyRef;
use crate::{ffi, Error, DB};
use libc::c_char;

use crate::db::DBInner;
use crate::ffi_util::to_cpath;
use crate::{ThreadMode, DBCommon, ColumnFamily};
use std::ffi::{CString, CStr};
use std::fs::File;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::Path;

/// Undocumented parameter for `ffi::rocksdb_checkpoint_create` function. Zero by default.
const LOG_SIZE_FOR_FLUSH: u64 = 0_u64;

/// Database's checkpoint object.
/// Used to create checkpoints of the specified DB from time to time.
pub struct Checkpoint<'db> {
    inner: *mut ffi::rocksdb_checkpoint_t,
    _db: PhantomData<&'db ()>,
}

pub struct ExportImportFilesMetaData {
    pub inner: *mut ffi::rocksdb_export_import_files_metadata_t,
}

#[derive(Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
struct RocksdbExportImportFilesMetaData {
    db_comparator_name: String,
    files: Vec<RocksdbLevelMetaData>,
}

// file_type, num_entries, num_deletions, db_path,
#[derive(Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
struct RocksdbLevelMetaData {
    column_family_name: String,
    level: i32,
    relative_filename: String,
    name: String,
    file_number: u64,
    file_type: i32,
    directory: String,
    db_path: String,
    size: i32,
    smallest_seqno: u64,
    largest_seqno: u64,
    smallestkey: String,
    largestkey: String,
    num_reads_sampled: u64,
    being_compacted: i32,
    num_entries: u64,
    num_deletions: u64,
    temperature: u8,
    oldest_blob_file_number: u64,
    oldest_ancester_time: u64,
    file_creation_time: u64,
    file_checksum: String,
    file_checksum_func_name: String,
}

impl ExportImportFilesMetaData {
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        let mut file =
            File::create(path).map_err(|_| Error::new("Create metadata file failed".to_owned()))?;
        let str = unsafe {
            let c_buf: *const c_char = ffi_try!(ffi::rocksdb_marshal_export_import_files_metadata(
                self.inner
            ));
            let c_str: &CStr = CStr::from_ptr(c_buf);
            let str_slice: &str = c_str.to_str().unwrap();
            str_slice
        };
        let metadata: RocksdbExportImportFilesMetaData = serde_json::from_str(str).unwrap();
        file.write_all(serde_json::to_string_pretty(&metadata).unwrap().as_bytes())
            .map_err(|_| Error::new("Write metadate file failed".to_owned()))?;
        Ok(())
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<ExportImportFilesMetaData, Error> {
        let mut file =
            File::open(path).map_err(|_| Error::new("Open metadate file failed".to_owned()))?;
        let mut result = String::new();
        file.read_to_string(&mut result)
            .map_err(|_| Error::new("Read metadate file failed".to_owned()))?;

        let metadata: RocksdbExportImportFilesMetaData = serde_json::from_str(&result).unwrap();

        unsafe {
            let mut files = vec![];
            for file in metadata.files {
                let column_family_name = CString::new(file.column_family_name).unwrap();
                let relative_filename = CString::new(file.relative_filename).unwrap();
                let name = CString::new(file.name).unwrap();
                let directory = CString::new(file.directory).unwrap();
                let db_path = CString::new(file.db_path).unwrap();
                let smallestkey = CString::new(file.smallestkey).unwrap();
                let largestkey = CString::new(file.largestkey).unwrap();
                let file_checksum = CString::new(file.file_checksum).unwrap();
                let file_checksum_func_name = CString::new(file.file_checksum_func_name).unwrap();
                files.push(ffi_try!(ffi::rocksdb_new_live_file_metadata(
                    column_family_name.as_ptr(),
                    file.level,
                    relative_filename.as_ptr(),
                    name.as_ptr(),
                    file.file_number,
                    file.file_type,
                    directory.as_ptr(),
                    db_path.as_ptr(),
                    file.size,
                    file.smallest_seqno,
                    file.largest_seqno,
                    smallestkey.as_ptr(),
                    largestkey.as_ptr(),
                    file.num_reads_sampled,
                    file.being_compacted,
                    file.num_entries,
                    file.num_deletions,
                    file.temperature,
                    file.oldest_blob_file_number,
                    file.oldest_ancester_time,
                    file.file_creation_time,
                    file_checksum.as_ptr(),
                    file_checksum_func_name.as_ptr(),
                )));
            }

            let db_comparator_name = CString::new(metadata.db_comparator_name).unwrap();
            let inner = ffi_try!(ffi::rocksdb_new_export_import_files_metadata(
                db_comparator_name.as_ptr(),
                files.as_mut_ptr(),
                files.len() as i32,
            ));

            Ok(ExportImportFilesMetaData { inner })
        }
    }
}

impl<'db> Checkpoint<'db> {
    /// Creates new checkpoint object for specific DB.
    ///
    /// Does not actually produce checkpoints, call `.create_checkpoint()` method to produce
    /// a DB checkpoint.
    pub fn new<T: ThreadMode, I: DBInner>(db: &'db DBCommon<T, I>) -> Result<Self, Error> {
        let checkpoint: *mut ffi::rocksdb_checkpoint_t;

        unsafe {
            checkpoint = ffi_try!(ffi::rocksdb_checkpoint_object_create(db.inner.inner()));
        }

        if checkpoint.is_null() {
            return Err(Error::new("Could not create checkpoint object.".to_owned()));
        }

        Ok(Self {
            inner: checkpoint,
            _db: PhantomData,
        })
    }

    /// Creates new physical DB checkpoint in directory specified by `path`.
    pub fn create_checkpoint<P: AsRef<Path>>(&self, path: P) -> Result<(), Error> {
        let cpath = to_cpath(path)?;
        unsafe {
            ffi_try!(ffi::rocksdb_checkpoint_create(
                self.inner,
                cpath.as_ptr(),
                LOG_SIZE_FOR_FLUSH,
            ));
        }
        Ok(())
    }

    /// Exports all live SST files of a specified Column Family onto export_dir
    pub fn export_column_family<P: AsRef<Path>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        export_dir: P,
    ) -> Result<ExportImportFilesMetaData, Error> {
        let path = export_dir.as_ref();
        let cpath = if let Ok(c) = CString::new(path.to_string_lossy().as_bytes()) {
            c
        } else {
            return Err(Error::new(
                "Failed to convert path to CString when creating DB checkpoint".to_owned(),
            ));
        };

        let inner: *mut ffi::rocksdb_export_import_files_metadata_t;

        unsafe {
            inner = ffi_try!(ffi::rocksdb_column_family_export(
                self.inner,
                cf.inner(),
                cpath.as_ptr(),
            ));

            Ok(ExportImportFilesMetaData { inner })
        }
    }
}

impl<'db> Drop for Checkpoint<'db> {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_checkpoint_object_destroy(self.inner);
        }
    }
}
