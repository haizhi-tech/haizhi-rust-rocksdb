// Copyright 2020 Tyler Neely
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

mod util;

use pretty_assertions::assert_eq;

use haizhi_rocksdb as rocksdb;

use rocksdb::{
    checkpoint::{Checkpoint, ExportImportFilesMetaData},
    Options, DB,
};
use util::DBPath;

#[test]
pub fn test_single_checkpoint() {
    const PATH_PREFIX: &str = "_rust_rocksdb_cp_single_";

    // Create DB with some data
    let db_path = DBPath::new(&format!("{PATH_PREFIX}db1"));

    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = DB::open(&opts, &db_path).unwrap();

    db.put(b"k1", b"v1").unwrap();
    db.put(b"k2", b"v2").unwrap();
    db.put(b"k3", b"v3").unwrap();
    db.put(b"k4", b"v4").unwrap();

    // Create checkpoint
    let cp1 = Checkpoint::new(&db).unwrap();
    let cp1_path = DBPath::new(&format!("{PATH_PREFIX}cp1"));
    cp1.create_checkpoint(&cp1_path).unwrap();

    // Verify checkpoint
    let cp = DB::open_default(&cp1_path).unwrap();

    assert_eq!(cp.get(b"k1").unwrap().unwrap(), b"v1");
    assert_eq!(cp.get(b"k2").unwrap().unwrap(), b"v2");
    assert_eq!(cp.get(b"k3").unwrap().unwrap(), b"v3");
    assert_eq!(cp.get(b"k4").unwrap().unwrap(), b"v4");
}

#[test]
pub fn test_multi_checkpoints() {
    const PATH_PREFIX: &str = "_rust_rocksdb_cp_multi_";

    // Create DB with some data
    let db_path = DBPath::new(&format!("{PATH_PREFIX}db1"));

    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = DB::open(&opts, &db_path).unwrap();

    db.put(b"k1", b"v1").unwrap();
    db.put(b"k2", b"v2").unwrap();
    db.put(b"k3", b"v3").unwrap();
    db.put(b"k4", b"v4").unwrap();

    // Create first checkpoint
    let cp1 = Checkpoint::new(&db).unwrap();
    let cp1_path = DBPath::new(&format!("{PATH_PREFIX}cp1"));
    cp1.create_checkpoint(&cp1_path).unwrap();

    // Verify checkpoint
    let cp = DB::open_default(&cp1_path).unwrap();

    assert_eq!(cp.get(b"k1").unwrap().unwrap(), b"v1");
    assert_eq!(cp.get(b"k2").unwrap().unwrap(), b"v2");
    assert_eq!(cp.get(b"k3").unwrap().unwrap(), b"v3");
    assert_eq!(cp.get(b"k4").unwrap().unwrap(), b"v4");

    // Change some existing keys
    db.put(b"k1", b"modified").unwrap();
    db.put(b"k2", b"changed").unwrap();

    // Add some new keys
    db.put(b"k5", b"v5").unwrap();
    db.put(b"k6", b"v6").unwrap();

    // Create another checkpoint
    let cp2 = Checkpoint::new(&db).unwrap();
    let cp2_path = DBPath::new(&format!("{PATH_PREFIX}cp2"));
    cp2.create_checkpoint(&cp2_path).unwrap();

    // Verify second checkpoint
    let cp = DB::open_default(&cp2_path).unwrap();

    assert_eq!(cp.get(b"k1").unwrap().unwrap(), b"modified");
    assert_eq!(cp.get(b"k2").unwrap().unwrap(), b"changed");
    assert_eq!(cp.get(b"k5").unwrap().unwrap(), b"v5");
    assert_eq!(cp.get(b"k6").unwrap().unwrap(), b"v6");
}

#[test]
fn test_checkpoint_outlive_db() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/fail/checkpoint_outlive_db.rs");
}

#[test]
fn test_export_column_family() {
    const PATH_PREFIX: &str = "_rust_rocksdb_export_column_family_";

    // Create DB with some data
    let origin_db_path = DBPath::new(&format!("{}db1", PATH_PREFIX));

    let mut opts = Options::default();
    opts.create_if_missing(true);
    let mut origin_db = DB::open(&opts, &origin_db_path).unwrap();
    // create two column families
    assert!(origin_db.create_cf("cf1", &opts).is_ok());
    assert!(origin_db.create_cf("cf2", &opts).is_ok());
    assert!(origin_db.cf_handle("cf1").is_some());
    assert!(origin_db.cf_handle("cf2").is_some());

    assert!(origin_db.put(b"0", b"0").is_ok());
    let cf1 = origin_db.cf_handle("cf1").unwrap();
    assert!(origin_db.put_cf(&cf1, b"1", b"1").is_ok());
    let cf2 = origin_db.cf_handle("cf2").unwrap();
    assert!(origin_db.put_cf(&cf2, b"2", b"2").is_ok());

    let checkpoint = Checkpoint::new(&origin_db);
    assert!(checkpoint.is_ok());
    let checkpoint = checkpoint.unwrap();

    let export_path = DBPath::new(&format!("{}db1_backup", PATH_PREFIX));
    // let export_path = Path::new("db1_backup");
    let result = checkpoint.export_column_family(cf1, &export_path);
    assert!(result.is_ok());
    let metadata = result.unwrap();
    // println!("metadata {:?}", metadata.save("save"));
    // metadata = ExportImportFilesMetaData::load("save").unwrap();
    // new db from export path
    let recover_db_path = DBPath::new(&format!("{}db1_recover", PATH_PREFIX));
    let mut recover_db = DB::open(&opts, &recover_db_path).unwrap();
    assert!(recover_db.cf_handle("cf1").is_none());
    assert!(recover_db.cf_handle("cf2").is_none());
    let result = recover_db.create_cf_with_import("cf1", &opts, &metadata);
    assert!(result.is_ok());
    assert!(recover_db.cf_handle("cf1").is_some());
    let cf1 = recover_db.cf_handle("cf1").unwrap();
    assert_eq!(recover_db.get_cf(&cf1, b"1").unwrap().unwrap(), b"1");
    assert!(recover_db.cf_handle("cf2").is_none());
    assert!(recover_db.get_cf(&cf1, b"2").unwrap().is_none());
}
