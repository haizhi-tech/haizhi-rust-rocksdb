use std::time::Duration;

use gdiist_rocksdb as rocksdb;

use rocksdb::Ranges;
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
#[test]
fn test_approximate() {
    let path = "test1";
    let cf_opts = Options::default();
    let cf1 = ColumnFamilyDescriptor::new("cf1", cf_opts.clone());
    let cf2 = ColumnFamilyDescriptor::new("cf2", cf_opts);
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    let db = DB::open_cf_descriptors(&db_opts, path, vec![cf1, cf2]).unwrap();
    //
    let a = 1.to_string();
    let start_key: &[u8] = a.as_ref();
    let b = 9999.to_string();
    let end_key: &[u8] = b.as_ref();
    let cf1 = db.cf_handle("cf1").unwrap();
    let cf2 = db.cf_handle("cf2").unwrap();
    for key in 0..10000 {
        if key % 2 == 1 {
            db.put_cf(cf1, key.to_string(), (key * 2).to_string())
                .unwrap();
        } else {
            db.put_cf(cf2, key.to_string(), (key * 2).to_string())
                .unwrap();
        }
    }
    db.flush_cf(cf1).unwrap();
    db.flush_cf(cf2).unwrap();
    std::thread::sleep(Duration::from_secs(2));
    println!(
        "start_key {:?}, end_key {:?}",
        start_key.clone(),
        end_key.clone()
    );
    let files_error_margin: f64 = 1.0;
    let f = db
        .get_approximate_sizes_with_option(
            cf1,
            &[Ranges::new(start_key, end_key)],
            files_error_margin,
        )
        .unwrap();

    for ele in f {
        println!("the size of cf2 with memtable and sstfile is {}", ele);
    }

    for key in 0..10000 {
        if key % 2 == 1 {
            db.delete_cf(cf1, key.to_string()).unwrap();
        } else {
            db.delete_cf(cf2, key.to_string()).unwrap();
        }
    }
    db.flush_cf(cf1).unwrap();
    std::thread::sleep(Duration::from_secs(5));
    let none: Option<Vec<u8>> = None;
    db.compact_range(none.clone(), none);
    let f = db
        .get_approximate_sizes_with_option(
            cf1,
            &[Ranges::new(start_key, end_key)],
            files_error_margin,
        )
        .unwrap();
    for ele in f {
        println!("the size of cf1 with memtable and sstfile is {}", ele);
    }
}
